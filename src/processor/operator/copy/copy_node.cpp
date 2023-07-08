#include "processor/operator/copy/copy_node.h"

#include "common/string_utils.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

CopyNodeSharedState::CopyNodeSharedState(uint64_t& numRows, MemoryManager* memoryManager)
    : numRows{numRows}, pkColumnID{0}, hasLoggedWAL{false} {
    auto ftTableSchema = std::make_unique<FactorizedTableSchema>();
    ftTableSchema->appendColumn(
        std::make_unique<ColumnSchema>(false /* flat */, 0 /* dataChunkPos */,
            LogicalTypeUtils::getRowLayoutSize(LogicalType{LogicalTypeID::STRING})));
    table = std::make_shared<FactorizedTable>(memoryManager, std::move(ftTableSchema));
}

void CopyNodeSharedState::initializePrimaryKey(
    NodeTableSchema* nodeTableSchema, const std::string& directory) {
    if (nodeTableSchema->getPrimaryKey().dataType.getLogicalTypeID() != LogicalTypeID::SERIAL) {
        pkIndex = std::make_unique<PrimaryKeyIndexBuilder>(
            StorageUtils::getNodeIndexFName(
                directory, nodeTableSchema->tableID, DBFileType::ORIGINAL),
            nodeTableSchema->getPrimaryKey().dataType);
        pkIndex->bulkReserve(numRows);
    }
    for (auto& property : nodeTableSchema->properties) {
        if (property.propertyID == nodeTableSchema->getPrimaryKey().propertyID) {
            break;
        }
        pkColumnID++;
    }
}

void CopyNodeSharedState::initializeColumns(
    NodeTableSchema* nodeTableSchema, const std::string& directory) {
    columns.reserve(nodeTableSchema->properties.size());
    for (auto& property : nodeTableSchema->properties) {
        if (property.dataType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
            // Skip SERIAL, as it is not physically stored.
            continue;
        }
        auto fPath = StorageUtils::getNodePropertyColumnFName(
            directory, nodeTableSchema->tableID, property.propertyID, DBFileType::ORIGINAL);
        columns.push_back(std::make_unique<InMemColumn>(fPath, property.dataType));
    }
}

std::pair<offset_t, offset_t> CopyNodeLocalState::getStartAndEndOffset(vector_idx_t columnIdx) {
    auto startOffset =
        offsetVector->getValue<int64_t>(offsetVector->state->selVector->selectedPositions[0]);
    auto numNodes = ArrowColumnVector::getArrowColumn(arrowColumnVectors[columnIdx])->length();
    auto endOffset = startOffset + numNodes - 1;
    return {startOffset, endOffset};
}

void CopyNode::executeInternal(kuzu::processor::ExecutionContext* context) {
    logCopyWALRecord();
    while (children[0]->getNextTuple(context)) {
        std::vector<std::unique_ptr<InMemColumnChunk>> columnChunks;
        columnChunks.reserve(sharedState->columns.size());
        auto [startOffset, endOffset] = localState->getStartAndEndOffset(0 /* columnIdx */);
        for (auto i = 0u; i < sharedState->columns.size(); i++) {
            auto columnChunk = sharedState->columns[i]->getInMemColumnChunk(
                startOffset, endOffset, &localState->copyDesc);
            columnChunk->copyArrowArray(
                *ArrowColumnVector::getArrowColumn(localState->arrowColumnVectors[i]));
            columnChunks.push_back(std::move(columnChunk));
        }
        flushChunksAndPopulatePKIndex(columnChunks, startOffset, endOffset);
    }
}

void CopyNode::finalize(kuzu::processor::ExecutionContext* context) {
    auto tableID = localState->table->getTableID();
    if (sharedState->pkIndex) {
        sharedState->pkIndex->flush();
    }
    for (auto& column : sharedState->columns) {
        column->saveToFile();
    }
    for (auto& relTableSchema :
        localState->catalog->getAllRelTableSchemasContainBoundTable(tableID)) {
        localState->relsStore->getRelTable(relTableSchema->tableID)
            ->batchInitEmptyRelsForNewNodes(relTableSchema, sharedState->numRows);
    }
    localState->table->getNodeStatisticsAndDeletedIDs()->setNumTuplesForTable(
        tableID, sharedState->numRows);
    auto outputMsg = StringUtils::string_format("{} number of tuples has been copied to table: {}.",
        sharedState->numRows,
        localState->catalog->getReadOnlyVersion()->getTableName(tableID).c_str());
    FactorizedTableUtils::appendStringToTable(
        sharedState->table.get(), outputMsg, context->memoryManager);
}

void CopyNode::flushChunksAndPopulatePKIndex(
    const std::vector<std::unique_ptr<InMemColumnChunk>>& columnChunks, offset_t startNodeOffset,
    offset_t endNodeOffset) {
    // Flush each page within the [StartOffset, endOffset] range.
    for (auto i = 0u; i < sharedState->columns.size(); i++) {
        sharedState->columns[i]->flushChunk(columnChunks[i].get());
    }
    if (sharedState->pkIndex) {
        // Populate the primary key index.
        populatePKIndex(columnChunks[sharedState->pkColumnID].get(),
            sharedState->columns[sharedState->pkColumnID]->getInMemOverflowFile(), startNodeOffset,
            (endNodeOffset - startNodeOffset + 1));
    }
}

template<>
void CopyNode::appendToPKIndex<int64_t>(
    InMemColumnChunk* chunk, offset_t startOffset, uint64_t numValues) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        auto value = chunk->getValue<int64_t>(i);
        sharedState->pkIndex->append(value, offset);
    }
}

template<>
void CopyNode::appendToPKIndex<ku_string_t, InMemOverflowFile*>(InMemColumnChunk* chunk,
    offset_t startOffset, uint64_t numValues, InMemOverflowFile* overflowFile) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        auto value = chunk->getValue<ku_string_t>(i);
        auto key = overflowFile->readString(&value);
        sharedState->pkIndex->append(key.c_str(), offset);
    }
}

void CopyNode::populatePKIndex(InMemColumnChunk* chunk, InMemOverflowFile* overflowFile,
    offset_t startOffset, uint64_t numValues) {
    // First, check if there is any nulls.
    for (auto posInChunk = 0u; posInChunk < numValues; posInChunk++) {
        if (chunk->isNull(posInChunk)) {
            throw CopyException("Primary key cannot be null.");
        }
    }
    // No nulls, so we can populate the index with actual values.
    sharedState->pkIndex->lock();
    switch (chunk->getDataType().getLogicalTypeID()) {
    case LogicalTypeID::INT64: {
        appendToPKIndex<int64_t>(chunk, startOffset, numValues);
    } break;
    case LogicalTypeID::STRING: {
        appendToPKIndex<ku_string_t, InMemOverflowFile*>(
            chunk, startOffset, numValues, overflowFile);
    } break;
    default: {
        throw CopyException("Primary key must be either INT64, STRING or SERIAL.");
    }
    }
    sharedState->pkIndex->unlock();
}

void CopyNode::logCopyWALRecord() {
    std::unique_lock xLck{sharedState->mtx};
    if (!sharedState->hasLoggedWAL) {
        localState->wal->logCopyNodeRecord(localState->table->getTableID());
        localState->wal->flushAllPages();
        sharedState->hasLoggedWAL = true;
    }
}

} // namespace processor
} // namespace kuzu

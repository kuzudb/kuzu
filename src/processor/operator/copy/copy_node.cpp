#include "processor/operator/copy/copy_node.h"

#include "common/string_utils.h"

namespace kuzu {
namespace processor {

CopyNodeSharedState::CopyNodeSharedState(uint64_t& numRows, storage::MemoryManager* memoryManager)
    : numRows{numRows}, pkColumnID{0}, hasLoggedWAL{false} {
    auto ftTableSchema = std::make_unique<FactorizedTableSchema>();
    ftTableSchema->appendColumn(
        std::make_unique<ColumnSchema>(false /* flat */, 0 /* dataChunkPos */,
            common::LogicalTypeUtils::getRowLayoutSize(
                common::LogicalType{common::LogicalTypeID::STRING})));
    table = std::make_shared<FactorizedTable>(memoryManager, std::move(ftTableSchema));
}

void CopyNodeSharedState::initializePrimaryKey(
    catalog::NodeTableSchema* nodeTableSchema, const std::string& directory) {
    if (nodeTableSchema->getPrimaryKey().dataType.getLogicalTypeID() !=
        common::LogicalTypeID::SERIAL) {
        pkIndex = std::make_unique<storage::PrimaryKeyIndexBuilder>(
            storage::StorageUtils::getNodeIndexFName(
                directory, nodeTableSchema->tableID, common::DBFileType::ORIGINAL),
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
    catalog::NodeTableSchema* nodeTableSchema, const std::string& directory) {
    columns.reserve(nodeTableSchema->properties.size());
    for (auto& property : nodeTableSchema->properties) {
        if (property.dataType.getLogicalTypeID() == common::LogicalTypeID::SERIAL) {
            // Skip SERIAL, as it is not physically stored.
            continue;
        }
        auto fPath = storage::StorageUtils::getNodePropertyColumnFName(
            directory, nodeTableSchema->tableID, property.propertyID, common::DBFileType::ORIGINAL);
        columns.push_back(std::make_unique<storage::InMemColumn>(fPath, property.dataType));
    }
}

void CopyNode::executeInternal(kuzu::processor::ExecutionContext* context) {
    {
        std::unique_lock xLck{sharedState->mtx};
        if (!sharedState->hasLoggedWAL) {
            localState->wal->logCopyNodeRecord(localState->table->getTableID());
            localState->wal->flushAllPages();
            sharedState->hasLoggedWAL = true;
        }
    }
    while (children[0]->getNextTuple(context)) {
        std::vector<std::unique_ptr<storage::InMemColumnChunk>> columnChunks;
        columnChunks.reserve(sharedState->columns.size());
        auto offsetVector = localState->offsetVector;
        auto startOffset =
            offsetVector->getValue<int64_t>(offsetVector->state->selVector->selectedPositions[0]);
        auto endOffset =
            startOffset +
            common::ArrowColumnVector::getArrowColumn(localState->arrowColumnVectors[0])->length() -
            1;
        for (auto i = 0u; i < sharedState->columns.size(); i++) {
            auto columnChunk = sharedState->columns[i]->getInMemColumnChunk(
                startOffset, endOffset, &localState->copyDesc);
            columnChunk->copyArrowArray(
                *common::ArrowColumnVector::getArrowColumn(localState->arrowColumnVectors[i]));
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
    auto outputMsg = common::StringUtils::string_format(
        "{} number of tuples has been copied to table: {}.", sharedState->numRows,
        localState->catalog->getReadOnlyVersion()->getTableName(tableID).c_str());
    FactorizedTableUtils::appendStringToTable(
        sharedState->table.get(), outputMsg, context->memoryManager);
}

void CopyNode::flushChunksAndPopulatePKIndex(
    const std::vector<std::unique_ptr<storage::InMemColumnChunk>>& columnChunks,
    common::offset_t startNodeOffset, common::offset_t endNodeOffset) {
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
    storage::InMemColumnChunk* chunk, common::offset_t startOffset, uint64_t numValues) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        auto value = chunk->getValue<int64_t>(i);
        sharedState->pkIndex->append(value, offset);
    }
}

template<>
void CopyNode::appendToPKIndex<common::ku_string_t, storage::InMemOverflowFile*>(
    storage::InMemColumnChunk* chunk, common::offset_t startOffset, uint64_t numValues,
    storage::InMemOverflowFile* overflowFile) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        auto value = chunk->getValue<common::ku_string_t>(i);
        auto key = overflowFile->readString(&value);
        sharedState->pkIndex->append(key.c_str(), offset);
    }
}

void CopyNode::populatePKIndex(storage::InMemColumnChunk* chunk,
    storage::InMemOverflowFile* overflowFile, common::offset_t startOffset, uint64_t numValues) {
    // First, check if there is any nulls.
    for (auto posInChunk = 0u; posInChunk < numValues; posInChunk++) {
        if (chunk->isNull(posInChunk)) {
            throw common::CopyException("Primary key cannot be null.");
        }
    }
    // No nulls, so we can populate the index with actual values.
    sharedState->pkIndex->lock();
    switch (chunk->getDataType().getLogicalTypeID()) {
    case common::LogicalTypeID::INT64: {
        appendToPKIndex<int64_t>(chunk, startOffset, numValues);
    } break;
    case common::LogicalTypeID::STRING: {
        appendToPKIndex<common::ku_string_t, storage::InMemOverflowFile*>(
            chunk, startOffset, numValues, overflowFile);
    } break;
    default: {
        throw common::CopyException("Primary key must be either INT64, STRING or SERIAL.");
    }
    }
    sharedState->pkIndex->unlock();
}

} // namespace processor
} // namespace kuzu

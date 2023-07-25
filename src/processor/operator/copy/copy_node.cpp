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

CopyNode::CopyNode(std::shared_ptr<CopyNodeSharedState> sharedState, CopyNodeInfo copyNodeInfo,
    std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
    std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
    : Sink{std::move(resultSetDescriptor), PhysicalOperatorType::COPY_NODE, std::move(child), id,
          paramsString},
      sharedState{std::move(sharedState)}, copyNodeInfo{std::move(copyNodeInfo)},
      rowIdxVector{nullptr}, filePathVector{nullptr} {
    auto tableSchema = this->copyNodeInfo.catalog->getReadOnlyVersion()->getNodeTableSchema(
        this->copyNodeInfo.table->getTableID());
    copyStates.resize(tableSchema->getNumProperties());
    for (auto i = 0u; i < tableSchema->getNumProperties(); i++) {
        auto& property = tableSchema->properties[i];
        copyStates[i] = std::make_unique<storage::PropertyCopyState>(property.dataType);
    }
}

std::pair<row_idx_t, row_idx_t> CopyNode::getStartAndEndRowIdx(common::vector_idx_t columnIdx) {
    auto startRowIdx =
        rowIdxVector->getValue<int64_t>(rowIdxVector->state->selVector->selectedPositions[0]);
    auto numRows = ArrowColumnVector::getArrowColumn(dataColumnVectors[columnIdx])->length();
    auto endRowIdx = startRowIdx + numRows - 1;
    return {startRowIdx, endRowIdx};
}

std::pair<std::string, common::row_idx_t> CopyNode::getFilePathAndRowIdxInFile() {
    auto filePath = filePathVector->getValue<ku_string_t>(
        filePathVector->state->selVector->selectedPositions[0]);
    auto rowIdxInFile =
        rowIdxVector->getValue<int64_t>(rowIdxVector->state->selVector->selectedPositions[1]);
    return {filePath.getAsString(), rowIdxInFile};
}

void CopyNode::executeInternal(kuzu::processor::ExecutionContext* context) {
    logCopyWALRecord();
    while (children[0]->getNextTuple(context)) {
        std::vector<std::unique_ptr<InMemColumnChunk>> columnChunks;
        columnChunks.reserve(sharedState->columns.size());
        auto [startRowIdx, endRowIdx] = getStartAndEndRowIdx(0 /* columnIdx */);
        auto [filePath, startRowIdxInFile] = getFilePathAndRowIdxInFile();
        for (auto i = 0u; i < sharedState->columns.size(); i++) {
            auto columnChunk = sharedState->columns[i]->createInMemColumnChunk(
                startRowIdx, endRowIdx, &copyNodeInfo.copyDesc);
            columnChunk->copyArrowArray(
                *ArrowColumnVector::getArrowColumn(dataColumnVectors[i]), copyStates[i].get());
            columnChunks.push_back(std::move(columnChunk));
        }
        flushChunksAndPopulatePKIndex(
            columnChunks, startRowIdx, endRowIdx, filePath, startRowIdxInFile);
    }
}

void CopyNode::finalize(kuzu::processor::ExecutionContext* context) {
    auto tableID = copyNodeInfo.table->getTableID();
    if (sharedState->pkIndex) {
        sharedState->pkIndex->flush();
    }
    for (auto& column : sharedState->columns) {
        column->saveToFile();
    }
    for (auto& relTableSchema :
        copyNodeInfo.catalog->getAllRelTableSchemasContainBoundTable(tableID)) {
        copyNodeInfo.relsStore->getRelTable(relTableSchema->tableID)
            ->batchInitEmptyRelsForNewNodes(relTableSchema, sharedState->numRows);
    }
    copyNodeInfo.table->getNodeStatisticsAndDeletedIDs()->setNumTuplesForTable(
        tableID, sharedState->numRows);
    auto outputMsg = StringUtils::string_format("{} number of tuples has been copied to table: {}.",
        sharedState->numRows,
        copyNodeInfo.catalog->getReadOnlyVersion()->getTableName(tableID).c_str());
    FactorizedTableUtils::appendStringToTable(
        sharedState->table.get(), outputMsg, context->memoryManager);
}

void CopyNode::flushChunksAndPopulatePKIndex(
    const std::vector<std::unique_ptr<InMemColumnChunk>>& columnChunks, offset_t startNodeOffset,
    offset_t endNodeOffset, const std::string& filePath, row_idx_t startRowIdxInFile) {
    // Flush each page within the [StartOffset, endOffset] range.
    for (auto i = 0u; i < sharedState->columns.size(); i++) {
        sharedState->columns[i]->flushChunk(columnChunks[i].get());
    }
    if (sharedState->pkIndex) {
        // Populate the primary key index.
        populatePKIndex(columnChunks[sharedState->pkColumnID].get(),
            sharedState->columns[sharedState->pkColumnID]->getInMemOverflowFile(), startNodeOffset,
            (endNodeOffset - startNodeOffset + 1), filePath, startRowIdxInFile);
    }
}

template<>
uint64_t CopyNode::appendToPKIndex<int64_t>(
    InMemColumnChunk* chunk, offset_t startOffset, uint64_t numValues) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        auto value = chunk->getValue<int64_t>(i);
        if (!sharedState->pkIndex->append(value, offset)) {
            return i;
        }
    }
    return numValues;
}

template<>
uint64_t CopyNode::appendToPKIndex<ku_string_t, InMemOverflowFile*>(InMemColumnChunk* chunk,
    offset_t startOffset, uint64_t numValues, InMemOverflowFile* overflowFile) {
    for (auto i = 0u; i < numValues; i++) {
        auto offset = i + startOffset;
        auto value = chunk->getValue<ku_string_t>(i);
        auto key = overflowFile->readString(&value);
        if (!sharedState->pkIndex->append(key.c_str(), offset)) {
            return i;
        }
    }
    return numValues;
}

void CopyNode::populatePKIndex(InMemColumnChunk* chunk, InMemOverflowFile* overflowFile,
    offset_t startOffset, uint64_t numValues, const std::string& filePath,
    common::row_idx_t startRowIdxInFile) {
    // First, check if there is any nulls.
    for (auto posInChunk = 0u; posInChunk < numValues; posInChunk++) {
        if (chunk->isNull(posInChunk)) {
            throw CopyException(
                StringUtils::string_format("NULL found around L{} in file {} violates the non-null "
                                           "constraint of the primary key column.",
                    (startRowIdxInFile + posInChunk), filePath));
        }
    }
    // No nulls, so we can populate the index with actual values.
    std::string errorPKValueStr;
    row_idx_t errorPKRowIdx = INVALID_ROW_IDX;
    sharedState->pkIndex->lock();
    switch (chunk->getDataType().getLogicalTypeID()) {
    case LogicalTypeID::INT64: {
        auto numAppended = appendToPKIndex<int64_t>(chunk, startOffset, numValues);
        if (numAppended < numValues) {
            errorPKValueStr = std::to_string(chunk->getValue<int64_t>(startOffset + numAppended));
            errorPKRowIdx = startRowIdxInFile + numAppended;
        }
    } break;
    case LogicalTypeID::STRING: {
        auto numAppended = appendToPKIndex<ku_string_t, InMemOverflowFile*>(
            chunk, startOffset, numValues, overflowFile);
        if (numAppended < numValues) {
            errorPKValueStr = chunk->getValue<ku_string_t>(startOffset + numAppended).getAsString();
            errorPKRowIdx = startRowIdxInFile + numAppended;
        }
    } break;
    default: {
        throw CopyException(
            StringUtils::string_format("Invalid primary key column type {}. Primary key must be "
                                       "either INT64, STRING or SERIAL.",
                LogicalTypeUtils::dataTypeToString(chunk->getDataType())));
    }
    }
    sharedState->pkIndex->unlock();
    if (!errorPKValueStr.empty()) {
        assert(errorPKRowIdx != INVALID_ROW_IDX);
        throw CopyException(StringUtils::string_format(
            "Duplicated primary key value {} found around L{} in file {} violates the "
            "uniqueness constraint of the primary key column.",
            errorPKValueStr, errorPKRowIdx, filePath));
    }
}

void CopyNode::logCopyWALRecord() {
    std::unique_lock xLck{sharedState->mtx};
    if (!sharedState->hasLoggedWAL) {
        copyNodeInfo.wal->logCopyNodeRecord(copyNodeInfo.table->getTableID());
        copyNodeInfo.wal->flushAllPages();
        sharedState->hasLoggedWAL = true;
    }
}

} // namespace processor
} // namespace kuzu

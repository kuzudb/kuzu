#include "storage/store/struct_column.h"

#include "common/cast.h"
#include "storage/store/null_column.h"
#include "storage/store/struct_column_chunk.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

StructColumn::StructColumn(std::string name, LogicalType dataType,
    const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, Transaction* transaction,
    RWPropertyStats propertyStatistics, bool enableCompression)
    : Column{name, std::move(dataType), metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal,
          transaction, propertyStatistics, enableCompression, true /* requireNullColumn */} {
    auto fieldTypes = StructType::getFieldTypes(this->dataType);
    KU_ASSERT(metaDAHeaderInfo.childrenInfos.size() == fieldTypes.size());
    childColumns.resize(fieldTypes.size());
    for (auto i = 0u; i < fieldTypes.size(); i++) {
        auto childColName = StorageUtils::getColumnName(name,
            StorageUtils::ColumnType::STRUCT_CHILD, std::to_string(i));
        childColumns[i] = ColumnFactory::createColumn(childColName, *fieldTypes[i].copy(),
            *metaDAHeaderInfo.childrenInfos[i], dataFH, metadataFH, bufferManager, wal, transaction,
            propertyStatistics, enableCompression);
    }
}

void StructColumn::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    ColumnChunk* columnChunk, offset_t startOffset, offset_t endOffset) {
    KU_ASSERT(columnChunk->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT);
    nullColumn->scan(transaction, nodeGroupIdx, columnChunk->getNullChunk(), startOffset,
        endOffset);
    if (nodeGroupIdx >= metadataDA->getNumElements(transaction->getType())) {
        columnChunk->setNumValues(0);
    } else {
        auto chunkMetadata = metadataDA->get(nodeGroupIdx, transaction->getType());
        auto numValues = chunkMetadata.numValues == 0 ?
                             0 :
                             std::min(endOffset, chunkMetadata.numValues) - startOffset;
        columnChunk->setNumValues(numValues);
    }
    auto structColumnChunk = ku_dynamic_cast<ColumnChunk*, StructColumnChunk*>(columnChunk);
    for (auto i = 0u; i < childColumns.size(); i++) {
        childColumns[i]->scan(transaction, nodeGroupIdx, structColumnChunk->getChild(i),
            startOffset, endOffset);
    }
}

void StructColumn::initChunkState(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    ChunkState& readState) {
    Column::initChunkState(transaction, nodeGroupIdx, readState);
    readState.childrenStates.resize(childColumns.size());
    for (auto i = 0u; i < childColumns.size(); i++) {
        childColumns[i]->initChunkState(transaction, nodeGroupIdx, readState.childrenStates[i]);
    }
}

void StructColumn::scan(Transaction* transaction, ChunkState& readState,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    nullColumn->scan(transaction, *readState.nullState, startOffsetInGroup, endOffsetInGroup,
        resultVector, offsetInVector);
    for (auto i = 0u; i < childColumns.size(); i++) {
        auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childColumns[i]->scan(transaction, readState.childrenStates[i], startOffsetInGroup,
            endOffsetInGroup, fieldVector, offsetInVector);
    }
}

void StructColumn::scanInternal(Transaction* transaction, ChunkState& readState,
    ValueVector* nodeIDVector, ValueVector* resultVector) {
    for (auto i = 0u; i < childColumns.size(); i++) {
        auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childColumns[i]->scan(transaction, readState.childrenStates[i], nodeIDVector, fieldVector);
    }
}

void StructColumn::lookupInternal(Transaction* transaction, ChunkState& readState,
    ValueVector* nodeIDVector, ValueVector* resultVector) {
    for (auto i = 0u; i < childColumns.size(); i++) {
        auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childColumns[i]->lookup(transaction, readState.childrenStates[i], nodeIDVector,
            fieldVector);
    }
}

void StructColumn::write(ChunkState& state, offset_t offsetInChunk, ValueVector* vectorToWriteFrom,
    uint32_t posInVectorToWriteFrom) {
    KU_ASSERT(vectorToWriteFrom->dataType.getPhysicalType() == PhysicalTypeID::STRUCT);
    if (vectorToWriteFrom->isNull(posInVectorToWriteFrom)) {
        return;
    }
    KU_ASSERT(childColumns.size() == StructVector::getFieldVectors(vectorToWriteFrom).size());
    for (auto i = 0u; i < childColumns.size(); i++) {
        auto fieldVector = StructVector::getFieldVector(vectorToWriteFrom, i).get();
        childColumns[i]->write(state.childrenStates[i], offsetInChunk, fieldVector,
            posInVectorToWriteFrom);
    }
}

void StructColumn::write(ChunkState& state, offset_t offsetInChunk, ColumnChunk* data,
    offset_t dataOffset, length_t numValues) {
    KU_ASSERT(data->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT);
    nullColumn->write(*state.nullState, offsetInChunk, data->getNullChunk(), dataOffset, numValues);
    auto structData = ku_dynamic_cast<ColumnChunk*, StructColumnChunk*>(data);
    for (auto i = 0u; i < childColumns.size(); i++) {
        auto childData = structData->getChild(i);
        childColumns[i]->write(state.childrenStates[i], offsetInChunk, childData, dataOffset,
            numValues);
    }
}

void StructColumn::append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) {
    Column::append(columnChunk, nodeGroupIdx);
    KU_ASSERT(columnChunk->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT);
    auto structColumnChunk = static_cast<StructColumnChunk*>(columnChunk);
    for (auto i = 0u; i < childColumns.size(); i++) {
        childColumns[i]->append(structColumnChunk->getChild(i), nodeGroupIdx);
    }
}

void StructColumn::checkpointInMemory() {
    Column::checkpointInMemory();
    for (const auto& childColumn : childColumns) {
        childColumn->checkpointInMemory();
    }
}

void StructColumn::rollbackInMemory() {
    Column::rollbackInMemory();
    for (const auto& childColumn : childColumns) {
        childColumn->rollbackInMemory();
    }
}

void StructColumn::prepareCommit() {
    Column::prepareCommit();
    for (const auto& childColumn : childColumns) {
        childColumn->prepareCommit();
    }
}

void StructColumn::prepareCommitForExistingChunk(Transaction* transaction, ChunkState& state,
    const ChunkCollection& localInsertChunk, const offset_to_row_idx_t& insertInfo,
    const ChunkCollection& localUpdateChunk, const offset_to_row_idx_t& updateInfo,
    const offset_set_t& deleteInfo) {
    // STRUCT column doesn't have actual data stored in buffer. Only need to update the null
    // column.
    nullColumn->prepareCommitForExistingChunk(transaction, *state.nullState,
        getNullChunkCollection(localInsertChunk), insertInfo,
        getNullChunkCollection(localUpdateChunk), updateInfo, deleteInfo);
    if (state.metadata.numValues != state.nullState->metadata.numValues) {
        state.metadata.numValues = state.nullState->metadata.numValues;
        metadataDA->update(state.nodeGroupIdx, state.metadata);
    }
    // Update each child column separately
    for (auto i = 0u; i < childColumns.size(); i++) {
        childColumns[i]->prepareCommitForExistingChunk(transaction, state.childrenStates[i],
            getStructChildChunkCollection(localInsertChunk, i), insertInfo,
            getStructChildChunkCollection(localUpdateChunk, i), updateInfo, deleteInfo);
    }
}

void StructColumn::prepareCommitForExistingChunk(Transaction* transaction, ChunkState& state,
    const std::vector<offset_t>& dstOffsets, ColumnChunk* chunk, offset_t srcOffset) {
    KU_ASSERT(chunk->getDataType().getPhysicalType() == dataType.getPhysicalType());
    // STRUCT column doesn't have actual data stored in buffer. Only need to update the null
    // column.
    nullColumn->prepareCommitForExistingChunk(transaction, *state.nullState, dstOffsets,
        chunk->getNullChunk(), srcOffset);
    if (state.metadata.numValues != state.nullState->metadata.numValues) {
        state.metadata.numValues = state.nullState->metadata.numValues;
        metadataDA->update(state.nodeGroupIdx, state.metadata);
    }
    // Update each child column separately
    for (auto i = 0u; i < childColumns.size(); i++) {
        auto childChunk = ku_dynamic_cast<ColumnChunk*, StructColumnChunk*>(chunk)->getChild(i);
        childColumns[i]->prepareCommitForExistingChunk(transaction, state.childrenStates[i],
            dstOffsets, childChunk, srcOffset);
    }
}

ChunkCollection StructColumn::getStructChildChunkCollection(const ChunkCollection& chunkCollection,
    vector_idx_t childIdx) {
    ChunkCollection childChunkCollection;
    for (const auto& chunk : chunkCollection) {
        auto structChunk = ku_dynamic_cast<ColumnChunk*, StructColumnChunk*>(chunk);
        childChunkCollection.push_back(structChunk->getChild(childIdx));
    }
    return childChunkCollection;
}

} // namespace storage
} // namespace kuzu

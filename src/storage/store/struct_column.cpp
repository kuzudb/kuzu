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
    auto fieldTypes = StructType::getFieldTypes(&this->dataType);
    KU_ASSERT(metaDAHeaderInfo.childrenInfos.size() == fieldTypes.size());
    childColumns.resize(fieldTypes.size());
    for (auto i = 0u; i < fieldTypes.size(); i++) {
        auto childColName = StorageUtils::getColumnName(
            name, StorageUtils::ColumnType::STRUCT_CHILD, std::to_string(i));
        childColumns[i] = ColumnFactory::createColumn(childColName, *fieldTypes[i]->copy(),
            *metaDAHeaderInfo.childrenInfos[i], dataFH, metadataFH, bufferManager, wal, transaction,
            propertyStatistics, enableCompression);
    }
}

void StructColumn::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    ColumnChunk* columnChunk, offset_t startOffset, offset_t endOffset) {
    KU_ASSERT(columnChunk->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT);
    nullColumn->scan(
        transaction, nodeGroupIdx, columnChunk->getNullChunk(), startOffset, endOffset);
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
        childColumns[i]->scan(
            transaction, nodeGroupIdx, structColumnChunk->getChild(i), startOffset, endOffset);
    }
}

void StructColumn::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    nullColumn->scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector,
        offsetInVector);
    for (auto i = 0u; i < childColumns.size(); i++) {
        auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childColumns[i]->scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup,
            fieldVector, offsetInVector);
    }
}

void StructColumn::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    for (auto i = 0u; i < childColumns.size(); i++) {
        auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childColumns[i]->scan(transaction, nodeIDVector, fieldVector);
    }
}

void StructColumn::lookupInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    for (auto i = 0u; i < childColumns.size(); i++) {
        auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childColumns[i]->lookup(transaction, nodeIDVector, fieldVector);
    }
}

void StructColumn::write(node_group_idx_t nodeGroupIdx, offset_t offsetInChunk,
    ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    KU_ASSERT(vectorToWriteFrom->dataType.getPhysicalType() == PhysicalTypeID::STRUCT);
    if (vectorToWriteFrom->isNull(posInVectorToWriteFrom)) {
        return;
    }
    KU_ASSERT(childColumns.size() == StructVector::getFieldVectors(vectorToWriteFrom).size());
    for (auto i = 0u; i < childColumns.size(); i++) {
        auto fieldVector = StructVector::getFieldVector(vectorToWriteFrom, i).get();
        childColumns[i]->write(nodeGroupIdx, offsetInChunk, fieldVector, posInVectorToWriteFrom);
    }
}

void StructColumn::write(node_group_idx_t nodeGroupIdx, offset_t offsetInChunk, ColumnChunk* data,
    offset_t dataOffset, length_t numValues) {
    KU_ASSERT(data->getDataType().getPhysicalType() == PhysicalTypeID::STRUCT);
    nullColumn->write(nodeGroupIdx, offsetInChunk, data->getNullChunk(), dataOffset, numValues);
    auto structData = ku_dynamic_cast<ColumnChunk*, StructColumnChunk*>(data);
    for (auto i = 0u; i < childColumns.size(); i++) {
        auto childData = structData->getChild(i);
        childColumns[i]->write(nodeGroupIdx, offsetInChunk, childData, dataOffset, numValues);
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

bool StructColumn::canCommitInPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    const LocalVectorCollection& localInsertChunk, const offset_to_row_idx_t& insertInfo,
    const LocalVectorCollection& localUpdateChunk, const offset_to_row_idx_t& updateInfo) {
    // STRUCT column doesn't have actual data stored in buffer. Only need to check the null column.
    // Children columns are committed separately.
    return nullColumn->canCommitInPlace(
        transaction, nodeGroupIdx, localInsertChunk, insertInfo, localUpdateChunk, updateInfo);
}

bool StructColumn::canCommitInPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    const std::vector<offset_t>& dstOffsets, ColumnChunk* chunk, offset_t srcOffset) {
    return nullColumn->canCommitInPlace(
        transaction, nodeGroupIdx, dstOffsets, chunk->getNullChunk(), srcOffset);
}

void StructColumn::prepareCommitForChunk(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    const LocalVectorCollection& localInsertChunk, const offset_to_row_idx_t& insertInfo,
    const LocalVectorCollection& localUpdateChunk, const offset_to_row_idx_t& updateInfo,
    const offset_set_t& deleteInfo) {
    auto currentNumNodeGroups = metadataDA->getNumElements(transaction->getType());
    auto isNewNodeGroup = nodeGroupIdx >= currentNumNodeGroups;
    if (isNewNodeGroup) {
        // If this is a new node group, updateInfo should be empty. We should perform out-of-place
        // commit with a new column chunk.
        commitLocalChunkOutOfPlace(transaction, nodeGroupIdx, isNewNodeGroup, localInsertChunk,
            insertInfo, localUpdateChunk, updateInfo, deleteInfo);
    } else {
        // STRUCT column doesn't have actual data stored in buffer. Only need to update the null
        // column.
        if (canCommitInPlace(transaction, nodeGroupIdx, localInsertChunk, insertInfo,
                localUpdateChunk, updateInfo)) {
            nullColumn->commitLocalChunkInPlace(transaction, nodeGroupIdx, localInsertChunk,
                insertInfo, localUpdateChunk, updateInfo, deleteInfo);
        } else {
            nullColumn->commitLocalChunkOutOfPlace(transaction, nodeGroupIdx, isNewNodeGroup,
                localInsertChunk, insertInfo, localUpdateChunk, updateInfo, deleteInfo);
        }
        // Update each child column separately
        for (auto i = 0u; i < childColumns.size(); i++) {
            const auto& childColumn = childColumns[i];
            childColumn->prepareCommitForChunk(transaction, nodeGroupIdx,
                localInsertChunk.getStructChildVectorCollection(i), insertInfo,
                localUpdateChunk.getStructChildVectorCollection(i), updateInfo, deleteInfo);
        }
    }
}

void StructColumn::prepareCommitForChunk(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    const std::vector<offset_t>& dstOffsets, ColumnChunk* chunk, offset_t srcOffset) {
    KU_ASSERT(chunk->getDataType().getPhysicalType() == dataType.getPhysicalType());
    auto currentNumNodeGroups = metadataDA->getNumElements(transaction->getType());
    auto isNewNodeGroup = nodeGroupIdx >= currentNumNodeGroups;
    if (isNewNodeGroup) {
        // If this is a new node group, updateInfo should be empty. We should perform out-of-place
        // commit with a new column chunk.
        commitColumnChunkOutOfPlace(
            transaction, nodeGroupIdx, isNewNodeGroup, dstOffsets, chunk, srcOffset);
    } else {
        // STRUCT column doesn't have actual data stored in buffer. Only need to update the null
        // column.
        if (canCommitInPlace(transaction, nodeGroupIdx, dstOffsets, chunk, srcOffset)) {
            nullColumn->commitColumnChunkInPlace(
                nodeGroupIdx, dstOffsets, chunk->getNullChunk(), srcOffset);
        } else {
            nullColumn->commitColumnChunkOutOfPlace(transaction, nodeGroupIdx, isNewNodeGroup,
                dstOffsets, chunk->getNullChunk(), srcOffset);
        }
        // Update each child column separately
        for (auto i = 0u; i < childColumns.size(); i++) {
            const auto& childColumn = childColumns[i];
            auto childChunk = ku_dynamic_cast<ColumnChunk*, StructColumnChunk*>(chunk)->getChild(i);
            childColumn->prepareCommitForChunk(
                transaction, nodeGroupIdx, dstOffsets, childChunk, srcOffset);
        }
    }
}

} // namespace storage
} // namespace kuzu

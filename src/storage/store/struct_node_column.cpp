#include "storage/store/struct_node_column.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

StructNodeColumn::StructNodeColumn(LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
    BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
    Transaction* transaction)
    : NodeColumn{std::move(dataType), metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal,
          transaction, true} {
    auto fieldTypes = StructType::getFieldTypes(&this->dataType);
    assert(metaDAHeaderInfo.childrenInfos.size() == fieldTypes.size());
    childrenColumns.resize(fieldTypes.size());
    for (auto i = 0u; i < fieldTypes.size(); i++) {
        childrenColumns[i] =
            NodeColumnFactory::createNodeColumn(*fieldTypes[i], *metaDAHeaderInfo.childrenInfos[i],
                dataFH, metadataFH, bufferManager, wal, transaction);
    }
}

void StructNodeColumn::scan(transaction::Transaction* transaction, node_group_idx_t nodeGroupIdx,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    nullColumn->scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector,
        offsetInVector);
    for (auto i = 0u; i < childrenColumns.size(); i++) {
        auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childrenColumns[i]->scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup,
            fieldVector, offsetInVector);
    }
}

void StructNodeColumn::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    for (auto i = 0u; i < childrenColumns.size(); i++) {
        auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childrenColumns[i]->scan(transaction, nodeIDVector, fieldVector);
    }
}

void StructNodeColumn::lookupInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    for (auto i = 0u; i < childrenColumns.size(); i++) {
        auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childrenColumns[i]->lookup(transaction, nodeIDVector, fieldVector);
    }
}

void StructNodeColumn::writeInternal(
    offset_t nodeOffset, ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    nullColumn->writeInternal(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
}

} // namespace storage
} // namespace kuzu

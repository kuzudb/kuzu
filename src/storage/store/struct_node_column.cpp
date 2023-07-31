#include "storage/store/struct_node_column.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

StructNodeColumn::StructNodeColumn(LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
    BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal)
    : NodeColumn{
          std::move(dataType), metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, true} {
    auto fieldTypes = StructType::getFieldTypes(&this->dataType);
    assert(metaDAHeaderInfo.childrenInfos.size() == fieldTypes.size());
    childrenColumns.resize(fieldTypes.size());
    for (auto i = 0u; i < fieldTypes.size(); i++) {
        childrenColumns[i] = NodeColumnFactory::createNodeColumn(*fieldTypes[i],
            metaDAHeaderInfo.childrenInfos[i], dataFH, metadataFH, bufferManager, wal);
    }
}

void StructNodeColumn::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    for (auto i = 0u; i < childrenColumns.size(); i++) {
        auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childrenColumns[i]->scan(transaction, nodeIDVector, fieldVector);
    }
}

void StructNodeColumn::lookupInternal(transaction::Transaction* transaction,
    common::ValueVector* nodeIDVector, common::ValueVector* resultVector) {
    for (auto i = 0u; i < childrenColumns.size(); i++) {
        auto fieldVector = StructVector::getFieldVector(resultVector, i).get();
        childrenColumns[i]->lookup(transaction, nodeIDVector, fieldVector);
    }
}

} // namespace storage
} // namespace kuzu

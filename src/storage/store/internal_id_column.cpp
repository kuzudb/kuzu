#include "storage/store/internal_id_column.h"

#include "common/cast.h"
#include "storage/store/null_column.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

std::shared_ptr<common::ValueVector> InternalIDColumn::reWriteAsStructIDVector(
    common::ValueVector* resultVector, storage::MemoryManager* mm) {
    KU_ASSERT(resultVector->dataType.getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
    auto resultInternalIDs = (internalID_t*)resultVector->getData();
    auto resultInternalIDVector =
        std::make_shared<ValueVector>(*LogicalType::STRUCT(LogicalTypeID::INTERNAL_ID).get(), mm);
    auto resultTableIDVector = std::make_shared<ValueVector>(*LogicalType::INT64().get(), mm);
    auto resultOffsetVector = std::make_shared<ValueVector>(*LogicalType::INT64().get(), mm);
    resultTableIDVector->copyInfoFromVector(resultVector);
    resultOffsetVector->copyInfoFromVector(resultVector);
    resultInternalIDVector->copyInfoFromVector(resultVector);
    for (auto j = 0u; j < resultVector->state->selVector->selectedSize; j++) {
        auto pos = resultVector->state->selVector->selectedPositions[j];
        resultTableIDVector->setValue(pos, resultInternalIDs[pos].tableID);
        resultOffsetVector->setValue(pos, resultInternalIDs[pos].offset);
        resultOffsetVector->setNull(pos, resultOffsetVector->isNull(pos));
    }
    StructVector::referenceVector(resultInternalIDVector.get(), 0, resultTableIDVector);
    StructVector::referenceVector(resultInternalIDVector.get(), 1, resultOffsetVector);
    return resultInternalIDVector;
}

// TODO: should remove after rewritting internalID's physical type as STRUCT
void InternalIDColumn::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    ColumnChunk* columnChunk, offset_t startOffset, offset_t endOffset) {
    StructColumn::scan(transaction, nodeGroupIdx, columnChunk, startOffset, endOffset);
}

void InternalIDColumn::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    nullColumn->scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector,
        offsetInVector);
    auto mm = transaction->getLocalStorage()->getMemoryManager();
    auto resultInternalIDVector = reWriteAsStructIDVector(resultVector, mm);
    for (auto i = 0u; i < childColumns.size(); i++) {
        auto fieldVector = StructVector::getFieldVector(resultInternalIDVector.get(), i).get();
        childColumns[i]->scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup,
            fieldVector, offsetInVector);
    }
    // reset result vector
    auto finalIDs =
        (table_id_t*)StructVector::getFieldVector(resultInternalIDVector.get(), 0)->getData();
    auto finalOffsets =
        (offset_t*)StructVector::getFieldVector(resultInternalIDVector.get(), 1)->getData();
    resultVector->copyInfoFromVector(resultInternalIDVector.get());
    for (auto i = 0u; i < resultInternalIDVector->state->selVector->selectedSize; i++) {
        auto pos = resultInternalIDVector->state->selVector->selectedPositions[i];
        resultVector->setValue(pos, internalID_t{finalOffsets[pos], finalIDs[pos]});
    }
}

void InternalIDColumn::append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) {
    StructColumn::append(columnChunk, nodeGroupIdx);
}

void InternalIDColumn::scan(transaction::Transaction* transaction,
    common::ValueVector* nodeIDVector, common::ValueVector* resultVector) {
    if (nullColumn) {
        nullColumn->scan(transaction, nodeIDVector, resultVector);
    }
    scanInternal(transaction, nodeIDVector, resultVector);
}

void InternalIDColumn::lookup(transaction::Transaction* transaction,
    common::ValueVector* nodeIDVector, common::ValueVector* resultVector) {
    if (nullColumn) {
        nullColumn->lookup(transaction, nodeIDVector, resultVector);
    }
    KU_ASSERT(nodeIDVector->dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID);
    KU_ASSERT(resultVector->dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID);
    auto mm = transaction->getLocalStorage()->getMemoryManager();
    auto nodeInternalIDVector = reWriteAsStructIDVector(nodeIDVector, mm);
    auto resultInternalIDVector = reWriteAsStructIDVector(resultVector, mm);
    StructColumn::lookup(transaction, nodeIDVector, resultInternalIDVector.get());
    // reset result vector
    resultVector->copyInfoFromVector(resultInternalIDVector.get());
    auto finalIDs =
        (table_id_t*)StructVector::getFieldVector(resultInternalIDVector.get(), 0)->getData();
    auto finalOffsets =
        (offset_t*)StructVector::getFieldVector(resultInternalIDVector.get(), 1)->getData();
    for (auto i = 0u; i < resultVector->state->selVector->selectedSize; i++) {
        auto pos = resultVector->state->selVector->selectedPositions[i];
        resultVector->setValue(pos, internalID_t{finalOffsets[pos], finalIDs[pos]});
    }
}

void InternalIDColumn::scanInternal(transaction::Transaction* transaction,
    common::ValueVector* nodeIDVector, common::ValueVector* resultVector) {
    auto mm = transaction->getLocalStorage()->getMemoryManager();
    auto resultInternalIDVector = reWriteAsStructIDVector(resultVector, mm);
    StructColumn::scanInternal(transaction, nodeIDVector, resultInternalIDVector.get());
    // reset result vector
    auto finalIDs =
        (table_id_t*)StructVector::getFieldVector(resultInternalIDVector.get(), 0)->getData();
    auto finalOffsets =
        (offset_t*)StructVector::getFieldVector(resultInternalIDVector.get(), 1)->getData();
    resultVector->copyInfoFromVector(resultInternalIDVector.get());
    for (auto i = 0u; i < resultInternalIDVector->state->selVector->selectedSize; i++) {
        auto pos = resultInternalIDVector->state->selVector->selectedPositions[i];
        resultVector->setValue(pos, internalID_t{finalOffsets[pos], finalIDs[pos]});
    }
}

} // namespace storage
} // namespace kuzu

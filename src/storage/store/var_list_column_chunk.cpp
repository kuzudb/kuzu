#include "storage/store/var_list_column_chunk.h"

#include "arrow/array.h"
#include "common/types/value/nested.h"
#include "common/types/value/value.h"
#include "storage/store/table_copy_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void VarListDataColumnChunk::reset() const {
    dataColumnChunk->resetToEmpty();
}

void VarListDataColumnChunk::resizeBuffer(uint64_t numValues) {
    if (numValues <= capacity) {
        return;
    }
    while (capacity < numValues) {
        capacity *= VAR_LIST_RESIZE_RATIO;
    }
    dataColumnChunk->resize(capacity);
}

VarListColumnChunk::VarListColumnChunk(LogicalType dataType, bool enableCompression)
    : ColumnChunk{std::move(dataType), enableCompression, true /* hasNullChunk */},
      varListDataColumnChunk{ColumnChunkFactory::createColumnChunk(
          *VarListType::getChildType(&this->dataType), enableCompression)} {
    assert(this->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
}

void VarListColumnChunk::append(ColumnChunk* other, offset_t startPosInOtherChunk,
    offset_t startPosInChunk, uint32_t numValuesToAppend) {
    nullChunk->append(
        other->getNullChunk(), startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    auto otherListChunk = reinterpret_cast<VarListColumnChunk*>(other);
    auto offsetInDataChunkToAppend = varListDataColumnChunk.getNumValues();
    for (auto i = 0u; i < numValuesToAppend; i++) {
        offsetInDataChunkToAppend += otherListChunk->getListLen(startPosInOtherChunk + i);
        setValue(offsetInDataChunkToAppend, startPosInChunk + i);
    }
    auto startOffset = otherListChunk->getListOffset(startPosInOtherChunk);
    auto endOffset = otherListChunk->getListOffset(startPosInOtherChunk + numValuesToAppend);
    varListDataColumnChunk.resizeBuffer(offsetInDataChunkToAppend);
    varListDataColumnChunk.dataColumnChunk->append(
        otherListChunk->varListDataColumnChunk.dataColumnChunk.get(), startOffset,
        varListDataColumnChunk.getNumValues(), endOffset - startOffset);
    numValues += numValuesToAppend;
}

void VarListColumnChunk::write(const Value& listVal, uint64_t posToWrite) {
    assert(listVal.getDataType()->getPhysicalType() == PhysicalTypeID::VAR_LIST);
    auto numValuesInList = NestedVal::getChildrenSize(&listVal);
    varListDataColumnChunk.resizeBuffer(varListDataColumnChunk.getNumValues() + numValuesInList);
    for (auto i = 0u; i < numValuesInList; i++) {
        varListDataColumnChunk.dataColumnChunk->write(
            *NestedVal::getChildVal(&listVal, i), varListDataColumnChunk.getNumValues());
        varListDataColumnChunk.increaseNumValues(1);
    }
    setValue(varListDataColumnChunk.getNumValues(), posToWrite);
}

void VarListColumnChunk::resetToEmpty() {
    ColumnChunk::resetToEmpty();
    varListDataColumnChunk.reset();
}

void VarListColumnChunk::append(common::ValueVector* vector, common::offset_t startPosInChunk) {
    auto nextListOffsetInChunk = getListOffset(startPosInChunk);
    auto offsetBufferToWrite = (common::offset_t*)(buffer.get());
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto pos = vector->state->selVector->selectedPositions[i];
        uint64_t listLen = vector->isNull(pos) ? 0 : vector->getValue<list_entry_t>(pos).size;
        nullChunk->setNull(startPosInChunk + i, vector->isNull(pos));
        nextListOffsetInChunk += listLen;
        offsetBufferToWrite[startPosInChunk + i] = nextListOffsetInChunk;
    }
    varListDataColumnChunk.resizeBuffer(nextListOffsetInChunk);
    auto dataVector = ListVector::getDataVector(vector);
    dataVector->setState(std::make_unique<DataChunkState>());
    dataVector->state->selVector->resetSelectorToValuePosBuffer();
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto pos = vector->state->selVector->selectedPositions[i];
        if (vector->isNull(pos)) {
            continue;
        }
        copyListValues(vector->getValue<list_entry_t>(pos), dataVector);
    }
    numValues += vector->state->selVector->selectedSize;
}

void VarListColumnChunk::copyListValues(const list_entry_t& entry, ValueVector* dataVector) {
    auto numListValuesToCopy = entry.size;
    auto numListValuesCopied = 0;
    while (numListValuesCopied < numListValuesToCopy) {
        auto numListValuesToCopyInBatch =
            std::min<uint64_t>(numListValuesToCopy - numListValuesCopied, DEFAULT_VECTOR_CAPACITY);
        dataVector->state->selVector->selectedSize = numListValuesToCopyInBatch;
        for (auto j = 0u; j < numListValuesToCopyInBatch; j++) {
            dataVector->state->selVector->selectedPositions[j] =
                entry.offset + numListValuesCopied + j;
        }
        varListDataColumnChunk.append(dataVector);
        numListValuesCopied += numListValuesToCopyInBatch;
    }
}

} // namespace storage
} // namespace kuzu

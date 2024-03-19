#include "storage/store/var_list_column_chunk.h"

#include "common/cast.h"
#include "common/data_chunk/sel_vector.h"
#include "common/types/value/value.h"
#include "storage/store/column_chunk.h"

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
    capacity = capacity == 0 ? 1 : capacity;
    while (capacity < numValues) {
        capacity = std::ceil(capacity * CHUNK_RESIZE_RATIO);
    }
    dataColumnChunk->resize(capacity);
}

VarListColumnChunk::VarListColumnChunk(
    LogicalType dataType, uint64_t capacity, bool enableCompression, bool inMemory)
    : ColumnChunk{std::move(dataType), capacity, enableCompression, true /* hasNullChunk */} {
    sizeColumnChunk = ColumnChunkFactory::createColumnChunk(
        *common::LogicalType::UINT32(), enableCompression, capacity);
    varListDataColumnChunk = std::make_unique<VarListDataColumnChunk>(
        ColumnChunkFactory::createColumnChunk(*VarListType::getChildType(&this->dataType)->copy(),
            enableCompression, 0 /* capacity */, inMemory));
    KU_ASSERT(this->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
}

void VarListColumnChunk::append(
    ColumnChunk* other, offset_t startPosInOtherChunk, uint32_t numValuesToAppend) {
    auto otherListChunk = ku_dynamic_cast<ColumnChunk*, VarListColumnChunk*>(other);
    nullChunk->append(other->getNullChunk(), startPosInOtherChunk, numValuesToAppend);
    sizeColumnChunk->getNullChunk()->append(
        other->getNullChunk(), startPosInOtherChunk, numValuesToAppend);
    offset_t offsetInDataChunkToAppend = varListDataColumnChunk->getNumValues();
    for (auto i = 0u; i < numValuesToAppend; i++) {
        auto appendListLen = otherListChunk->getListLen(startPosInOtherChunk + i);
        sizeColumnChunk->setValue<uint32_t>(appendListLen, numValues);
        offsetInDataChunkToAppend += appendListLen;
        setValue<offset_t>(offsetInDataChunkToAppend, numValues);
    }
    varListDataColumnChunk->resizeBuffer(offsetInDataChunkToAppend);
    for (auto i = 0u; i < numValuesToAppend; i++) {
        auto startOffset = otherListChunk->getListStartOffset(startPosInOtherChunk + i);
        auto appendLen = otherListChunk->getListLen(startPosInOtherChunk + i);
        varListDataColumnChunk->dataColumnChunk->append(
            otherListChunk->varListDataColumnChunk->dataColumnChunk.get(), startOffset, appendLen);
    }
    KU_ASSERT(sizeColumnChunk->getNumValues() == numValues);
    KU_ASSERT(nullChunk->getNumValues() == numValues);
}

void VarListColumnChunk::resetToEmpty() {
    ColumnChunk::resetToEmpty();
    sizeColumnChunk->resetToEmpty();
    varListDataColumnChunk = std::make_unique<VarListDataColumnChunk>(
        ColumnChunkFactory::createColumnChunk(*VarListType::getChildType(&this->dataType)->copy(),
            enableCompression, 0 /* capacity */));
}

void VarListColumnChunk::append(ValueVector* vector, SelectionVector& selVector) {
    auto numToAppend = selVector.selectedSize;
    auto newCapacity = capacity;
    while (numValues + numToAppend >= newCapacity) {
        newCapacity = std::ceil(newCapacity * 1.5);
    }
    if (capacity < newCapacity) {
        resize(newCapacity);
    }
    offset_t nextListOffsetInChunk = varListDataColumnChunk->getNumValues();
    auto offsetBufferToWrite = (offset_t*)(buffer.get());
    for (auto i = 0u; i < selVector.selectedSize; i++) {
        auto pos = selVector.selectedPositions[i];
        auto listLen = vector->isNull(pos) ? 0 : vector->getValue<list_entry_t>(pos).size;
        sizeColumnChunk->setValue<uint32_t>(listLen, numValues + i);
        sizeColumnChunk->getNullChunk()->setNull(numValues + i, vector->isNull(pos));
        nullChunk->setNull(numValues + i, vector->isNull(pos));
        nextListOffsetInChunk += listLen;
        offsetBufferToWrite[numValues + i] = nextListOffsetInChunk;
    }
    varListDataColumnChunk->resizeBuffer(nextListOffsetInChunk);
    auto dataVector = ListVector::getDataVector(vector);
    dataVector->setState(std::make_unique<DataChunkState>());
    dataVector->state->selVector->resetSelectorToValuePosBuffer();
    for (auto i = 0u; i < selVector.selectedSize; i++) {
        auto pos = selVector.selectedPositions[i];
        if (vector->isNull(pos)) {
            continue;
        }
        copyListValues(vector->getValue<list_entry_t>(pos), dataVector);
    }
    numValues += numToAppend;
    KU_ASSERT(sizeColumnChunk->getNumValues() == numValues);
    KU_ASSERT(nullChunk->getNumValues() == numValues);
}

void VarListColumnChunk::appendNullList() {
    offset_t nextListOffsetInChunk = varListDataColumnChunk->getNumValues();
    auto offsetBufferToWrite = (offset_t*)(buffer.get());
    sizeColumnChunk->setValue<uint32_t>(0, numValues);
    sizeColumnChunk->getNullChunk()->setNull(numValues, true);
    offsetBufferToWrite[numValues] = nextListOffsetInChunk;
    nullChunk->setNull(numValues, true);
    numValues++;
}

void VarListColumnChunk::lookup(
    offset_t offsetInChunk, ValueVector& output, sel_t posInOutputVector) const {
    KU_ASSERT(offsetInChunk < numValues);
    output.setNull(posInOutputVector, nullChunk->isNull(offsetInChunk));
    if (output.isNull(posInOutputVector)) {
        return;
    }
    auto startOffset = offsetInChunk == 0 ? 0 : getValue<offset_t>(offsetInChunk - 1);
    auto listLen = getListLen(offsetInChunk);
    auto dataVector = ListVector::getDataVector(&output);
    auto currentListDataSize = ListVector::getDataVectorSize(&output);
    ListVector::resizeDataVector(&output, currentListDataSize + listLen);
    // TODO(Guodong): Should add `scan` interface and use `scan` here.
    for (auto i = 0u; i < listLen; i++) {
        varListDataColumnChunk->dataColumnChunk->lookup(startOffset + i, *dataVector, i);
    }
    KU_ASSERT(sizeColumnChunk->getNumValues() == numValues);
    KU_ASSERT(nullChunk->getNumValues() == numValues);
}

void VarListColumnChunk::write(
    ColumnChunk* chunk, ColumnChunk* dstOffsets, RelMultiplicity /*multiplicity*/) {
    KU_ASSERT(chunk->getDataType().getPhysicalType() == dataType.getPhysicalType() &&
              dstOffsets->getDataType().getPhysicalType() == PhysicalTypeID::INT64 &&
              chunk->getNumValues() == dstOffsets->getNumValues());
    offset_t currentIndex = varListDataColumnChunk->getNumValues();
    auto otherListChunk = ku_dynamic_cast<ColumnChunk*, VarListColumnChunk*>(chunk);
    varListDataColumnChunk->resizeBuffer(varListDataColumnChunk->getNumValues() +
                                         otherListChunk->varListDataColumnChunk->getNumValues());
    varListDataColumnChunk->dataColumnChunk->append(
        otherListChunk->varListDataColumnChunk->dataColumnChunk.get(), 0,
        otherListChunk->varListDataColumnChunk->getNumValues());
    offset_t maxDstOffset = 0;
    for (auto i = 0u; i < dstOffsets->getNumValues(); i++) {
        auto posInChunk = dstOffsets->getValue<offset_t>(i);
        if (posInChunk > maxDstOffset) {
            maxDstOffset = posInChunk;
        }
    }
    while (maxDstOffset >= numValues) {
        appendNullList();
    }
    for (auto i = 0u; i < dstOffsets->getNumValues(); i++) {
        auto posInChunk = dstOffsets->getValue<offset_t>(i);
        auto appendLen = otherListChunk->getListLen(i);
        currentIndex += appendLen;
        setValue<offset_t>(currentIndex, posInChunk);
        nullChunk->setNull(posInChunk, otherListChunk->nullChunk->isNull(i));
        sizeColumnChunk->setValue<uint32_t>(appendLen, posInChunk);
        sizeColumnChunk->getNullChunk()->setNull(posInChunk, otherListChunk->nullChunk->isNull(i));
    }
    KU_ASSERT(sizeColumnChunk->getNumValues() == numValues);
    KU_ASSERT(nullChunk->getNumValues() == numValues);
}

void VarListColumnChunk::write(
    ValueVector* vector, offset_t offsetInVector, offset_t offsetInChunk) {
    auto selVector = std::make_unique<SelectionVector>(1);
    selVector->resetSelectorToValuePosBuffer();
    selVector->selectedPositions[0] = offsetInVector;
    auto appendLen =
        vector->isNull(offsetInVector) ? 0 : vector->getValue<list_entry_t>(offsetInVector).size;
    varListDataColumnChunk->resizeBuffer(varListDataColumnChunk->getNumValues() + appendLen);
    auto dataVector = ListVector::getDataVector(vector);
    dataVector->setState(std::make_unique<DataChunkState>());
    dataVector->state->selVector->resetSelectorToValuePosBuffer();
    copyListValues(vector->getValue<list_entry_t>(offsetInVector), dataVector);
    while (offsetInChunk >= numValues) {
        appendNullList();
    }
    auto isNull = vector->isNull(offsetInVector);
    nullChunk->setNull(offsetInChunk, isNull);
    sizeColumnChunk->getNullChunk()->setNull(offsetInChunk, isNull);
    if (!isNull) {
        sizeColumnChunk->setValue<uint32_t>(appendLen, offsetInChunk);
        setValue<offset_t>(varListDataColumnChunk->getNumValues(), offsetInChunk);
    }
    KU_ASSERT(sizeColumnChunk->getNumValues() == numValues);
    KU_ASSERT(nullChunk->getNumValues() == numValues);
}

void VarListColumnChunk::write(ColumnChunk* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->getDataType().getPhysicalType() == PhysicalTypeID::VAR_LIST);
    auto srcListChunk = ku_dynamic_cast<ColumnChunk*, VarListColumnChunk*>(srcChunk);
    auto offsetInDataChunkToAppend = varListDataColumnChunk->getNumValues();
    for (auto i = 0u; i < numValuesToCopy; i++) {
        auto appendListLen = srcListChunk->getListLen(srcOffsetInChunk + i);
        offsetInDataChunkToAppend += appendListLen;
        sizeColumnChunk->setValue<uint32_t>(appendListLen, dstOffsetInChunk + i);
        setValue<offset_t>(offsetInDataChunkToAppend, dstOffsetInChunk + i);
        nullChunk->setNull(
            dstOffsetInChunk + i, srcListChunk->nullChunk->isNull(srcOffsetInChunk + i));
        sizeColumnChunk->getNullChunk()->setNull(
            dstOffsetInChunk + i, srcListChunk->nullChunk->isNull(srcOffsetInChunk + i));
    }
    varListDataColumnChunk->resizeBuffer(offsetInDataChunkToAppend);
    for (auto i = 0u; i < numValuesToCopy; i++) {
        auto startOffsetInSrcChunk = srcListChunk->getListStartOffset(srcOffsetInChunk + i);
        auto appendLen = srcListChunk->getListLen(srcOffsetInChunk + i);
        varListDataColumnChunk->dataColumnChunk->append(
            srcListChunk->varListDataColumnChunk->dataColumnChunk.get(), startOffsetInSrcChunk,
            appendLen);
    }
    KU_ASSERT(sizeColumnChunk->getNumValues() == numValues);
    KU_ASSERT(nullChunk->getNumValues() == numValues);
}

void VarListColumnChunk::copy(ColumnChunk* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->getDataType().getPhysicalType() == PhysicalTypeID::VAR_LIST);
    KU_ASSERT(dstOffsetInChunk >= numValues);
    while (numValues < dstOffsetInChunk) {
        appendNullList();
    }
    append(srcChunk, srcOffsetInChunk, numValuesToCopy);
    KU_ASSERT(sizeColumnChunk->getNumValues() == numValues);
    KU_ASSERT(nullChunk->getNumValues() == numValues);
}

void VarListColumnChunk::copyListValues(const list_entry_t& entry, ValueVector* dataVector) {
    auto numListValuesToCopy = entry.size;
    auto numListValuesCopied = 0u;
    while (numListValuesCopied < numListValuesToCopy) {
        auto numListValuesToCopyInBatch =
            std::min<uint64_t>(numListValuesToCopy - numListValuesCopied, DEFAULT_VECTOR_CAPACITY);
        dataVector->state->selVector->selectedSize = numListValuesToCopyInBatch;
        for (auto j = 0u; j < numListValuesToCopyInBatch; j++) {
            dataVector->state->selVector->selectedPositions[j] =
                entry.offset + numListValuesCopied + j;
        }
        varListDataColumnChunk->append(dataVector, *dataVector->state->selVector);
        numListValuesCopied += numListValuesToCopyInBatch;
    }
}

void VarListColumnChunk::resetOffset() {
    offset_t nextListOffsetReset = 0;
    for (auto i = 0u; i < numValues; i++) {
        auto listLen = getListLen(i);
        nextListOffsetReset += uint64_t(listLen);
        setValue<offset_t>(nextListOffsetReset, i);
        sizeColumnChunk->setValue<uint32_t>(listLen, i);
    }
}

} // namespace storage
} // namespace kuzu

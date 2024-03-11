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
        capacity *= CHUNK_RESIZE_RATIO;
    }
    dataColumnChunk->resize(capacity);
}

VarListColumnChunk::VarListColumnChunk(
    LogicalType dataType, uint64_t capacity, bool enableCompression, bool inMemory)
    : ColumnChunk{std::move(dataType), capacity, enableCompression, true /* hasNullChunk */},
      needFinalize{false} {
    sizeColumnChunk = ColumnChunkFactory::createColumnChunk(
        *common::LogicalType::INT64(), enableCompression, capacity);
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
    auto offsetInDataChunkToAppend = varListDataColumnChunk->getNumValues();
    for (auto i = 0u; i < numValuesToAppend; i++) {
        uint64_t appendListLen = otherListChunk->getListLen(startPosInOtherChunk + i);
        sizeColumnChunk->setValue(appendListLen, numValues);
        offsetInDataChunkToAppend += appendListLen;
        setValue(offsetInDataChunkToAppend, numValues);
    }
    auto startOffset = otherListChunk->getListStartOffset(startPosInOtherChunk);
    auto endOffset = otherListChunk->getListStartOffset(startPosInOtherChunk + numValuesToAppend);
    KU_ASSERT(endOffset >= startOffset);
    varListDataColumnChunk->resizeBuffer(offsetInDataChunkToAppend);
    varListDataColumnChunk->dataColumnChunk->append(
        otherListChunk->varListDataColumnChunk->dataColumnChunk.get(), startOffset,
        endOffset - startOffset);
}

void VarListColumnChunk::resetToEmpty() {
    ColumnChunk::resetToEmpty();
    sizeColumnChunk = ColumnChunkFactory::createColumnChunk(
        *common::LogicalType::INT64(), enableCompression, capacity);
    sizeColumnChunk->resetToEmpty();
    varListDataColumnChunk = std::make_unique<VarListDataColumnChunk>(
        ColumnChunkFactory::createColumnChunk(*VarListType::getChildType(&this->dataType)->copy(),
            enableCompression, 0 /* capacity */));
}

void VarListColumnChunk::append(ValueVector* vector, SelectionVector& selVector) {
    auto numToAppend = selVector.selectedSize;
    auto newCapacity = capacity;
    while (numValues + numToAppend >= newCapacity) {
        newCapacity *= 1.5;
    }
    if (capacity < newCapacity) {
        resize(newCapacity);
    }
    auto nextListOffsetInChunk = varListDataColumnChunk->getNumValues();
    auto offsetBufferToWrite = (offset_t*)(buffer.get());
    for (auto i = 0u; i < selVector.selectedSize; i++) {
        auto pos = selVector.selectedPositions[i];
        uint64_t listLen = vector->isNull(pos) ? 0 : vector->getValue<list_entry_t>(pos).size;
        sizeColumnChunk->setValue(listLen, numValues + i);
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
}

void VarListColumnChunk::appendNullList() {
    offset_t nextListOffsetInChunk = varListDataColumnChunk->getNumValues();
    auto offsetBufferToWrite = (offset_t*)(buffer.get());
    sizeColumnChunk->setValue(int64_t(0), numValues);
    sizeColumnChunk->getNullChunk()->setNull(numValues, true);
    offsetBufferToWrite[numValues] = nextListOffsetInChunk;
    nullChunk->setNull(numValues, true);
    numValues++;
}

void VarListColumnChunk::write(ColumnChunk* chunk, ColumnChunk* dstOffsets, bool /*isCSR*/) {
    needFinalize = true;
    if (!indicesColumnChunk) {
        initializeIndices();
    }
    KU_ASSERT(chunk->getDataType().getPhysicalType() == dataType.getPhysicalType() &&
              dstOffsets->getDataType().getPhysicalType() == PhysicalTypeID::INT64 &&
              chunk->getNumValues() == dstOffsets->getNumValues());
    auto currentIndex = numValues;
    append(chunk, 0, chunk->getNumValues());
    for (auto i = 0u; i < dstOffsets->getNumValues(); i++) {
        auto posInChunk = dstOffsets->getValue<offset_t>(i);
        KU_ASSERT(posInChunk < capacity);
        indicesColumnChunk->setValue<int64_t>(currentIndex++, posInChunk);
        indicesColumnChunk->getNullChunk()->setNull(posInChunk, false);
        if (indicesColumnChunk->getNumValues() <= posInChunk) {
            indicesColumnChunk->setNumValues(posInChunk + 1);
        }
    }
    KU_ASSERT(currentIndex == numValues &&
              indicesColumnChunk->getNumValues() <= indicesColumnChunk->getCapacity());
}

void VarListColumnChunk::write(
    ValueVector* vector, offset_t offsetInVector, offset_t offsetInChunk) {
    needFinalize = true;
    if (!indicesColumnChunk) {
        initializeIndices();
    }
    auto currentIndex = numValues;
    append(vector, *vector->state->selVector);
    KU_ASSERT(offsetInChunk < capacity);
    indicesColumnChunk->setValue(currentIndex, offsetInChunk);
    indicesColumnChunk->getNullChunk()->setNull(offsetInChunk, vector->isNull(offsetInVector));
    if (indicesColumnChunk->getNumValues() <= offsetInChunk) {
        indicesColumnChunk->setNumValues(offsetInChunk + 1);
    }
    KU_ASSERT(sizeColumnChunk->getNumValues() == numValues);
}

void VarListColumnChunk::write(ColumnChunk* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->getDataType().getPhysicalType() == PhysicalTypeID::VAR_LIST);
    needFinalize = true;
    if (!indicesColumnChunk) {
        initializeIndices();
    }
    nullChunk->write(srcChunk->getNullChunk(), srcOffsetInChunk, dstOffsetInChunk, numValuesToCopy);
    sizeColumnChunk->getNullChunk()->write(
        srcChunk->getNullChunk(), srcOffsetInChunk, dstOffsetInChunk, numValuesToCopy);
    auto srcListChunk = ku_dynamic_cast<ColumnChunk*, VarListColumnChunk*>(srcChunk);
    auto offsetInDataChunkToAppend = varListDataColumnChunk->getNumValues();
    auto currentIndex = numValues;
    for (auto i = 0u; i < numValuesToCopy; i++) {
        uint64_t appendListLen = srcListChunk->getListLen(srcOffsetInChunk + i);
        sizeColumnChunk->setValue(appendListLen, currentIndex + i);
        offsetInDataChunkToAppend += appendListLen;
        setValue<offset_t>(offsetInDataChunkToAppend, currentIndex + i);
        indicesColumnChunk->setValue<offset_t>(currentIndex + i, dstOffsetInChunk + i);
        indicesColumnChunk->getNullChunk()->setNull(dstOffsetInChunk + i, false);
    }
    varListDataColumnChunk->resizeBuffer(offsetInDataChunkToAppend);
    auto startOffsetInSrcChunk = srcListChunk->getListStartOffset(srcOffsetInChunk);
    auto endOffsetInSrcChunk = srcListChunk->getListStartOffset(srcOffsetInChunk + numValuesToCopy);
    varListDataColumnChunk->dataColumnChunk->append(
        srcListChunk->varListDataColumnChunk->dataColumnChunk.get(), startOffsetInSrcChunk,
        endOffsetInSrcChunk - startOffsetInSrcChunk);
    if (indicesColumnChunk->getNumValues() < dstOffsetInChunk + numValuesToCopy) {
        indicesColumnChunk->setNumValues(dstOffsetInChunk + numValuesToCopy + 1);
    }
    KU_ASSERT(sizeColumnChunk->getNumValues() == numValues);
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

void VarListColumnChunk::finalize() {
    if (!needFinalize) {
        return;
    }
    auto newColumnChunk = ColumnChunkFactory::createColumnChunk(
        std::move(*dataType.copy()), enableCompression, capacity);
    auto newVarListChunk = ku_dynamic_cast<ColumnChunk*, VarListColumnChunk*>(newColumnChunk.get());
    auto totalListLen = varListDataColumnChunk->getNumValues();
    newVarListChunk->getDataColumnChunk()->resize(totalListLen);
    for (auto i = 0u; i < indicesColumnChunk->getNumValues(); i++) {
        if (indicesColumnChunk->getNullChunk()->isNull(i)) {
            newVarListChunk->appendNullList();
        } else {
            auto index = indicesColumnChunk->getValue<offset_t>(i);
            newVarListChunk->append(this, index, 1);
        }
    }
    // Move offsets, null, data from newVarListChunk to this column chunk. And release indices.
    resetFromOtherChunk(newVarListChunk);
    KU_ASSERT(sizeColumnChunk->getNumValues() == numValues);
}

void VarListColumnChunk::resetFromOtherChunk(VarListColumnChunk* other) {
    other->resetOffset();
    buffer = std::move(other->buffer);
    nullChunk = std::move(other->nullChunk);
    sizeColumnChunk = std::move(other->sizeColumnChunk);
    varListDataColumnChunk = std::move(other->varListDataColumnChunk);
    numValues = other->numValues;
    // Reset indices and needFinalize.
    indicesColumnChunk.reset();
    needFinalize = false;
}

void VarListColumnChunk::resetOffset() {
    uint64_t nextListOffsetReset = 0;
    for (auto i = 0u; i < numValues; i++) {
        int64_t listLen = getListLen(i);
        nextListOffsetReset += uint64_t(listLen);
        setValue(nextListOffsetReset, i);
        sizeColumnChunk->setValue(listLen, i);
    }
}

} // namespace storage
} // namespace kuzu

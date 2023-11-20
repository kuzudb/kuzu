#include "storage/store/var_list_column_chunk.h"

#include "common/cast.h"
#include "common/types/value/value.h"

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
    std::unique_ptr<LogicalType> dataType, uint64_t capacity, bool enableCompression)
    : ColumnChunk{std::move(dataType), capacity, enableCompression, true /* hasNullChunk */},
      enableCompression{enableCompression}, needFinalize{false} {
    varListDataColumnChunk =
        std::make_unique<VarListDataColumnChunk>(ColumnChunkFactory::createColumnChunk(
            VarListType::getChildType(this->dataType.get())->copy(), enableCompression,
            0 /* capacity */));
    KU_ASSERT(this->dataType->getPhysicalType() == PhysicalTypeID::VAR_LIST);
}

void VarListColumnChunk::append(
    ColumnChunk* other, offset_t startPosInOtherChunk, uint32_t numValuesToAppend) {
    nullChunk->append(other->getNullChunk(), startPosInOtherChunk, numValuesToAppend);
    auto otherListChunk = reinterpret_cast<VarListColumnChunk*>(other);
    auto offsetInDataChunkToAppend = varListDataColumnChunk->getNumValues();
    for (auto i = 0u; i < numValuesToAppend; i++) {
        offsetInDataChunkToAppend += otherListChunk->getListLen(startPosInOtherChunk + i);
        setValue(offsetInDataChunkToAppend, numValues + i);
    }
    auto startOffset = otherListChunk->getListOffset(startPosInOtherChunk);
    auto endOffset = otherListChunk->getListOffset(startPosInOtherChunk + numValuesToAppend);
    varListDataColumnChunk->resizeBuffer(offsetInDataChunkToAppend);
    varListDataColumnChunk->dataColumnChunk->append(
        otherListChunk->varListDataColumnChunk->dataColumnChunk.get(), startOffset,
        endOffset - startOffset);
    numValues += numValuesToAppend;
}

void VarListColumnChunk::resetToEmpty() {
    ColumnChunk::resetToEmpty();
    varListDataColumnChunk.reset();
}

void VarListColumnChunk::append(ValueVector* vector) {
    auto nextListOffsetInChunk = getListOffset(numValues);
    auto offsetBufferToWrite = (offset_t*)(buffer.get());
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto pos = vector->state->selVector->selectedPositions[i];
        uint64_t listLen = vector->isNull(pos) ? 0 : vector->getValue<list_entry_t>(pos).size;
        nullChunk->setNull(numValues + i, vector->isNull(pos));
        nextListOffsetInChunk += listLen;
        offsetBufferToWrite[numValues + i] = nextListOffsetInChunk;
    }
    varListDataColumnChunk->resizeBuffer(nextListOffsetInChunk);
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

void VarListColumnChunk::appendNullList() {
    auto nextListOffsetInChunk = getListOffset(numValues);
    auto offsetBufferToWrite = (offset_t*)(buffer.get());
    offsetBufferToWrite[numValues] = nextListOffsetInChunk;
    nullChunk->setNull(numValues, true);
    numValues++;
}

void VarListColumnChunk::write(
    ValueVector* valueVector, ValueVector* offsetInChunkVector, bool /*isCSR*/) {
    needFinalize = true;
    if (!indicesColumnChunk) {
        initializeIndices();
    }
    KU_ASSERT(valueVector->dataType.getPhysicalType() == dataType->getPhysicalType() &&
              offsetInChunkVector->dataType.getPhysicalType() == PhysicalTypeID::INT64 &&
              valueVector->state->selVector->selectedSize ==
                  offsetInChunkVector->state->selVector->selectedSize);
    auto currentIndex = numValues;
    append(valueVector);
    for (auto i = 0u; i < offsetInChunkVector->state->selVector->selectedSize; i++) {
        auto posInChunk = offsetInChunkVector->getValue<offset_t>(
            offsetInChunkVector->state->selVector->selectedPositions[i]);
        KU_ASSERT(posInChunk < capacity);
        indicesColumnChunk->setValue(currentIndex++, posInChunk);
        indicesColumnChunk->getNullChunk()->setNull(posInChunk, false);
        if (indicesColumnChunk->getNumValues() <= posInChunk) {
            indicesColumnChunk->setNumValues(posInChunk + 1);
        }
    }
    KU_ASSERT(currentIndex == numValues &&
              indicesColumnChunk->getNumValues() < indicesColumnChunk->getCapacity());
}

void VarListColumnChunk::write(
    ValueVector* vector, offset_t /*offsetInVector*/, offset_t offsetInChunk) {
    needFinalize = true;
    if (!indicesColumnChunk) {
        initializeIndices();
    }
    auto currentIndex = numValues;
    append(vector);
    KU_ASSERT(offsetInChunk < capacity);
    indicesColumnChunk->setValue(currentIndex, offsetInChunk);
    indicesColumnChunk->getNullChunk()->setNull(offsetInChunk, false);
    if (indicesColumnChunk->getNumValues() <= offsetInChunk) {
        indicesColumnChunk->setNumValues(offsetInChunk + 1);
    }
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
        varListDataColumnChunk->append(dataVector);
        numListValuesCopied += numListValuesToCopyInBatch;
    }
}

void VarListColumnChunk::finalize() {
    if (!needFinalize) {
        return;
    }
    auto newColumnChunk =
        ColumnChunkFactory::createColumnChunk(dataType->copy(), enableCompression, capacity);
    auto totalListLen = getListOffset(numValues);
    auto newVarListChunk = ku_dynamic_cast<ColumnChunk*, VarListColumnChunk*>(newColumnChunk.get());
    newVarListChunk->getDataColumnChunk()->resize(totalListLen);
    for (auto i = 0u; i < indicesColumnChunk->getNumValues(); i++) {
        if (indicesColumnChunk->getNullChunk()->isNull(i)) {
            newVarListChunk->appendNullList();
        } else {
            auto index = indicesColumnChunk->getValue<offset_t>(i);
            newColumnChunk->append(this, index, 1);
        }
    }
    // Move offsets, null, data from newVarListChunk to this column chunk. And release indices.
    resetFromOtherChunk(newVarListChunk);
}

void VarListColumnChunk::resetFromOtherChunk(VarListColumnChunk* other) {
    buffer = std::move(other->buffer);
    nullChunk = std::move(other->nullChunk);
    varListDataColumnChunk = std::move(other->varListDataColumnChunk);
    numValues = other->numValues;
    // Reset indices and needFinalize.
    indicesColumnChunk.reset();
    needFinalize = false;
}

} // namespace storage
} // namespace kuzu

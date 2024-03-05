#include "storage/store/array_column_chunk.h"

#include <iostream>

#include "common/cast.h"
#include "common/types/value/value.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void ArrayDataColumnChunk::reset() const {
    dataColumnChunk->resetToEmpty();
}

void ArrayDataColumnChunk::resizeBuffer(uint64_t numValues) {
    if (numValues <= capacity) {
        return;
    }
    capacity = capacity == 0 ? 1 : capacity;
    while (capacity < numValues) {
        capacity *= CHUNK_RESIZE_RATIO;
    }
    dataColumnChunk->resize(capacity);
}

ArrayColumnChunk::ArrayColumnChunk(
    common::LogicalType dataType, uint64_t capacity, bool enableCompression)
    : ColumnChunk{std::move(dataType), capacity, enableCompression, true /* hasNullChunk */},
      needFinalize{false} {
    arrayDataColumnChunk =
        std::make_unique<ArrayDataColumnChunk>(ColumnChunkFactory::createColumnChunk(
            *LogicalType::BLOB(), enableCompression, 0 /* capacity */));
    KU_ASSERT(this->dataType.getPhysicalType() == PhysicalTypeID::ARRAY);
}

void ArrayColumnChunk::append(
    ColumnChunk* other, offset_t startPosInOtherChunk, uint32_t numValuesToAppend) {
    std::cout << "append here1\n";
    nullChunk->append(other->getNullChunk(), startPosInOtherChunk, numValuesToAppend);
    auto otherListChunk = ku_dynamic_cast<ColumnChunk*, ArrayColumnChunk*>(other);
    auto offsetInDataChunkToAppend = arrayDataColumnChunk->getNumValues();
    KU_ASSERT(numValues + numValuesToAppend <= capacity);
    for (auto i = 0u; i < numValuesToAppend; i++) {
        offsetInDataChunkToAppend += otherListChunk->getListLen(startPosInOtherChunk + i);
        setValue(offsetInDataChunkToAppend, numValues);
    }
    auto startOffset = otherListChunk->getListOffset(startPosInOtherChunk);
    auto endOffset = otherListChunk->getListOffset(startPosInOtherChunk + numValuesToAppend);
    KU_ASSERT(endOffset >= startOffset);
    arrayDataColumnChunk->resizeBuffer(offsetInDataChunkToAppend);
    arrayDataColumnChunk->dataColumnChunk->append(
        otherListChunk->arrayDataColumnChunk->dataColumnChunk.get(), startOffset,
        endOffset - startOffset);
}

void ArrayColumnChunk::resetToEmpty() {
    ColumnChunk::resetToEmpty();
    arrayDataColumnChunk =
        std::make_unique<ArrayDataColumnChunk>(ColumnChunkFactory::createColumnChunk(
            *LogicalType::BLOB(), enableCompression, 0 /* capacity */));
}

// TODO(Jimain)
void ArrayColumnChunk::append(ValueVector* vector) {
    std::cout << "append here2\n";
    auto numToAppend = vector->state->selVector->selectedSize;
    auto newCapacity = capacity;
    while (numValues + numToAppend >= newCapacity) {
        newCapacity *= 1.5;
    }
    if (capacity != newCapacity) {
        resize(newCapacity);
    }
    // simplfy: just get from listinfo?
    auto nextListOffsetInChunk = getListOffset(numValues);
    uint64_t listLen = 5; // sizeof(LogicalType::BLOB());
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto pos = vector->state->selVector->selectedPositions[i];
        nullChunk->setNull(numValues + i, vector->isNull(pos));
        nextListOffsetInChunk += listLen;
    }
    arrayDataColumnChunk->resizeBuffer(nextListOffsetInChunk);
    //    auto offsetBufferToWrite = (offset_t*)(buffer.get());
    //    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
    //        auto pos = vector->state->selVector->selectedPositions[i];
    //        uint64_t listLen = vector->isNull(pos) ? 0 : vector->getValue<list_entry_t>(pos).size;
    //        nullChunk->setNull(numValues + i, vector->isNull(pos));
    //        nextListOffsetInChunk += listLen;
    //        offsetBufferToWrite[numValues + i] = nextListOffsetInChunk;
    //    }
    //    arrayDataColumnChunk->resizeBuffer(nextListOffsetInChunk);
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

void ArrayColumnChunk::appendNullList() {
    auto nextListOffsetInChunk = getListOffset(numValues);
    auto offsetBufferToWrite = (offset_t*)(buffer.get());
    offsetBufferToWrite[numValues] = nextListOffsetInChunk;
    nullChunk->setNull(numValues, true);
    numValues++;
}

void ArrayColumnChunk::write(
    ValueVector* valueVector, ValueVector* offsetInChunkVector, bool /*isCSR*/) {
    std::cout << "write here1\n";
    needFinalize = true;
    //    if (!indicesColumnChunk) {
    //        initializeIndices();
    //    }
    KU_ASSERT(valueVector->dataType.getPhysicalType() == dataType.getPhysicalType() &&
              offsetInChunkVector->dataType.getPhysicalType() == PhysicalTypeID::INT64 &&
              valueVector->state->selVector->selectedSize ==
                  offsetInChunkVector->state->selVector->selectedSize);
    auto currentIndex = numValues;
    append(valueVector);
    for (auto i = 0u; i < offsetInChunkVector->state->selVector->selectedSize; i++) {
        auto posInChunk = offsetInChunkVector->getValue<offset_t>(
            offsetInChunkVector->state->selVector->selectedPositions[i]);
        KU_ASSERT(posInChunk < capacity);
        //        indicesColumnChunk->setValue<int64_t>(currentIndex++, posInChunk);
        //        indicesColumnChunk->getNullChunk()->setNull(posInChunk, false);
        //        if (indicesColumnChunk->getNumValues() <= posInChunk) {
        //            indicesColumnChunk->setNumValues(posInChunk + 1);
        //        }
    }
    KU_ASSERT(currentIndex == numValues);
    //    &&
    //        indicesColumnChunk->getNumValues() <= indicesColumnChunk->getCapacity()
}

void ArrayColumnChunk::write(ValueVector* vector, offset_t offsetInVector, offset_t offsetInChunk) {
    std::cout << "write here2\n";
    needFinalize = true;
    //    if (!indicesColumnChunk) {
    //        initializeIndices();
    //    }
    auto currentIndex = numValues;
    append(vector);
    KU_ASSERT(offsetInChunk < capacity);
    //    indicesColumnChunk->setValue(currentIndex, offsetInChunk);
    //    indicesColumnChunk->getNullChunk()->setNull(offsetInChunk,
    //    vector->isNull(offsetInVector)); if (indicesColumnChunk->getNumValues() <= offsetInChunk)
    //    {
    //        indicesColumnChunk->setNumValues(offsetInChunk + 1);
    //    }
}

void ArrayColumnChunk::write(ColumnChunk* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    std::cout << "write here3\n";
    KU_ASSERT(srcChunk->getDataType().getPhysicalType() == PhysicalTypeID::VAR_LIST);
    needFinalize = true;
    //    if (!indicesColumnChunk) {
    //        initializeIndices();
    //    }
    nullChunk->write(srcChunk->getNullChunk(), srcOffsetInChunk, dstOffsetInChunk, numValuesToCopy);
    auto srcListChunk = ku_dynamic_cast<ColumnChunk*, ArrayColumnChunk*>(srcChunk);
    auto offsetInDataChunkToAppend = arrayDataColumnChunk->getNumValues();
    auto currentIndex = numValues;
    for (auto i = 0u; i < numValuesToCopy; i++) {
        offsetInDataChunkToAppend += srcListChunk->getListLen(srcOffsetInChunk + i);
        setValue<offset_t>(offsetInDataChunkToAppend, currentIndex + i);
        //        indicesColumnChunk->setValue<offset_t>(currentIndex + i, dstOffsetInChunk + i);
        //        indicesColumnChunk->getNullChunk()->setNull(dstOffsetInChunk + i, false);
    }
    arrayDataColumnChunk->resizeBuffer(offsetInDataChunkToAppend);
    auto startOffsetInSrcChunk = srcListChunk->getListOffset(srcOffsetInChunk);
    auto endOffsetInSrcChunk = srcListChunk->getListOffset(srcOffsetInChunk + numValuesToCopy);
    arrayDataColumnChunk->dataColumnChunk->append(
        srcListChunk->arrayDataColumnChunk->dataColumnChunk.get(), startOffsetInSrcChunk,
        endOffsetInSrcChunk - startOffsetInSrcChunk);
    //    if (indicesColumnChunk->getNumValues() < dstOffsetInChunk + numValuesToCopy) {
    //        indicesColumnChunk->setNumValues(dstOffsetInChunk + numValuesToCopy + 1);
    //    }
}

void ArrayColumnChunk::copy(ColumnChunk* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    std::cout << "copy here1\n";
    KU_ASSERT(srcChunk->getDataType().getPhysicalType() == PhysicalTypeID::ARRAY);
    KU_ASSERT(dstOffsetInChunk >= numValues);
    while (numValues < dstOffsetInChunk) {
        appendNullList();
    }
    append(srcChunk, srcOffsetInChunk, numValuesToCopy);
}

void ArrayColumnChunk::copyListValues(const list_entry_t& entry, ValueVector* dataVector) {
    std::cout << "copy here2\n";
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
        arrayDataColumnChunk->append(dataVector);
        numListValuesCopied += numListValuesToCopyInBatch;
    }
}

void ArrayColumnChunk::finalize() {
    std::cout << "finalize1\n";
    if (!needFinalize) {
        return;
    }
    auto newColumnChunk = ColumnChunkFactory::createColumnChunk(
        std::move(*dataType.copy()), enableCompression, capacity);
    auto totalListLen = getListOffset(numValues) + 1;
    auto newArrayChunk = ku_dynamic_cast<ColumnChunk*, ArrayColumnChunk*>(newColumnChunk.get());
    newArrayChunk->getDataColumnChunk()->resize(totalListLen);
    //    for (auto i = 0u; i < indicesColumnChunk->getNumValues(); i++) {
    //        if (indicesColumnChunk->getNullChunk()->isNull(i)) {
    //            newVarListChunk->appendNullList();
    //        } else {
    //            auto index = indicesColumnChunk->getValue<offset_t>(i);
    //            newColumnChunk->append(this, index, 1);
    //        }
    //    }
    // Move offsets, null, data from newVarListChunk to this column chunk. And release indices.
    resetFromOtherChunk(newArrayChunk);
}

void ArrayColumnChunk::resetFromOtherChunk(ArrayColumnChunk* other) {
    buffer = std::move(other->buffer);
    nullChunk = std::move(other->nullChunk);
    arrayDataColumnChunk = std::move(other->arrayDataColumnChunk);
    numValues = other->numValues;
    // Reset needFinalize.
    needFinalize = false;
}

} // namespace storage
} // namespace kuzu

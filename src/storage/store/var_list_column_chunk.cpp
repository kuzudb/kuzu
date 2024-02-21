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
    checkOffsetSortedAsc = false;
    KU_ASSERT(this->dataType.getPhysicalType() == PhysicalTypeID::VAR_LIST);
}

bool VarListColumnChunk::isOffsetsConsecutiveAndSortedAscending(
    uint64_t startPos, uint64_t endPos) const {
    offset_t prevEndOffset = getListStartOffset(startPos);
    for (auto i = startPos; i < endPos; i++) {
        offset_t currentEndOffset = getListEndOffset(i);
        auto size = getListSize(i);
        prevEndOffset += size;
        if (currentEndOffset != prevEndOffset) {
            return false;
        }
    }
    return true;
}

offset_t VarListColumnChunk::getListStartOffset(offset_t offset) const {
    if (numValues == 0)
        return 0;
    return offset == numValues ? getListEndOffset(offset - 1) :
                                 getListEndOffset(offset) - getListSize(offset);
}

offset_t VarListColumnChunk::getListEndOffset(offset_t offset) const {
    if (numValues == 0)
        return 0;
    KU_ASSERT(offset < numValues);
    return getValue<uint64_t>(offset);
}

list_size_t VarListColumnChunk::getListSize(common::offset_t offset) const {
    if (numValues == 0)
        return 0;
    KU_ASSERT(offset < sizeColumnChunk->getNumValues());
    return sizeColumnChunk->getValue<list_size_t>(offset);
}

void VarListColumnChunk::append(
    ColumnChunk* other, offset_t startPosInOtherChunk, uint32_t numValuesToAppend) {
    checkOffsetSortedAsc = true;
    auto otherListChunk = ku_dynamic_cast<ColumnChunk*, VarListColumnChunk*>(other);
    nullChunk->append(other->getNullChunk(), startPosInOtherChunk, numValuesToAppend);
    sizeColumnChunk->getNullChunk()->append(
        other->getNullChunk(), startPosInOtherChunk, numValuesToAppend);
    offset_t offsetInDataChunkToAppend = varListDataColumnChunk->getNumValues();
    for (auto i = 0u; i < numValuesToAppend; i++) {
        auto appendSize = otherListChunk->getListSize(startPosInOtherChunk + i);
        sizeColumnChunk->setValue<list_size_t>(appendSize, numValues);
        offsetInDataChunkToAppend += appendSize;
        setValue<offset_t>(offsetInDataChunkToAppend, numValues);
    }
    varListDataColumnChunk->resizeBuffer(offsetInDataChunkToAppend);
    for (auto i = 0u; i < numValuesToAppend; i++) {
        auto startOffset = otherListChunk->getListStartOffset(startPosInOtherChunk + i);
        auto appendSize = otherListChunk->getListSize(startPosInOtherChunk + i);
        varListDataColumnChunk->dataColumnChunk->append(
            otherListChunk->varListDataColumnChunk->dataColumnChunk.get(), startOffset, appendSize);
    }
    sanityCheck();
}

void VarListColumnChunk::resetToEmpty() {
    ColumnChunk::resetToEmpty();
    sizeColumnChunk->resetToEmpty();
    varListDataColumnChunk = std::make_unique<VarListDataColumnChunk>(
        ColumnChunkFactory::createColumnChunk(*VarListType::getChildType(&this->dataType)->copy(),
            enableCompression, 0 /* capacity */));
}

void VarListColumnChunk::append(ValueVector* vector, const SelectionVector& selVector) {
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
        sizeColumnChunk->setValue<list_size_t>(listLen, numValues + i);
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
    sanityCheck();
}

void VarListColumnChunk::appendNullList() {
    offset_t nextListOffsetInChunk = varListDataColumnChunk->getNumValues();
    auto offsetBufferToWrite = (offset_t*)(buffer.get());
    sizeColumnChunk->setValue<list_size_t>(0, numValues);
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
    auto listSize = getListSize(offsetInChunk);
    auto dataVector = ListVector::getDataVector(&output);
    auto currentListDataSize = ListVector::getDataVectorSize(&output);
    ListVector::resizeDataVector(&output, currentListDataSize + listSize);
    // TODO(Guodong): Should add `scan` interface and use `scan` here.
    for (auto i = 0u; i < listSize; i++) {
        varListDataColumnChunk->dataColumnChunk->lookup(startOffset + i, *dataVector, i);
    }
}

void VarListColumnChunk::write(
    ColumnChunk* chunk, ColumnChunk* dstOffsets, RelMultiplicity /*multiplicity*/) {
    KU_ASSERT(chunk->getDataType().getPhysicalType() == dataType.getPhysicalType() &&
              dstOffsets->getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID &&
              chunk->getNumValues() == dstOffsets->getNumValues());
    checkOffsetSortedAsc = true;
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
        auto appendSize = otherListChunk->getListSize(i);
        currentIndex += appendSize;
        setValue<offset_t>(currentIndex, posInChunk);
        nullChunk->setNull(posInChunk, otherListChunk->nullChunk->isNull(i));
        sizeColumnChunk->setValue<list_size_t>(appendSize, posInChunk);
        sizeColumnChunk->getNullChunk()->setNull(posInChunk, otherListChunk->nullChunk->isNull(i));
    }
    sanityCheck();
}

void VarListColumnChunk::write(
    ValueVector* vector, offset_t offsetInVector, offset_t offsetInChunk) {
    checkOffsetSortedAsc = true;
    auto selVector = std::make_unique<SelectionVector>(1);
    selVector->resetSelectorToValuePosBuffer();
    selVector->selectedPositions[0] = offsetInVector;
    auto appendSize =
        vector->isNull(offsetInVector) ? 0 : vector->getValue<list_entry_t>(offsetInVector).size;
    varListDataColumnChunk->resizeBuffer(varListDataColumnChunk->getNumValues() + appendSize);
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
        sizeColumnChunk->setValue<list_size_t>(appendSize, offsetInChunk);
        setValue<offset_t>(varListDataColumnChunk->getNumValues(), offsetInChunk);
    }
    sanityCheck();
}

void VarListColumnChunk::write(ColumnChunk* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->getDataType().getPhysicalType() == PhysicalTypeID::VAR_LIST);
    checkOffsetSortedAsc = true;
    auto srcListChunk = ku_dynamic_cast<ColumnChunk*, VarListColumnChunk*>(srcChunk);
    auto offsetInDataChunkToAppend = varListDataColumnChunk->getNumValues();
    for (auto i = 0u; i < numValuesToCopy; i++) {
        auto appendSize = srcListChunk->getListSize(srcOffsetInChunk + i);
        offsetInDataChunkToAppend += appendSize;
        sizeColumnChunk->setValue<list_size_t>(appendSize, dstOffsetInChunk + i);
        setValue<offset_t>(offsetInDataChunkToAppend, dstOffsetInChunk + i);
        nullChunk->setNull(
            dstOffsetInChunk + i, srcListChunk->nullChunk->isNull(srcOffsetInChunk + i));
        sizeColumnChunk->getNullChunk()->setNull(
            dstOffsetInChunk + i, srcListChunk->nullChunk->isNull(srcOffsetInChunk + i));
    }
    varListDataColumnChunk->resizeBuffer(offsetInDataChunkToAppend);
    for (auto i = 0u; i < numValuesToCopy; i++) {
        auto startOffsetInSrcChunk = srcListChunk->getListStartOffset(srcOffsetInChunk + i);
        auto appendSize = srcListChunk->getListSize(srcOffsetInChunk + i);
        varListDataColumnChunk->dataColumnChunk->append(
            srcListChunk->varListDataColumnChunk->dataColumnChunk.get(), startOffsetInSrcChunk,
            appendSize);
    }
    sanityCheck();
}

void VarListColumnChunk::copy(ColumnChunk* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->getDataType().getPhysicalType() == PhysicalTypeID::VAR_LIST);
    KU_ASSERT(dstOffsetInChunk >= numValues);
    while (numValues < dstOffsetInChunk) {
        appendNullList();
    }
    append(srcChunk, srcOffsetInChunk, numValuesToCopy);
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
        auto listSize = getListSize(i);
        nextListOffsetReset += uint64_t(listSize);
        setValue<offset_t>(nextListOffsetReset, i);
        sizeColumnChunk->setValue<list_size_t>(listSize, i);
    }
}

void VarListColumnChunk::finalize() {
    // rewrite the column chunk for better scanning performance
    auto newColumnChunk = ColumnChunkFactory::createColumnChunk(
        std::move(*dataType.copy()), enableCompression, capacity);
    uint64_t totalListLen = varListDataColumnChunk->getNumValues();
    uint64_t resizeThreshold = varListDataColumnChunk->capacity / 2;
    // if the list is not very long, we do not need to rewrite
    if (totalListLen < resizeThreshold) {
        return;
    }
    // if we do not trigger random write, we do not need to rewrite
    if (!checkOffsetSortedAsc) {
        return;
    }
    // if the list is in ascending order, we do not need to rewrite
    if (isOffsetsConsecutiveAndSortedAscending(0, numValues)) {
        return;
    }
    auto newVarListChunk = ku_dynamic_cast<ColumnChunk*, VarListColumnChunk*>(newColumnChunk.get());
    newVarListChunk->resize(numValues);
    newVarListChunk->getDataColumnChunk()->resize(totalListLen);
    auto dataColumnChunk = newVarListChunk->getDataColumnChunk();
    newVarListChunk->varListDataColumnChunk->resizeBuffer(totalListLen);
    offset_t offsetInChunk = 0;
    offset_t currentIndex = 0;
    for (auto i = 0u; i < numValues; i++) {
        if (nullChunk->isNull(i)) {
            newVarListChunk->appendNullList();
        } else {
            auto startOffset = getListStartOffset(i);
            auto listSize = getListSize(i);
            dataColumnChunk->append(
                varListDataColumnChunk->dataColumnChunk.get(), startOffset, listSize);
            offsetInChunk += listSize;
            newVarListChunk->getNullChunk()->setNull(currentIndex, false);
            newVarListChunk->sizeColumnChunk->getNullChunk()->setNull(currentIndex, false);
            newVarListChunk->sizeColumnChunk->setValue<list_size_t>(listSize, currentIndex);
            newVarListChunk->setValue<offset_t>(offsetInChunk, currentIndex);
        }
        currentIndex++;
    }
    newVarListChunk->sanityCheck();
    // Move offsets, null, data from newVarListChunk to this column chunk. And release indices.
    resetFromOtherChunk(newVarListChunk);
}
void VarListColumnChunk::resetFromOtherChunk(VarListColumnChunk* other) {
    buffer = std::move(other->buffer);
    nullChunk = std::move(other->nullChunk);
    sizeColumnChunk = std::move(other->sizeColumnChunk);
    varListDataColumnChunk = std::move(other->varListDataColumnChunk);
    numValues = other->numValues;
    checkOffsetSortedAsc = false;
}

bool VarListColumnChunk::sanityCheck() {
    KU_ASSERT(ColumnChunk::sanityCheck());
    KU_ASSERT(sizeColumnChunk->sanityCheck());
    KU_ASSERT(getDataColumnChunk()->sanityCheck());
    return sizeColumnChunk->getNumValues() == numValues;
}

} // namespace storage
} // namespace kuzu

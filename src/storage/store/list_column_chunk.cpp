#include "storage/store/list_column_chunk.h"

#include "common/cast.h"
#include "common/data_chunk/sel_vector.h"
#include "common/types/value/value.h"
#include "storage/store/column_chunk.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void ListDataColumnChunk::reset() const {
    dataColumnChunk->resetToEmpty();
}

void ListDataColumnChunk::resizeBuffer(uint64_t numValues) {
    if (numValues <= capacity) {
        return;
    }
    capacity = capacity == 0 ? 1 : capacity;
    while (capacity < numValues) {
        capacity = std::ceil(capacity * CHUNK_RESIZE_RATIO);
    }
    dataColumnChunk->resize(capacity);
}

ListColumnChunk::ListColumnChunk(LogicalType dataType, uint64_t capacity, bool enableCompression,
    bool inMemory)
    : ColumnChunk{std::move(dataType), capacity, enableCompression, true /* hasNullChunk */} {
    sizeColumnChunk = ColumnChunkFactory::createColumnChunk(*common::LogicalType::UINT32(),
        enableCompression, capacity);
    listDataColumnChunk = std::make_unique<ListDataColumnChunk>(
        ColumnChunkFactory::createColumnChunk(*ListType::getChildType(this->dataType).copy(),
            enableCompression, 0 /* capacity */, inMemory));
    checkOffsetSortedAsc = false;
    KU_ASSERT(this->dataType.getPhysicalType() == PhysicalTypeID::LIST ||
              this->dataType.getPhysicalType() == PhysicalTypeID::ARRAY);
}

bool ListColumnChunk::isOffsetsConsecutiveAndSortedAscending(uint64_t startPos,
    uint64_t endPos) const {
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

offset_t ListColumnChunk::getListStartOffset(offset_t offset) const {
    if (numValues == 0)
        return 0;
    return offset == numValues ? getListEndOffset(offset - 1) :
                                 getListEndOffset(offset) - getListSize(offset);
}

offset_t ListColumnChunk::getListEndOffset(offset_t offset) const {
    if (numValues == 0)
        return 0;
    KU_ASSERT(offset < numValues);
    return getValue<uint64_t>(offset);
}

list_size_t ListColumnChunk::getListSize(common::offset_t offset) const {
    if (numValues == 0)
        return 0;
    KU_ASSERT(offset < sizeColumnChunk->getNumValues());
    return sizeColumnChunk->getValue<list_size_t>(offset);
}

void ListColumnChunk::append(ColumnChunk* other, offset_t startPosInOtherChunk,
    uint32_t numValuesToAppend) {
    checkOffsetSortedAsc = true;
    auto otherListChunk = ku_dynamic_cast<ColumnChunk*, ListColumnChunk*>(other);
    nullChunk->append(other->getNullChunk(), startPosInOtherChunk, numValuesToAppend);
    sizeColumnChunk->getNullChunk()->append(other->getNullChunk(), startPosInOtherChunk,
        numValuesToAppend);
    offset_t offsetInDataChunkToAppend = listDataColumnChunk->getNumValues();
    for (auto i = 0u; i < numValuesToAppend; i++) {
        auto appendSize = otherListChunk->getListSize(startPosInOtherChunk + i);
        sizeColumnChunk->setValue<list_size_t>(appendSize, numValues);
        offsetInDataChunkToAppend += appendSize;
        setValue<offset_t>(offsetInDataChunkToAppend, numValues);
    }
    listDataColumnChunk->resizeBuffer(offsetInDataChunkToAppend);
    for (auto i = 0u; i < numValuesToAppend; i++) {
        auto startOffset = otherListChunk->getListStartOffset(startPosInOtherChunk + i);
        auto appendSize = otherListChunk->getListSize(startPosInOtherChunk + i);
        listDataColumnChunk->dataColumnChunk->append(
            otherListChunk->listDataColumnChunk->dataColumnChunk.get(), startOffset, appendSize);
    }
    sanityCheck();
}

void ListColumnChunk::resetToEmpty() {
    ColumnChunk::resetToEmpty();
    sizeColumnChunk->resetToEmpty();
    listDataColumnChunk = std::make_unique<ListDataColumnChunk>(
        ColumnChunkFactory::createColumnChunk(*ListType::getChildType(this->dataType).copy(),
            enableCompression, 0 /* capacity */));
}

void ListColumnChunk::append(ValueVector* vector, const SelectionVector& selVector) {
    auto numToAppend = selVector.getSelSize();
    auto newCapacity = capacity;
    while (numValues + numToAppend >= newCapacity) {
        newCapacity = std::ceil(newCapacity * 1.5);
    }
    if (capacity < newCapacity) {
        resize(newCapacity);
    }
    offset_t nextListOffsetInChunk = listDataColumnChunk->getNumValues();
    auto offsetBufferToWrite = (offset_t*)(buffer.get());
    for (auto i = 0u; i < selVector.getSelSize(); i++) {
        auto pos = selVector[i];
        auto listLen = vector->isNull(pos) ? 0 : vector->getValue<list_entry_t>(pos).size;
        sizeColumnChunk->setValue<list_size_t>(listLen, numValues + i);
        sizeColumnChunk->getNullChunk()->setNull(numValues + i, vector->isNull(pos));
        nullChunk->setNull(numValues + i, vector->isNull(pos));
        nextListOffsetInChunk += listLen;
        offsetBufferToWrite[numValues + i] = nextListOffsetInChunk;
    }
    listDataColumnChunk->resizeBuffer(nextListOffsetInChunk);
    auto dataVector = ListVector::getDataVector(vector);
    // TODO(Guodong): we should not set vector to a new state.
    dataVector->setState(std::make_unique<DataChunkState>());
    dataVector->state->getSelVectorUnsafe().setToFiltered();
    for (auto i = 0u; i < selVector.getSelSize(); i++) {
        auto pos = selVector[i];
        if (vector->isNull(pos)) {
            continue;
        }
        copyListValues(vector->getValue<list_entry_t>(pos), dataVector);
    }
    numValues += numToAppend;
    sanityCheck();
}

void ListColumnChunk::appendNullList() {
    offset_t nextListOffsetInChunk = listDataColumnChunk->getNumValues();
    auto offsetBufferToWrite = (offset_t*)(buffer.get());
    sizeColumnChunk->setValue<list_size_t>(0, numValues);
    sizeColumnChunk->getNullChunk()->setNull(numValues, true);
    offsetBufferToWrite[numValues] = nextListOffsetInChunk;
    nullChunk->setNull(numValues, true);
    numValues++;
}

void ListColumnChunk::lookup(offset_t offsetInChunk, ValueVector& output,
    sel_t posInOutputVector) const {
    KU_ASSERT(offsetInChunk < numValues);
    output.setNull(posInOutputVector, nullChunk->isNull(offsetInChunk));
    if (output.isNull(posInOutputVector)) {
        return;
    }
    auto startOffset = getListStartOffset(offsetInChunk);
    auto listSize = getListSize(offsetInChunk);
    output.setValue<list_entry_t>(posInOutputVector, list_entry_t{startOffset, listSize});
    auto dataVector = ListVector::getDataVector(&output);
    auto currentListDataSize = ListVector::getDataVectorSize(&output);
    ListVector::resizeDataVector(&output, currentListDataSize + listSize);
    // TODO(Guodong): Should add `scan` interface and use `scan` here.
    for (auto i = 0u; i < listSize; i++) {
        listDataColumnChunk->dataColumnChunk->lookup(startOffset + i, *dataVector,
            currentListDataSize + i);
    }
    // reset offset
    output.setValue<list_entry_t>(posInOutputVector, list_entry_t{currentListDataSize, listSize});
}

void ListColumnChunk::write(ColumnChunk* chunk, ColumnChunk* dstOffsets,
    RelMultiplicity /*multiplicity*/) {
    KU_ASSERT(chunk->getDataType().getPhysicalType() == dataType.getPhysicalType() &&
              dstOffsets->getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID &&
              chunk->getNumValues() == dstOffsets->getNumValues());
    checkOffsetSortedAsc = true;
    offset_t currentIndex = listDataColumnChunk->getNumValues();
    auto otherListChunk = ku_dynamic_cast<ColumnChunk*, ListColumnChunk*>(chunk);
    listDataColumnChunk->resizeBuffer(
        listDataColumnChunk->getNumValues() + otherListChunk->listDataColumnChunk->getNumValues());
    listDataColumnChunk->dataColumnChunk->append(
        otherListChunk->listDataColumnChunk->dataColumnChunk.get(), 0,
        otherListChunk->listDataColumnChunk->getNumValues());
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

void ListColumnChunk::write(ValueVector* vector, offset_t offsetInVector, offset_t offsetInChunk) {
    checkOffsetSortedAsc = true;
    auto selVector = std::make_unique<SelectionVector>(1);
    selVector->setToFiltered();
    selVector->operator[](0) = offsetInVector;
    auto appendSize =
        vector->isNull(offsetInVector) ? 0 : vector->getValue<list_entry_t>(offsetInVector).size;
    listDataColumnChunk->resizeBuffer(listDataColumnChunk->getNumValues() + appendSize);
    // TODO(Guodong): Do not set vector to a new state.
    auto dataVector = ListVector::getDataVector(vector);
    dataVector->setState(std::make_unique<DataChunkState>());
    dataVector->state->getSelVectorUnsafe().setToFiltered();
    copyListValues(vector->getValue<list_entry_t>(offsetInVector), dataVector);
    while (offsetInChunk >= numValues) {
        appendNullList();
    }
    auto isNull = vector->isNull(offsetInVector);
    nullChunk->setNull(offsetInChunk, isNull);
    sizeColumnChunk->getNullChunk()->setNull(offsetInChunk, isNull);
    if (!isNull) {
        sizeColumnChunk->setValue<list_size_t>(appendSize, offsetInChunk);
        setValue<offset_t>(listDataColumnChunk->getNumValues(), offsetInChunk);
    }
    sanityCheck();
}

void ListColumnChunk::write(ColumnChunk* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->getDataType().getPhysicalType() == PhysicalTypeID::LIST ||
              srcChunk->getDataType().getPhysicalType() == PhysicalTypeID::ARRAY);
    checkOffsetSortedAsc = true;
    auto srcListChunk = ku_dynamic_cast<ColumnChunk*, ListColumnChunk*>(srcChunk);
    auto offsetInDataChunkToAppend = listDataColumnChunk->getNumValues();
    for (auto i = 0u; i < numValuesToCopy; i++) {
        auto appendSize = srcListChunk->getListSize(srcOffsetInChunk + i);
        offsetInDataChunkToAppend += appendSize;
        sizeColumnChunk->setValue<list_size_t>(appendSize, dstOffsetInChunk + i);
        setValue<offset_t>(offsetInDataChunkToAppend, dstOffsetInChunk + i);
        nullChunk->setNull(dstOffsetInChunk + i,
            srcListChunk->nullChunk->isNull(srcOffsetInChunk + i));
        sizeColumnChunk->getNullChunk()->setNull(dstOffsetInChunk + i,
            srcListChunk->nullChunk->isNull(srcOffsetInChunk + i));
    }
    listDataColumnChunk->resizeBuffer(offsetInDataChunkToAppend);
    for (auto i = 0u; i < numValuesToCopy; i++) {
        auto startOffsetInSrcChunk = srcListChunk->getListStartOffset(srcOffsetInChunk + i);
        auto appendSize = srcListChunk->getListSize(srcOffsetInChunk + i);
        listDataColumnChunk->dataColumnChunk->append(
            srcListChunk->listDataColumnChunk->dataColumnChunk.get(), startOffsetInSrcChunk,
            appendSize);
    }
    sanityCheck();
}

void ListColumnChunk::copy(ColumnChunk* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->getDataType().getPhysicalType() == PhysicalTypeID::LIST ||
              srcChunk->getDataType().getPhysicalType() == PhysicalTypeID::ARRAY);
    KU_ASSERT(dstOffsetInChunk >= numValues);
    while (numValues < dstOffsetInChunk) {
        appendNullList();
    }
    append(srcChunk, srcOffsetInChunk, numValuesToCopy);
}

void ListColumnChunk::copyListValues(const list_entry_t& entry, ValueVector* dataVector) {
    auto numListValuesToCopy = entry.size;
    auto numListValuesCopied = 0u;
    while (numListValuesCopied < numListValuesToCopy) {
        auto numListValuesToCopyInBatch =
            std::min<uint64_t>(numListValuesToCopy - numListValuesCopied, DEFAULT_VECTOR_CAPACITY);
        dataVector->state->getSelVectorUnsafe().setSelSize(numListValuesToCopyInBatch);
        for (auto j = 0u; j < numListValuesToCopyInBatch; j++) {
            dataVector->state->getSelVectorUnsafe()[j] = entry.offset + numListValuesCopied + j;
        }
        listDataColumnChunk->append(dataVector, dataVector->state->getSelVector());
        numListValuesCopied += numListValuesToCopyInBatch;
    }
}

void ListColumnChunk::resetOffset() {
    offset_t nextListOffsetReset = 0;
    for (auto i = 0u; i < numValues; i++) {
        auto listSize = getListSize(i);
        nextListOffsetReset += uint64_t(listSize);
        setValue<offset_t>(nextListOffsetReset, i);
        sizeColumnChunk->setValue<list_size_t>(listSize, i);
    }
}

void ListColumnChunk::finalize() {
    // rewrite the column chunk for better scanning performance
    auto newColumnChunk = ColumnChunkFactory::createColumnChunk(std::move(*dataType.copy()),
        enableCompression, capacity);
    uint64_t totalListLen = listDataColumnChunk->getNumValues();
    uint64_t resizeThreshold = listDataColumnChunk->capacity / 2;
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
    auto newListChunk = ku_dynamic_cast<ColumnChunk*, ListColumnChunk*>(newColumnChunk.get());
    newListChunk->resize(numValues);
    newListChunk->getDataColumnChunk()->resize(totalListLen);
    auto dataColumnChunk = newListChunk->getDataColumnChunk();
    newListChunk->listDataColumnChunk->resizeBuffer(totalListLen);
    offset_t offsetInChunk = 0;
    offset_t currentIndex = 0;
    for (auto i = 0u; i < numValues; i++) {
        if (nullChunk->isNull(i)) {
            newListChunk->appendNullList();
        } else {
            auto startOffset = getListStartOffset(i);
            auto listSize = getListSize(i);
            dataColumnChunk->append(listDataColumnChunk->dataColumnChunk.get(), startOffset,
                listSize);
            offsetInChunk += listSize;
            newListChunk->getNullChunk()->setNull(currentIndex, false);
            newListChunk->sizeColumnChunk->getNullChunk()->setNull(currentIndex, false);
            newListChunk->sizeColumnChunk->setValue<list_size_t>(listSize, currentIndex);
            newListChunk->setValue<offset_t>(offsetInChunk, currentIndex);
        }
        currentIndex++;
    }
    newListChunk->sanityCheck();
    // Move offsets, null, data from newListChunk to this column chunk. And release indices.
    resetFromOtherChunk(newListChunk);
}
void ListColumnChunk::resetFromOtherChunk(ListColumnChunk* other) {
    buffer = std::move(other->buffer);
    nullChunk = std::move(other->nullChunk);
    sizeColumnChunk = std::move(other->sizeColumnChunk);
    listDataColumnChunk = std::move(other->listDataColumnChunk);
    numValues = other->numValues;
    checkOffsetSortedAsc = false;
}

bool ListColumnChunk::sanityCheck() {
    KU_ASSERT(ColumnChunk::sanityCheck());
    KU_ASSERT(sizeColumnChunk->sanityCheck());
    KU_ASSERT(getDataColumnChunk()->sanityCheck());
    return sizeColumnChunk->getNumValues() == numValues;
}

} // namespace storage
} // namespace kuzu

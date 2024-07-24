#include "storage/store/list_chunk_data.h"

#include <cmath>

#include "common/data_chunk/sel_vector.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/types/types.h"
#include "common/types/value/value.h"
#include "common/vector/value_vector.h"
#include "storage/store/column_chunk_data.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

ListChunkData::ListChunkData(LogicalType dataType, uint64_t capacity, bool enableCompression,
    ResidencyState residencyState)
    : ColumnChunkData{std::move(dataType), capacity, enableCompression, residencyState,
          true /*hasNullData*/} {
    // TODO(Guodong): offset/size should contain no nulls.
    offsetColumnChunk = ColumnChunkFactory::createColumnChunkData(LogicalType::UINT64(),
        enableCompression, capacity, ResidencyState::IN_MEMORY, false /*hasNull*/);
    sizeColumnChunk = ColumnChunkFactory::createColumnChunkData(LogicalType::UINT32(),
        enableCompression, capacity, ResidencyState::IN_MEMORY, false /*hasNull*/);
    dataColumnChunk =
        ColumnChunkFactory::createColumnChunkData(ListType::getChildType(this->dataType).copy(),
            enableCompression, 0 /* capacity */, ResidencyState::IN_MEMORY);
    checkOffsetSortedAsc = false;
    KU_ASSERT(this->dataType.getPhysicalType() == PhysicalTypeID::LIST ||
              this->dataType.getPhysicalType() == PhysicalTypeID::ARRAY);
}

ListChunkData::ListChunkData(LogicalType dataType, bool enableCompression,
    const ColumnChunkMetadata& metadata)
    : ColumnChunkData{std::move(dataType), enableCompression, metadata, true /*hasNullData*/},
      offsetColumnChunk{ColumnChunkFactory::createColumnChunkData(LogicalType::UINT64(),
          enableCompression, 0, ResidencyState::ON_DISK)},
      sizeColumnChunk{ColumnChunkFactory::createColumnChunkData(LogicalType::UINT32(),
          enableCompression, 0, ResidencyState::ON_DISK)},
      dataColumnChunk{
          ColumnChunkFactory::createColumnChunkData(ListType::getChildType(this->dataType).copy(),
              enableCompression, 0 /* capacity */, ResidencyState::ON_DISK)},
      checkOffsetSortedAsc{false} {}

bool ListChunkData::isOffsetsConsecutiveAndSortedAscending(uint64_t startPos,
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

offset_t ListChunkData::getListStartOffset(offset_t offset) const {
    if (numValues == 0 || (offset != numValues && nullData->isNull(offset)))
        return 0;
    KU_ASSERT(offset == numValues || getListEndOffset(offset) >= getListSize(offset));
    return offset == numValues ? getListEndOffset(offset - 1) :
                                 getListEndOffset(offset) - getListSize(offset);
}

offset_t ListChunkData::getListEndOffset(offset_t offset) const {
    if (numValues == 0 || nullData->isNull(offset))
        return 0;
    KU_ASSERT(offset < numValues);
    return offsetColumnChunk->getValue<uint64_t>(offset);
}

list_size_t ListChunkData::getListSize(offset_t offset) const {
    if (numValues == 0 || nullData->isNull(offset))
        return 0;
    KU_ASSERT(offset < sizeColumnChunk->getNumValues());
    return sizeColumnChunk->getValue<list_size_t>(offset);
}

void ListChunkData::setOffsetChunkValue(offset_t val, offset_t pos) {
    offsetColumnChunk->setValue(val, pos);

    // we will keep numValues in the main column synchronized
    numValues = offsetColumnChunk->getNumValues();
}

void ListChunkData::append(ColumnChunkData* other, offset_t startPosInOtherChunk,
    uint32_t numValuesToAppend) {
    checkOffsetSortedAsc = true;
    auto& otherListChunk = other->cast<ListChunkData>();
    nullData->append(other->getNullData(), startPosInOtherChunk, numValuesToAppend);
    offset_t offsetInDataChunkToAppend = dataColumnChunk->getNumValues();
    for (auto i = 0u; i < numValuesToAppend; i++) {
        auto appendSize = otherListChunk.getListSize(startPosInOtherChunk + i);
        sizeColumnChunk->setValue<list_size_t>(appendSize, numValues);
        offsetInDataChunkToAppend += appendSize;
        setOffsetChunkValue(offsetInDataChunkToAppend, numValues);
    }
    dataColumnChunk->resize(offsetInDataChunkToAppend);
    for (auto i = 0u; i < numValuesToAppend; i++) {
        auto startOffset = otherListChunk.getListStartOffset(startPosInOtherChunk + i);
        auto appendSize = otherListChunk.getListSize(startPosInOtherChunk + i);
        dataColumnChunk->append(otherListChunk.dataColumnChunk.get(), startOffset, appendSize);
    }
    KU_ASSERT(sanityCheck());
}

void ListChunkData::resetToEmpty() {
    ColumnChunkData::resetToEmpty();
    sizeColumnChunk->resetToEmpty();
    offsetColumnChunk->resetToEmpty();
    dataColumnChunk =
        ColumnChunkFactory::createColumnChunkData(ListType::getChildType(this->dataType).copy(),
            enableCompression, 0 /* capacity */, ResidencyState::IN_MEMORY);
}

void ListChunkData::resetNumValuesFromMetadata() {
    ColumnChunkData::resetNumValuesFromMetadata();
    sizeColumnChunk->resetNumValuesFromMetadata();
    offsetColumnChunk->resetNumValuesFromMetadata();
    dataColumnChunk->resetNumValuesFromMetadata();
}

void ListChunkData::append(ValueVector* vector, const SelectionVector& selVector) {
    auto numToAppend = selVector.getSelSize();
    auto newCapacity = capacity;
    while (numValues + numToAppend >= newCapacity) {
        newCapacity = std::ceil(newCapacity * 1.5);
    }
    if (capacity < newCapacity) {
        resize(newCapacity);
    }
    offset_t nextListOffsetInChunk = dataColumnChunk->getNumValues();
    const offset_t appendBaseOffset = numValues;
    for (auto i = 0u; i < selVector.getSelSize(); i++) {
        auto pos = selVector[i];
        auto listLen = vector->isNull(pos) ? 0 : vector->getValue<list_entry_t>(pos).size;
        sizeColumnChunk->setValue<list_size_t>(listLen, appendBaseOffset + i);

        nullData->setNull(appendBaseOffset + i, vector->isNull(pos));

        nextListOffsetInChunk += listLen;
        setOffsetChunkValue(nextListOffsetInChunk, appendBaseOffset + i);
    }
    dataColumnChunk->resize(nextListOffsetInChunk);
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
    KU_ASSERT(sanityCheck());
}

void ListChunkData::appendNullList() {
    offset_t nextListOffsetInChunk = dataColumnChunk->getNumValues();
    const offset_t appendPosition = numValues;
    sizeColumnChunk->setValue<list_size_t>(0, appendPosition);
    setOffsetChunkValue(nextListOffsetInChunk, appendPosition);
    nullData->setNull(appendPosition, true);
}

void ListChunkData::scan(ValueVector& output, offset_t offset, length_t length,
    sel_t posInOutputVector) const {
    KU_ASSERT(offset + length <= numValues);
    if (nullData) {
        nullData->scan(output, offset, length, posInOutputVector);
    }
    auto currentListDataSize = ListVector::getDataVectorSize(&output);
    auto dataSize = 0ul;
    for (auto i = 0u; i < length; i++) {
        auto listSize = getListSize(offset + i);
        output.setValue<list_entry_t>(posInOutputVector + i,
            list_entry_t{currentListDataSize + dataSize, listSize});
        dataSize += listSize;
    }
    ListVector::resizeDataVector(&output, currentListDataSize + dataSize);
    auto dataVector = ListVector::getDataVector(&output);
    if (isOffsetsConsecutiveAndSortedAscending(offset, offset + length)) {
        dataColumnChunk->scan(*dataVector, getListStartOffset(offset), dataSize,
            currentListDataSize);
    } else {
        for (auto i = 0u; i < length; i++) {
            auto startOffset = getListStartOffset(offset + i);
            auto listSize = getListSize(offset + i);
            dataColumnChunk->scan(*dataVector, startOffset, listSize, currentListDataSize);
            currentListDataSize += listSize;
        }
    }
}

void ListChunkData::lookup(offset_t offsetInChunk, ValueVector& output,
    sel_t posInOutputVector) const {
    KU_ASSERT(offsetInChunk < numValues);
    output.setNull(posInOutputVector, nullData->isNull(offsetInChunk));
    if (output.isNull(posInOutputVector)) {
        return;
    }
    auto startOffset = getListStartOffset(offsetInChunk);
    auto listSize = getListSize(offsetInChunk);
    output.setValue<list_entry_t>(posInOutputVector, list_entry_t{startOffset, listSize});
    auto dataVector = ListVector::getDataVector(&output);
    auto currentListDataSize = ListVector::getDataVectorSize(&output);
    ListVector::resizeDataVector(&output, currentListDataSize + listSize);
    dataColumnChunk->scan(*dataVector, startOffset, listSize, currentListDataSize);
    // reset offset
    output.setValue<list_entry_t>(posInOutputVector, list_entry_t{currentListDataSize, listSize});
}

void ListChunkData::initializeScanState(ChunkState& state) const {
    ColumnChunkData::initializeScanState(state);
    state.childrenStates.resize(CHILD_COLUMN_COUNT);
    sizeColumnChunk->initializeScanState(state.childrenStates[SIZE_COLUMN_CHILD_READ_STATE_IDX]);
    dataColumnChunk->initializeScanState(state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX]);
    offsetColumnChunk->initializeScanState(
        state.childrenStates[OFFSET_COLUMN_CHILD_READ_STATE_IDX]);
}

void ListChunkData::write(ColumnChunkData* chunk, ColumnChunkData* dstOffsets, RelMultiplicity) {
    KU_ASSERT(chunk->getDataType().getPhysicalType() == dataType.getPhysicalType() &&
              dstOffsets->getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID &&
              chunk->getNumValues() == dstOffsets->getNumValues());
    checkOffsetSortedAsc = true;
    offset_t currentIndex = dataColumnChunk->getNumValues();
    auto& otherListChunk = chunk->cast<ListChunkData>();
    dataColumnChunk->resize(
        dataColumnChunk->getNumValues() + otherListChunk.dataColumnChunk->getNumValues());
    dataColumnChunk->append(otherListChunk.dataColumnChunk.get(), 0,
        otherListChunk.dataColumnChunk->getNumValues());
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
        auto appendSize = otherListChunk.getListSize(i);
        currentIndex += appendSize;
        nullData->setNull(posInChunk, otherListChunk.nullData->isNull(i));
        setOffsetChunkValue(currentIndex, posInChunk);
        sizeColumnChunk->setValue<list_size_t>(appendSize, posInChunk);
    }
    KU_ASSERT(sanityCheck());
}

void ListChunkData::write(const ValueVector* vector, offset_t offsetInVector,
    offset_t offsetInChunk) {
    checkOffsetSortedAsc = true;
    auto selVector = std::make_unique<SelectionVector>(1);
    selVector->setToFiltered();
    selVector->operator[](0) = offsetInVector;
    auto appendSize =
        vector->isNull(offsetInVector) ? 0 : vector->getValue<list_entry_t>(offsetInVector).size;
    dataColumnChunk->resize(dataColumnChunk->getNumValues() + appendSize);
    // TODO(Guodong): Do not set vector to a new state.
    auto dataVector = ListVector::getDataVector(vector);
    dataVector->setState(std::make_unique<DataChunkState>());
    dataVector->state->getSelVectorUnsafe().setToFiltered();
    copyListValues(vector->getValue<list_entry_t>(offsetInVector), dataVector);
    while (offsetInChunk >= numValues) {
        appendNullList();
    }
    auto isNull = vector->isNull(offsetInVector);
    nullData->setNull(offsetInChunk, isNull);
    if (!isNull) {
        sizeColumnChunk->setValue<list_size_t>(appendSize, offsetInChunk);
        setOffsetChunkValue(dataColumnChunk->getNumValues(), offsetInChunk);
    }
    KU_ASSERT(sanityCheck());
}

void ListChunkData::write(ColumnChunkData* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->getDataType().getPhysicalType() == PhysicalTypeID::LIST ||
              srcChunk->getDataType().getPhysicalType() == PhysicalTypeID::ARRAY);
    checkOffsetSortedAsc = true;
    auto& srcListChunk = srcChunk->cast<ListChunkData>();
    auto offsetInDataChunkToAppend = dataColumnChunk->getNumValues();
    for (auto i = 0u; i < numValuesToCopy; i++) {
        auto appendSize = srcListChunk.getListSize(srcOffsetInChunk + i);
        offsetInDataChunkToAppend += appendSize;
        sizeColumnChunk->setValue<list_size_t>(appendSize, dstOffsetInChunk + i);
        setOffsetChunkValue(offsetInDataChunkToAppend, dstOffsetInChunk + i);
        nullData->setNull(dstOffsetInChunk + i,
            srcListChunk.nullData->isNull(srcOffsetInChunk + i));
    }
    dataColumnChunk->resize(offsetInDataChunkToAppend);
    for (auto i = 0u; i < numValuesToCopy; i++) {
        auto startOffsetInSrcChunk = srcListChunk.getListStartOffset(srcOffsetInChunk + i);
        auto appendSize = srcListChunk.getListSize(srcOffsetInChunk + i);
        dataColumnChunk->append(srcListChunk.dataColumnChunk.get(), startOffsetInSrcChunk,
            appendSize);
    }
    KU_ASSERT(sanityCheck());
}

void ListChunkData::copy(ColumnChunkData* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->getDataType().getPhysicalType() == PhysicalTypeID::LIST ||
              srcChunk->getDataType().getPhysicalType() == PhysicalTypeID::ARRAY);
    KU_ASSERT(dstOffsetInChunk >= numValues);
    while (numValues < dstOffsetInChunk) {
        appendNullList();
    }
    append(srcChunk, srcOffsetInChunk, numValuesToCopy);
}

void ListChunkData::copyListValues(const list_entry_t& entry, ValueVector* dataVector) {
    auto numListValuesToCopy = entry.size;
    auto numListValuesCopied = 0u;
    while (numListValuesCopied < numListValuesToCopy) {
        auto numListValuesToCopyInBatch =
            std::min<uint64_t>(numListValuesToCopy - numListValuesCopied, DEFAULT_VECTOR_CAPACITY);
        dataVector->state->getSelVectorUnsafe().setSelSize(numListValuesToCopyInBatch);
        for (auto j = 0u; j < numListValuesToCopyInBatch; j++) {
            dataVector->state->getSelVectorUnsafe()[j] = entry.offset + numListValuesCopied + j;
        }
        dataColumnChunk->append(dataVector, dataVector->state->getSelVector());
        numListValuesCopied += numListValuesToCopyInBatch;
    }
}

void ListChunkData::resetOffset() {
    offset_t nextListOffsetReset = 0;
    for (auto i = 0u; i < numValues; i++) {
        auto listSize = getListSize(i);
        nextListOffsetReset += uint64_t(listSize);
        setOffsetChunkValue(nextListOffsetReset, i);
        sizeColumnChunk->setValue<list_size_t>(listSize, i);
    }
}

void ListChunkData::finalize() {
    // rewrite the column chunk for better scanning performance
    auto newColumnChunk = ColumnChunkFactory::createColumnChunkData(dataType.copy(),
        enableCompression, capacity, ResidencyState::IN_MEMORY);
    uint64_t totalListLen = dataColumnChunk->getNumValues();
    uint64_t resizeThreshold = dataColumnChunk->getCapacity() / 2;
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
    auto& newListChunk = newColumnChunk->cast<ListChunkData>();
    newListChunk.resize(numValues);
    newListChunk.getDataColumnChunk()->resize(totalListLen);
    auto newDataColumnChunk = newListChunk.getDataColumnChunk();
    newDataColumnChunk->resize(totalListLen);
    offset_t offsetInChunk = 0;
    offset_t currentIndex = 0;
    for (auto i = 0u; i < numValues; i++) {
        if (nullData->isNull(i)) {
            newListChunk.appendNullList();
        } else {
            auto startOffset = getListStartOffset(i);
            auto listSize = getListSize(i);
            newDataColumnChunk->append(dataColumnChunk.get(), startOffset, listSize);
            offsetInChunk += listSize;
            newListChunk.nullData->setNull(currentIndex, false);
            newListChunk.sizeColumnChunk->setValue<list_size_t>(listSize, currentIndex);
            newListChunk.setOffsetChunkValue(offsetInChunk, currentIndex);
        }
        currentIndex++;
    }
    KU_ASSERT(newListChunk.sanityCheck());
    // Move offsets, null, data from newListChunk to this column chunk. And release indices.
    resetFromOtherChunk(&newListChunk);
}

void ListChunkData::resetFromOtherChunk(ListChunkData* other) {
    nullData = std::move(other->nullData);
    sizeColumnChunk = std::move(other->sizeColumnChunk);
    dataColumnChunk = std::move(other->dataColumnChunk);
    offsetColumnChunk = std::move(other->offsetColumnChunk);
    numValues = other->numValues;
    checkOffsetSortedAsc = false;
}

bool ListChunkData::sanityCheck() const {
    KU_ASSERT(ColumnChunkData::sanityCheck());
    KU_ASSERT(sizeColumnChunk->sanityCheck());
    KU_ASSERT(offsetColumnChunk->sanityCheck());
    KU_ASSERT(getDataColumnChunk()->sanityCheck());
    return sizeColumnChunk->getNumValues() == numValues;
}

uint64_t ListChunkData::getEstimatedMemoryUsage() const {
    return ColumnChunkData::getEstimatedMemoryUsage() + sizeColumnChunk->getEstimatedMemoryUsage() +
           dataColumnChunk->getEstimatedMemoryUsage() +
           offsetColumnChunk->getEstimatedMemoryUsage();
}

void ListChunkData::serialize(Serializer& serializer) const {
    ColumnChunkData::serialize(serializer);
    serializer.writeDebuggingInfo("size_column_chunk");
    sizeColumnChunk->serialize(serializer);
    serializer.writeDebuggingInfo("data_column_chunk");
    dataColumnChunk->serialize(serializer);
    serializer.writeDebuggingInfo("offset_column_chunk");
    offsetColumnChunk->serialize(serializer);
}

void ListChunkData::deserialize(Deserializer& deSer, ColumnChunkData& chunkData) {
    std::string key;
    deSer.validateDebuggingInfo(key, "size_column_chunk");
    chunkData.cast<ListChunkData>().sizeColumnChunk = ColumnChunkData::deserialize(deSer);
    deSer.validateDebuggingInfo(key, "data_column_chunk");
    chunkData.cast<ListChunkData>().dataColumnChunk = ColumnChunkData::deserialize(deSer);
    deSer.validateDebuggingInfo(key, "offset_column_chunk");
    chunkData.cast<ListChunkData>().offsetColumnChunk = ColumnChunkData::deserialize(deSer);
}

void ListChunkData::flush(BMFileHandle& dataFH) {
    ColumnChunkData::flush(dataFH);
    sizeColumnChunk->flush(dataFH);
    dataColumnChunk->flush(dataFH);
    offsetColumnChunk->flush(dataFH);
}

} // namespace storage
} // namespace kuzu

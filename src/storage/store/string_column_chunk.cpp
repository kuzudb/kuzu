#include "storage/store/string_column_chunk.h"

#include <bit>

using namespace kuzu::common;

namespace kuzu {
namespace storage {

// The offset chunk is able to grow beyond the node group size.
// We rely on appending to the dictionary when updating, however if the chunk is full,
// there will be no space for in-place updates.
// The data chunk doubles in size on use, but out of place updates will never need the offset
// chunk to be greater than the node group size since they remove unused entries.
// So the chunk is initialized with a size equal to 3/4 the node group size, making sure there
// is always extra space for updates.
static const uint64_t OFFSET_CHUNK_INITIAL_CAPACITY = StorageConstants::NODE_GROUP_SIZE * 0.75;

StringColumnChunk::StringColumnChunk(
    std::unique_ptr<LogicalType> dataType, uint64_t capacity, bool enableCompression)
    : ColumnChunk{std::move(dataType), capacity, enableCompression},
      enableCompression(enableCompression), needFinalize{false} {
    // Bitpacking might save 1 bit per value with regular ascii compared to UTF-8
    stringDataChunk = ColumnChunkFactory::createColumnChunk(LogicalType::UINT8(), false);
    offsetChunk = ColumnChunkFactory::createColumnChunk(
        LogicalType::UINT64(), enableCompression, OFFSET_CHUNK_INITIAL_CAPACITY);
}

void StringColumnChunk::resetToEmpty() {
    ColumnChunk::resetToEmpty();
    stringDataChunk->resetToEmpty();
    offsetChunk->resetToEmpty();
    indexTable.clear();
}

void StringColumnChunk::append(ValueVector* vector) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        // index is stored in main chunk, data is stored in the data chunk
        auto pos = vector->state->selVector->selectedPositions[i];
        nullChunk->setNull(numValues, vector->isNull(pos));
        if (vector->isNull(pos)) {
            numValues++;
            continue;
        }
        auto kuString = vector->getValue<ku_string_t>(pos);
        setValueFromString(kuString.getAsString().c_str(), kuString.len, numValues);
    }
}

void StringColumnChunk::append(
    ColumnChunk* other, offset_t startPosInOtherChunk, uint32_t numValuesToAppend) {
    auto otherChunk = reinterpret_cast<StringColumnChunk*>(other);
    nullChunk->append(otherChunk->getNullChunk(), startPosInOtherChunk, numValuesToAppend);
    switch (dataType->getLogicalTypeID()) {
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING: {
        appendStringColumnChunk(otherChunk, startPosInOtherChunk, numValuesToAppend);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

void StringColumnChunk::write(
    ValueVector* vector, offset_t offsetInVector, offset_t offsetInChunk) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    if (!needFinalize && offsetInChunk < numValues) [[unlikely]] {
        needFinalize = true;
    }
    nullChunk->setNull(offsetInChunk, vector->isNull(offsetInVector));
    if (!vector->isNull(offsetInVector)) {
        auto kuStr = vector->getValue<ku_string_t>(offsetInVector);
        setValueFromString(kuStr.getAsString().c_str(), kuStr.len, offsetInChunk);
    }
}

void StringColumnChunk::write(
    ValueVector* valueVector, ValueVector* offsetInChunkVector, bool /*isCSR*/) {
    KU_ASSERT(valueVector->dataType.getPhysicalType() == PhysicalTypeID::STRING &&
              offsetInChunkVector->dataType.getPhysicalType() == PhysicalTypeID::INT64 &&
              valueVector->state->selVector->selectedSize ==
                  offsetInChunkVector->state->selVector->selectedSize);
    auto offsets = (offset_t*)offsetInChunkVector->getData();
    for (auto i = 0u; i < valueVector->state->selVector->selectedSize; i++) {
        auto offsetInChunk = offsets[offsetInChunkVector->state->selVector->selectedPositions[i]];
        if (!needFinalize && offsetInChunk < numValues) [[unlikely]] {
            needFinalize = true;
        }
        auto offsetInVector = valueVector->state->selVector->selectedPositions[i];
        nullChunk->setNull(offsetInChunk, valueVector->isNull(offsetInVector));
        if (offsetInChunk >= numValues) {
            numValues = offsetInChunk + 1;
        }
        if (!valueVector->isNull(offsetInVector)) {
            auto kuStr = valueVector->getValue<ku_string_t>(offsetInVector);
            setValueFromString((const char*)kuStr.getData(), kuStr.len, offsetInChunk);
        }
    }
}

void StringColumnChunk::appendStringColumnChunk(
    StringColumnChunk* other, offset_t startPosInOtherChunk, uint32_t numValuesToAppend) {
    auto indices = reinterpret_cast<string_index_t*>(buffer.get());
    for (auto i = 0u; i < numValuesToAppend; i++) {
        auto posInChunk = numValues;
        auto posInOtherChunk = i + startPosInOtherChunk;
        if (nullChunk->isNull(posInChunk)) {
            indices[posInChunk] = 0;
            numValues++;
            continue;
        }
        auto stringInOtherChunk = other->getValue<std::string_view>(posInOtherChunk);
        setValueFromString(stringInOtherChunk.data(), stringInOtherChunk.size(), posInChunk);
    }
}

void StringColumnChunk::setValueFromString(const char* value, uint64_t length, uint64_t pos) {
    if (pos >= numValues) {
        numValues = pos + 1;
    }
    auto result = indexTable.find(std::string_view(value, length));
    // If the string already exists in the dictionary, skip it and refer to the existing string
    if (enableCompression && result != indexTable.end()) {
        ColumnChunk::setValue<string_index_t>(result->second, pos);
        return;
    }
    auto space = stringDataChunk->getCapacity() - stringDataChunk->getNumValues();
    if (length > space) {
        stringDataChunk->resize(std::bit_ceil(stringDataChunk->getCapacity() + length));
    }
    auto startOffset = stringDataChunk->getNumValues();
    memcpy(stringDataChunk->getData() + startOffset, value, length);
    stringDataChunk->setNumValues(startOffset + length);
    auto index = offsetChunk->getNumValues();

    if (index >= offsetChunk->getCapacity()) {
        offsetChunk->resize(offsetChunk->getCapacity() * 2);
    }
    offsetChunk->setValue<string_offset_t>(startOffset, index);
    offsetChunk->setNumValues(index + 1);
    ColumnChunk::setValue<string_index_t>(index, pos);
    indexTable.insert(std::make_pair(std::string(value, length), index));
}

void StringColumnChunk::finalize() {
    if (!needFinalize) {
        return;
    }
    // Only de-duplicate strings, not blobs
    if (dataType->getLogicalTypeID() != LogicalTypeID::STRING) {
        return;
    }
    // Prune unused entries in the dictionary before we flush
    // We already de-duplicate as we go, but when out of place updates occur new values will be
    // appended to the end and the original values may be able to be pruned before flushing them to
    // disk
    std::unordered_map<std::string_view, string_index_t> indexTable;
    // Create new buffers with only the unique strings in the index map, updating them as values
    // are added to the indexMap
    auto newDataChunk = ColumnChunkFactory::createColumnChunk(LogicalType::UINT8(), false);

    auto newOffsetChunk = ColumnChunkFactory::createColumnChunk(
        LogicalType::UINT64(), enableCompression, OFFSET_CHUNK_INITIAL_CAPACITY);

    // We re-write the source buffer as we go over it.
    // Each index is replaced by a new one for the de-duplicated data in the new data buffer
    auto outIndices = (string_index_t*)buffer.get();
    for (int i = 0; i < numValues; i++) {
        if (nullChunk->isNull(i)) {
            continue;
        }
        auto stringData = getValue<std::string_view>(i);
        auto index = ColumnChunk::getValue<string_index_t>(i);
        auto offset = offsetChunk->getValue<string_offset_t>(index);

        auto result = indexTable.find(stringData);
        if (enableCompression && result != indexTable.end()) {
            outIndices[i] = result->second;
        } else {
            if (enableCompression) {
                indexTable.insert(std::make_pair(stringData, indexTable.size()));
            }

            auto newIndex = newOffsetChunk->getNumValues();
            if (newIndex >= newOffsetChunk->getCapacity()) {
                newOffsetChunk->resize(newOffsetChunk->getCapacity() * CHUNK_RESIZE_RATIO);
            }
            newOffsetChunk->setValue<string_offset_t>(newDataChunk->getNumValues(), newIndex);
            outIndices[i] = newIndex;
            newOffsetChunk->setNumValues(newIndex + 1);

            auto space = newDataChunk->getCapacity() - newDataChunk->getNumValues();
            if (stringData.size() > space) {
                newDataChunk->resize(
                    std::bit_ceil(newDataChunk->getCapacity() + stringData.size()));
            }
            newDataChunk->append(stringDataChunk.get(), offset, stringData.size());
        }
    }
    stringDataChunk = std::move(newDataChunk);
    offsetChunk = std::move(newOffsetChunk);
}

// STRING
template<>
std::string_view StringColumnChunk::getValue<std::string_view>(offset_t pos) const {
    KU_ASSERT(pos < numValues);
    auto index = ColumnChunk::getValue<string_index_t>(pos);
    KU_ASSERT(index < offsetChunk->getNumValues());
    auto offset = offsetChunk->getValue<string_offset_t>(index);
    return std::string_view((const char*)stringDataChunk->getData() + offset, getStringLength(pos));
}
template<>
std::string StringColumnChunk::getValue<std::string>(offset_t pos) const {
    return std::string(getValue<std::string_view>(pos));
}

} // namespace storage
} // namespace kuzu

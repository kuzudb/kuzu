#include "storage/store/string_column_chunk.h"

#include <bit>

using namespace kuzu::common;

namespace kuzu {
namespace storage {

StringColumnChunk::StringColumnChunk(std::unique_ptr<LogicalType> dataType, uint64_t capacity)
    : ColumnChunk{std::move(dataType), capacity, true /*enableCompression*/} {
    bool enableCompression = true;
    // Bitpacking might save 1 bit per value with regular ascii compared to UTF-8
    stringDataChunk = ColumnChunkFactory::createColumnChunk(LogicalType::UINT8(), false);
    // The offset chunk is able to grow beyond the node group size.
    // We rely on appending to the dictionary when updating, however if the chunk is full,
    // there will be no space for in-place updates.
    // The data chunk doubles in size on use, but out of place updates will never need the offset
    // chunk to be greater than the node group size since they remove unused entries.
    // So the chunk is initialized with a size equal to 3/4 the node group size, making sure there
    // is always extra space for updates.
    offsetChunk = ColumnChunkFactory::createColumnChunk(
        LogicalType::UINT64(), enableCompression, StorageConstants::NODE_GROUP_SIZE * 0.75);
}

void StringColumnChunk::resetToEmpty() {
    ColumnChunk::resetToEmpty();
    stringDataChunk->resetToEmpty();
    offsetChunk->resetToEmpty();
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
    nullChunk->setNull(offsetInChunk, vector->isNull(offsetInVector));
    if (!vector->isNull(offsetInVector)) {
        auto kuStr = vector->getValue<ku_string_t>(offsetInVector);
        setValueFromString(kuStr.getAsString().c_str(), kuStr.len, offsetInChunk);
    }
}

void StringColumnChunk::write(ValueVector* valueVector, ValueVector* offsetInChunkVector) {
    KU_ASSERT(valueVector->dataType.getPhysicalType() == PhysicalTypeID::STRING &&
              offsetInChunkVector->dataType.getPhysicalType() == PhysicalTypeID::INT64 &&
              valueVector->state->selVector->selectedSize ==
                  offsetInChunkVector->state->selVector->selectedSize);
    auto offsets = (offset_t*)offsetInChunkVector->getData();
    for (auto i = 0u; i < valueVector->state->selVector->selectedSize; i++) {
        auto offsetInChunk = offsets[offsetInChunkVector->state->selVector->selectedPositions[i]];
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
    auto space = stringDataChunk->getCapacity() - stringDataChunk->getNumValues();
    if (length > space) {
        stringDataChunk->resize(std::bit_ceil(stringDataChunk->getCapacity() + length));
    }
    if (pos >= numValues) {
        numValues = pos + 1;
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
}

// STRING
template<>
std::string_view StringColumnChunk::getValue<std::string_view>(offset_t pos) const {
    auto index = ColumnChunk::getValue<string_index_t>(pos);
    auto offset = offsetChunk->getValue<string_offset_t>(index);
    return std::string_view((const char*)stringDataChunk->getData() + offset, getStringLength(pos));
}
template<>
std::string StringColumnChunk::getValue<std::string>(offset_t pos) const {
    return std::string(getValue<std::string_view>(pos));
}

} // namespace storage
} // namespace kuzu

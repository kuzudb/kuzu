#include "storage/store/dictionary_chunk.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "storage/enums/residency_state.h"
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
static constexpr double OFFSET_CHUNK_CAPACITY_FACTOR = 0.75;

DictionaryChunk::DictionaryChunk(uint64_t capacity, bool enableCompression,
    ResidencyState residencyState)
    : enableCompression{enableCompression},
      indexTable(0, StringOps(this) /*hash*/, StringOps(this) /*equals*/) {
    // Bitpacking might save 1 bit per value with regular ascii compared to UTF-8
    stringDataChunk = ColumnChunkFactory::createColumnChunkData(LogicalType::UINT8(),
        false /*enableCompression*/, capacity, residencyState, false);
    offsetChunk = ColumnChunkFactory::createColumnChunkData(LogicalType::UINT64(),
        enableCompression, capacity * OFFSET_CHUNK_CAPACITY_FACTOR, residencyState, false);
}

void DictionaryChunk::resetToEmpty() {
    stringDataChunk->resetToEmpty();
    offsetChunk->resetToEmpty();
    indexTable.clear();
}

uint64_t DictionaryChunk::getStringLength(string_index_t index) const {
    if (stringDataChunk->getNumValues() == 0) {
        return 0;
    }
    if (index + 1 < offsetChunk->getNumValues()) {
        KU_ASSERT(offsetChunk->getValue<string_offset_t>(index + 1) >=
                  offsetChunk->getValue<string_offset_t>(index));
        return offsetChunk->getValue<string_offset_t>(index + 1) -
               offsetChunk->getValue<string_offset_t>(index);
    }
    return stringDataChunk->getNumValues() - offsetChunk->getValue<string_offset_t>(index);
}

DictionaryChunk::string_index_t DictionaryChunk::appendString(std::string_view val) {
    const auto found = indexTable.find(val);
    // If the string already exists in the dictionary, skip it and refer to the existing string
    if (enableCompression && found != indexTable.end()) {
        return found->index;
    }
    const auto leftSpace = stringDataChunk->getCapacity() - stringDataChunk->getNumValues();
    if (leftSpace < val.size()) {
        stringDataChunk->resize(std::bit_ceil(stringDataChunk->getCapacity() + val.size()));
    }
    const auto startOffset = stringDataChunk->getNumValues();
    memcpy(stringDataChunk->getData() + startOffset, val.data(), val.size());
    stringDataChunk->setNumValues(startOffset + val.size());
    const auto index = offsetChunk->getNumValues();
    if (index >= offsetChunk->getCapacity()) {
        offsetChunk->resize(offsetChunk->getCapacity() == 0 ?
                                2 :
                                (offsetChunk->getCapacity() * CHUNK_RESIZE_RATIO));
    }
    offsetChunk->setValue<string_offset_t>(startOffset, index);
    offsetChunk->setNumValues(index + 1);
    if (enableCompression) {
        indexTable.insert({static_cast<string_index_t>(index)});
    }
    return index;
}

std::string_view DictionaryChunk::getString(string_index_t index) const {
    KU_ASSERT(index < offsetChunk->getNumValues());
    const auto startOffset = offsetChunk->getValue<string_offset_t>(index);
    const auto length = getStringLength(index);
    return std::string_view(reinterpret_cast<const char*>(stringDataChunk->getData()) + startOffset,
        length);
}

bool DictionaryChunk::sanityCheck() const {
    return offsetChunk->getNumValues() <= offsetChunk->getNumValues();
}

void DictionaryChunk::resetNumValuesFromMetadata() {
    stringDataChunk->resetNumValuesFromMetadata();
    offsetChunk->resetNumValuesFromMetadata();
}

uint64_t DictionaryChunk::getEstimatedMemoryUsage() const {
    return stringDataChunk->getEstimatedMemoryUsage() + offsetChunk->getEstimatedMemoryUsage();
}

void DictionaryChunk::flush(BMFileHandle& dataFH) {
    stringDataChunk->flush(dataFH);
    offsetChunk->flush(dataFH);
}

void DictionaryChunk::serialize(Serializer& serializer) const {
    serializer.writeDebuggingInfo("offset_chunk");
    offsetChunk->serialize(serializer);
    serializer.writeDebuggingInfo("string_data_chunk");
    stringDataChunk->serialize(serializer);
}

std::unique_ptr<DictionaryChunk> DictionaryChunk::deserialize(Deserializer& deSer) {
    auto chunk = std::make_unique<DictionaryChunk>(0, true, ResidencyState::ON_DISK);
    std::string key;
    deSer.validateDebuggingInfo(key, "offset_chunk");
    chunk->offsetChunk = ColumnChunkData::deserialize(deSer);
    deSer.validateDebuggingInfo(key, "string_data_chunk");
    chunk->stringDataChunk = ColumnChunkData::deserialize(deSer);
    return chunk;
}

} // namespace storage
} // namespace kuzu

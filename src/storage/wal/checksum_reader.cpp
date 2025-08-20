#include "storage/wal/checksum_reader.h"

#include <cstring>

#include "common/checksum.h"
#include "common/exception/storage.h"
#include "common/serializer/deserializer.h"

namespace kuzu::storage {
static constexpr uint64_t ENTRY_BUFFER_SIZE = common::KUZU_PAGE_SIZE;

ChecksumReader::ChecksumReader(std::unique_ptr<Reader> reader, MemoryManager& memoryManager)
    : deserializer(std::make_unique<common::Deserializer>(std::move(reader))), currentEntrySize(0),
      currentChecksum(), entryBuffer(memoryManager.allocateBuffer(false, ENTRY_BUFFER_SIZE)) {}

void ChecksumReader::read(uint8_t* data, uint64_t size) {
    deserializer->read(data, size);
    if (currentChecksum.has_value()) {
        KU_ASSERT(currentEntrySize + size <= ENTRY_BUFFER_SIZE);
        std::memcpy(entryBuffer->getData() + currentEntrySize, data, size);
        currentEntrySize += size;
    }
}

bool ChecksumReader::finished() {
    return deserializer->finished();
}

void ChecksumReader::startEntry() {
    currentEntrySize = 0;
    uint64_t storedChecksum{};
    deserializer->deserializeValue(storedChecksum);
    currentChecksum = storedChecksum;
}

void ChecksumReader::finishEntry() {
    KU_ASSERT(currentChecksum.has_value());
    const uint64_t computedChecksum = common::checksum(entryBuffer->getData(), currentEntrySize);
    if (currentChecksum != computedChecksum) {
        // TODO improve error message, maybe make behaviour configurable
        throw common::StorageException("WAL checksum mismatch");
    }
}

} // namespace kuzu::storage

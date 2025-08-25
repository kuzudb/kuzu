#include "storage/wal/checksum_reader.h"

#include <cstring>

#include "common/checksum.h"
#include "common/exception/storage.h"
#include "common/serializer/buffered_file.h"
#include "common/serializer/deserializer.h"

namespace kuzu::storage {
static constexpr uint64_t ENTRY_BUFFER_SIZE = common::KUZU_PAGE_SIZE;

ChecksumReader::ChecksumReader(common::FileInfo& fileInfo, MemoryManager& memoryManager)
    : deserializer(std::make_unique<common::BufferedFileReader>(fileInfo)), currentEntrySize(0),
      entryBuffer(memoryManager.allocateBuffer(false, ENTRY_BUFFER_SIZE)) {}

void ChecksumReader::read(uint8_t* data, uint64_t size) {
    deserializer.read(data, size);
    KU_ASSERT(currentEntrySize + size <= ENTRY_BUFFER_SIZE);
    std::memcpy(entryBuffer->getData() + currentEntrySize, data, size);
    currentEntrySize += size;
}

bool ChecksumReader::finished() {
    return deserializer.finished();
}

void ChecksumReader::finishEntry(std::string_view checksumMismatchMessage) {
    const uint64_t computedChecksum = common::checksum(entryBuffer->getData(), currentEntrySize);
    uint64_t storedChecksum{};
    deserializer.deserializeValue(storedChecksum);
    if (storedChecksum != computedChecksum) {
        // TODO: Maybe make behaviour configurable e.g. truncate WAL instead of erroring
        throw common::StorageException(std::string{checksumMismatchMessage});
    }

    // reset buffer
    currentEntrySize = 0;
}

uint64_t ChecksumReader::getReadOffset() const {
    return deserializer.getReader()->cast<common::BufferedFileReader>()->getReadOffset();
}

} // namespace kuzu::storage

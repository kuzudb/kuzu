#include "storage/wal/checksum_writer.h"

#include <cstring>

#include "common/checksum.h"
#include "common/serializer/serializer.h"

namespace kuzu::storage {
static constexpr uint64_t ENTRY_BUFFER_SIZE = common::KUZU_PAGE_SIZE;

ChecksumWriter::ChecksumWriter(std::shared_ptr<common::Writer> writer, MemoryManager& memoryManager)
    : writer(std::move(writer)), serializer(std::make_unique<common::Serializer>(this->writer)),
      entryBuffer(memoryManager.allocateBuffer(false, ENTRY_BUFFER_SIZE)) {}

void ChecksumWriter::write(const uint8_t* data, uint64_t size) {
    KU_ASSERT(currentEntrySize.has_value());
    KU_ASSERT(*currentEntrySize + size <= ENTRY_BUFFER_SIZE);
    std::memcpy(entryBuffer->getData() + *currentEntrySize, data, size);
    *currentEntrySize += size;
}

uint64_t ChecksumWriter::getSize() const {
    return writer->getSize();
}

void ChecksumWriter::clear() {
    writer->clear();
    currentEntrySize.reset();
}

void ChecksumWriter::flush() {
    KU_ASSERT(!currentEntrySize.has_value());
    writer->flush();
}

void ChecksumWriter::sync() {
    writer->sync();
}

void ChecksumWriter::startEntry() {
    KU_ASSERT(!currentEntrySize.has_value());
    currentEntrySize = 0;
}

void ChecksumWriter::finishEntry() {
    KU_ASSERT(currentEntrySize.has_value());
    const auto checksum = common::checksum(entryBuffer->getData(), *currentEntrySize);
    serializer->serializeValue(checksum);
    serializer->write(entryBuffer->getData(), *currentEntrySize);
    currentEntrySize.reset();
}
} // namespace kuzu::storage

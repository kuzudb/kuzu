#include "storage/wal/checksum_writer.h"

#include <cstring>

#include "common/checksum.h"
#include "common/serializer/serializer.h"

namespace kuzu::storage {
static constexpr uint64_t ENTRY_BUFFER_SIZE = common::KUZU_PAGE_SIZE;

ChecksumSerializer::ChecksumSerializer(std::shared_ptr<common::Writer> outputWriter,
    MemoryManager& memoryManager)
    : writer(std::make_shared<ChecksumWriter>(std::move(outputWriter), memoryManager)),
      serializer(writer) {}

ChecksumWriter::ChecksumWriter(std::shared_ptr<common::Writer> outputWriter,
    MemoryManager& memoryManager)
    : outputSerializer(std::move(outputWriter)), currentEntrySize(0),
      entryBuffer(memoryManager.allocateBuffer(false, ENTRY_BUFFER_SIZE)) {}

void ChecksumWriter::write(const uint8_t* data, uint64_t size) {
    KU_ASSERT(currentEntrySize + size <= ENTRY_BUFFER_SIZE);
    std::memcpy(entryBuffer->getData() + currentEntrySize, data, size);
    currentEntrySize += size;
}

void ChecksumWriter::clear() {
    currentEntrySize = 0;
}

void ChecksumWriter::flush() {
    const auto checksum = common::checksum(entryBuffer->getData(), currentEntrySize);
    outputSerializer.write(entryBuffer->getData(), currentEntrySize);
    outputSerializer.serializeValue(checksum);
    clear();
}

uint64_t ChecksumWriter::getSize() const {
    return entryBuffer->getBuffer().size();
}

void ChecksumWriter::sync() {}

} // namespace kuzu::storage

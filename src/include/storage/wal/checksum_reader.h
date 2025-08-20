#pragma once

#include <memory>
#include <optional>

#include "common/serializer/reader.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace storage {
class ChecksumReader : public common::Reader {
public:
    explicit ChecksumReader(std::unique_ptr<Reader> reader, MemoryManager& memoryManager);

    void read(uint8_t* data, uint64_t size) override;
    bool finished() override;

    // Reads + stores the checksum for the next entry
    void startEntry();
    // Computes + verifies the checksum for the entry that has just been read
    void finishEntry();

private:
    std::unique_ptr<common::Deserializer> deserializer;

    uint64_t currentEntrySize;
    std::optional<uint64_t> currentChecksum;
    std::unique_ptr<MemoryBuffer> entryBuffer;
};
} // namespace storage
} // namespace kuzu

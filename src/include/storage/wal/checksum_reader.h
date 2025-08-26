#pragma once

#include <memory>

#include "common/serializer/deserializer.h"
#include "common/serializer/reader.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu {
namespace storage {
class ChecksumReader : public common::Reader {
public:
    explicit ChecksumReader(common::FileInfo& fileInfo, MemoryManager& memoryManager);

    void read(uint8_t* data, uint64_t size) override;
    bool finished() override;

    // Reads the stored checksum
    // Also computes + verifies the checksum for the entry that has just been read against the
    // stored value
    void finishEntry(std::string_view checksumMismatchMessage);

    uint64_t getReadOffset() const;

private:
    common::Deserializer deserializer;

    uint64_t currentEntrySize;
    std::unique_ptr<MemoryBuffer> entryBuffer;
};
} // namespace storage
} // namespace kuzu

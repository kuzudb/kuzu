#pragma once

#include <vector>

#include "common/serializer/writer.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu::storage {
class ShadowFile;
}
namespace kuzu {
namespace common {

class MetadataWriter final : public Writer {
public:
    MetadataWriter(storage::MemoryManager* mm, storage::FileHandle* fileHandle);
    void write(const uint8_t* data, uint64_t size) override;

    void flush(storage::ShadowFile& shadowFile) const;

private:
    bool needNewBuffer(uint64_t size) const;

private:
    std::vector<std::unique_ptr<storage::MemoryBuffer>> buffers;
    storage::MemoryManager* mm;
    storage::FileHandle* fileHandle;
    uint64_t bufferOffset;
};

} // namespace common
} // namespace kuzu

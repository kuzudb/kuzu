#pragma once

#include <vector>

#include "common/serializer/writer.h"
#include "storage/buffer_manager/memory_manager.h"

namespace kuzu::storage {
struct PageRange;
class ShadowFile;
} // namespace kuzu::storage

namespace kuzu {
namespace common {

class MetaWriter final : public Writer {
public:
    explicit MetaWriter(storage::MemoryManager* mm);

    void write(const uint8_t* data, uint64_t size) override;

    std::span<uint8_t> getPage(page_idx_t pageIdx) const {
        KU_ASSERT(pageIdx < pages.size());
        return pages[pageIdx]->getBuffer();
    }

    storage::PageRange flush(storage::FileHandle* fileHandle,
        storage::ShadowFile& shadowFile) const;

private:
    bool needNewBuffer(uint64_t size) const;

private:
    std::vector<std::unique_ptr<storage::MemoryBuffer>> pages;
    storage::MemoryManager* mm;
    uint64_t pageOffset;
};

} // namespace common
} // namespace kuzu

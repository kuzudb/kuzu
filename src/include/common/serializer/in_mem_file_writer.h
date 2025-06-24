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
class BufferedFileWriter;

class InMemFileWriter final : public Writer {
public:
    explicit InMemFileWriter(storage::MemoryManager& mm);

    void write(const uint8_t* data, uint64_t size) override;

    std::span<uint8_t> getPage(page_idx_t pageIdx) const {
        KU_ASSERT(pageIdx < pages.size());
        return pages[pageIdx]->getBuffer();
    }

    // TODO: Should move this interface to somewhere else. Not part of Writer interface.
    storage::PageRange flush(storage::FileHandle* fileHandle,
        storage::ShadowFile& shadowFile) const;
    void flush(BufferedFileWriter& writer) const;

    uint64_t getSize() const override {
        uint64_t size = pages.size() > 1 ? KUZU_PAGE_SIZE * (pages.size() - 1) : 0;
        return size + pageOffset;
    }

    void clear() override {
        pages.clear();
        pageOffset = 0;
    }

    void flush() override {
        // DO NOTHING: InMemWriter does not need to flush.
    }
    void sync() override {
        // DO NOTHING: InMemWriter does not need to sync.
    }

private:
    bool needNewBuffer(uint64_t size) const;

private:
    std::vector<std::unique_ptr<storage::MemoryBuffer>> pages;
    storage::MemoryManager& mm;
    uint64_t pageOffset;
};

} // namespace common
} // namespace kuzu

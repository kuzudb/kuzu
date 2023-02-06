#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <stack>

#include "common/configs.h"
#include "storage/buffer_manager/buffer_manager.h"

namespace kuzu {
namespace storage {

struct MemoryBlock {

public:
    explicit MemoryBlock(common::page_idx_t pageIdx, uint8_t* data)
        : size(common::LARGE_PAGE_SIZE), pageIdx(pageIdx), data(data) {}

public:
    uint64_t size;
    common::page_idx_t pageIdx;
    uint8_t* data;
};

// Memory manager for allocating/reclaiming large intermediate memory blocks. It can allocate a
// memory block with fixed size of LARGE_PAGE_SIZE from the buffer manager.
class MemoryManager {
public:
    explicit MemoryManager(BufferManager* bm) : bm(bm) {
        // Because the memory manager only manages blocks in memory, this file should never be
        // created, opened, or written to. It's a place holder name. We keep the name for logging
        // purposes.
        fh = std::make_shared<FileHandle>(
            "mm-place-holder-file-name", FileHandle::O_IN_MEM_TEMP_FILE);
    }

    std::unique_ptr<MemoryBlock> allocateBlock(bool initializeToZero = false);

    void freeBlock(common::page_idx_t pageIdx);

private:
    std::shared_ptr<FileHandle> fh;
    BufferManager* bm;
    std::stack<common::page_idx_t> freePages;
    std::mutex memMgrLock;
};
} // namespace storage
} // namespace kuzu

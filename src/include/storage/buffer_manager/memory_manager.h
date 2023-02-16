#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <stack>

#include "common/constants.h"
#include "storage/buffer_manager/buffer_manager.h"

namespace kuzu {
namespace storage {

class MemoryAllocator;

class BlockHandle {
public:
    BlockHandle(
        MemoryAllocator* allocator, common::page_idx_t blockIdx, uint8_t* data, uint64_t pageSize);
    ~BlockHandle();

public:
    common::page_idx_t blockIdx;
    uint8_t* data;
    uint64_t blockSize;
    MemoryAllocator* allocator;
};

class MemoryAllocator {
    friend class BlockHandle;

public:
    MemoryAllocator(BufferManager* bm, common::PageSizeClass sizeClass);
    ~MemoryAllocator();

    std::unique_ptr<BlockHandle> allocateBlock(bool initializeToZero = false);

private:
    void freeBlock(common::page_idx_t pageIdx);

private:
    BufferManager* bm;
    std::unique_ptr<FileHandle> fh;
    uint64_t pageSize;
    std::stack<common::page_idx_t> freePages;
    std::mutex allocatorLock;
};

/*
 * The Memory Manager (MM) is used for allocating/reclaiming intermediate memory blocks.
 * It can allocate a memory block with a given size class from the buffer manager backed by a temp
 * in-mem file.
 *
 * Internally, MM uses a MemoryAllocator for each size class. The MemoryAllocator is holding a temp
 * in-mem file handle, and responsible for allocating/reclaiming memory blocks of its size class
 * from the buffer manager.
 * The MemoryAllocator keeps track of free pages in the temp in-mem file, so that it can reuse those
 * freed pages without allocating new pages.
 * The MemoryAllocator is thread-safe, so that multiple threads can allocate/reclaim memory blocks
 * with the same size class at the same time.
 *
 * MM will return a BlockHandle to the caller, which is a wrapper of the allocated memory block, and
 * it will automatically call its allocator to reclaim the memory block when it is destroyed.
 */
class MemoryManager {

public:
    explicit MemoryManager(BufferManager* bm) {
        for (auto sizeClass : common::BufferPoolConstants::PAGE_SIZE_CLASSES) {
            allocators.push_back(std::make_unique<MemoryAllocator>(bm, sizeClass));
        }
    }

    std::unique_ptr<BlockHandle> allocateBlock(
        common::PageSizeClass sizeClass, bool initializeToZero = false);

private:
    std::vector<std::unique_ptr<MemoryAllocator>> allocators;
};
} // namespace storage
} // namespace kuzu

#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <stack>

#include "common/types/types.h"

namespace kuzu {
namespace storage {

class MemoryAllocator;
class BMFileHandle;
class BufferManager;

class MemoryBuffer {
public:
    MemoryBuffer(MemoryAllocator* allocator, common::page_idx_t blockIdx, uint8_t* buffer);
    ~MemoryBuffer();

public:
    uint8_t* buffer;
    common::page_idx_t pageIdx;
    MemoryAllocator* allocator;
};

class MemoryAllocator {
    friend class MemoryBuffer;

public:
    explicit MemoryAllocator(BufferManager* bm);
    ~MemoryAllocator();

    std::unique_ptr<MemoryBuffer> allocateBuffer(bool initializeToZero = false);
    inline common::page_offset_t getPageSize() const { return pageSize; }

private:
    void freeBlock(common::page_idx_t pageIdx);

private:
    std::unique_ptr<BMFileHandle> fh;
    BufferManager* bm;
    common::page_offset_t pageSize;
    std::stack<common::page_idx_t> freePages;
    std::mutex allocatorLock;
};

/*
 * The Memory Manager (MM) is used for allocating/reclaiming intermediate memory blocks.
 * It can allocate a memory buffer of size PAGE_256KB from the buffer manager backed by a
 * BMFileHandle with temp in-mem file.
 *
 * Internally, MM uses a MemoryAllocator. The MemoryAllocator is holding the BMFileHandle backed by
 * a temp in-mem file, and responsible for allocating/reclaiming memory buffers of its size class
 * from the buffer manager. The MemoryAllocator keeps track of free pages in the BMFileHandle, so
 * that it can reuse those freed pages without allocating new pages. The MemoryAllocator is
 * thread-safe, so that multiple threads can allocate/reclaim memory blocks with the same size class
 * at the same time.
 *
 * MM will return a MemoryBuffer to the caller, which is a wrapper of the allocated memory block,
 * and it will automatically call its allocator to reclaim the memory block when it is destroyed.
 */
class MemoryManager {
public:
    explicit MemoryManager(BufferManager* bm) : bm{bm} {
        allocator = std::make_unique<MemoryAllocator>(bm);
    }

    inline std::unique_ptr<MemoryBuffer> allocateBuffer(bool initializeToZero = false) {
        return allocator->allocateBuffer(initializeToZero);
    }
    inline BufferManager* getBufferManager() const { return bm; }

private:
    BufferManager* bm;
    std::unique_ptr<MemoryAllocator> allocator;
};
} // namespace storage
} // namespace kuzu

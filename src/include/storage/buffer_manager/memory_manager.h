#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <stack>

#include "common/constants.h"
#include "common/types/types.h"
#include <span>

namespace kuzu {

namespace common {
class VirtualFileSystem;
}

namespace storage {

class MemoryManager;
class FileHandle;
class BufferManager;
class ChunkedNodeGroup;

class MemoryBuffer {
    friend class Spiller;

public:
    KUZU_API MemoryBuffer(MemoryManager* mm, common::page_idx_t blockIdx, uint8_t* buffer,
        uint64_t size = common::TEMP_PAGE_SIZE);
    KUZU_API ~MemoryBuffer();
    DELETE_COPY_AND_MOVE(MemoryBuffer);

    std::span<uint8_t> getBuffer() const {
        KU_ASSERT(!evicted);
        return buffer;
    }
    uint8_t* getData() const { return getBuffer().data(); }

    MemoryManager* getMemoryManager() const { return mm; }

private:
    // Can be called multiple times safely
    void prepareLoadFromDisk();

    // Must only be called once before loading from disk
    void setSpilledToDisk(uint64_t filePosition);

private:
    std::span<uint8_t> buffer;
    uint64_t filePosition = UINT64_MAX;
    MemoryManager* mm;
    common::page_idx_t pageIdx;
    bool evicted;
};

/*
 * The Memory Manager (MM) is used for allocating/reclaiming intermediate memory blocks.
 * It can allocate a memory buffer of size PAGE_256KB from the buffer manager backed by a
 * BMFileHandle with temp in-mem file.
 *
 * The MemoryManager holds a BMFileHandle backed by
 * a temp in-mem file, and is responsible for allocating/reclaiming memory buffers of its size class
 * from the buffer manager. The MemoryManager keeps track of free pages in the BMFileHandle, so
 * that it can reuse those freed pages without allocating new pages. The MemoryManager is
 * thread-safe, so that multiple threads can allocate/reclaim memory blocks with the same size class
 * at the same time.
 *
 * MM will return a MemoryBuffer to the caller, which is a wrapper of the allocated memory block,
 * and it will automatically call its allocator to reclaim the memory block when it is destroyed.
 */
class KUZU_API MemoryManager {
    friend class MemoryBuffer;

public:
    MemoryManager(BufferManager* bm, common::VirtualFileSystem* vfs);

    ~MemoryManager() = default;

    std::unique_ptr<MemoryBuffer> mallocBuffer(bool initializeToZero, uint64_t size);
    std::unique_ptr<MemoryBuffer> allocateBuffer(bool initializeToZero = false,
        uint64_t size = common::TEMP_PAGE_SIZE);
    common::page_offset_t getPageSize() const { return pageSize; }

    BufferManager* getBufferManager() const { return bm; }

private:
    void freeBlock(common::page_idx_t pageIdx, std::span<uint8_t> buffer);
    std::span<uint8_t> mallocBufferInternal(bool initializeToZero, uint64_t size);

private:
    FileHandle* fh;
    BufferManager* bm;
    common::page_offset_t pageSize;
    std::stack<common::page_idx_t> freePages;
    std::mutex allocatorLock;
};

} // namespace storage
} // namespace kuzu

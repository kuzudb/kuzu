#pragma once

#include <vector>

#include "common/constants.h"
#include "common/types/types.h"
#include "storage/buffer_manager/file_handle.h"

namespace kuzu {
namespace storage {

class Frame {
    friend class BufferManager;

public:
    explicit Frame(common::page_offset_t pageSize, uint8_t* buffer);
    ~Frame() noexcept(false);

private:
    void resetFrameWithoutLock();
    bool acquireFrameLock(bool block);
    inline void releaseFrameLock() { frameLock.clear(); }
    inline void setIsDirty(bool _isDirty) { isDirty = _isDirty; }
    void releaseBuffer();

private:
    // fileHandlePtr and pageIdx identify the file and the page in file whose data the buffer is
    // maintaining. pageIdx of -1u means that the frame is empty, i.e. it has no data.
    std::atomic<uint64_t> fileHandlePtr;
    std::atomic<common::page_idx_t> pageIdx;
    std::atomic<uint32_t> pinCount;

    bool isDirty;
    uint8_t* buffer;
    common::page_offset_t pageSize;
    std::atomic<uint64_t> evictTimestamp;
    std::atomic_flag frameLock;
};

// The buffer pool holds a vector of frames with the same size class. Our buffer manager holds
// multiple buffer pools with different size classes.
// Each buffer pool is backed by one or more mmap regions, which are allocated through `mmap()`
// system calls. For now, we assume the buffer pool is only destroyed when the process exits, which
// will automatically unmap all mapped regions.
class BufferPool {
    friend class BufferManager;
    // MMAP_REGION_MAX_SIZE is the max size of virtual memory that can be allocated by mmap in a
    // single call.
    static constexpr std::size_t MMAP_REGION_MAX_SIZE = (std::size_t)-1;

public:
    explicit BufferPool(
        uint64_t maxSize, common::PageSizeClass sizeClass = common::PageSizeClass::PAGE_4KB);

    void resize(uint64_t newSize);

    void moveClockHand(uint64_t newClockHand);

private:
    void allocateMmapRegion(std::size_t regionSize);

private:
    common::PageSizeClass sizeClass;
    std::vector<std::unique_ptr<Frame>> frames;
    std::atomic<uint64_t> clockHand;
};

} // namespace storage
} // namespace kuzu

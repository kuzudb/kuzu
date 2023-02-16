#include "storage/buffer_manager/buffer_pool.h"

#include <sys/mman.h>

#include "common/constants.h"
#include "common/exception.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

Frame::Frame(page_offset_t pageSize, std::uint8_t* buffer)
    : buffer{buffer}, pageSize{pageSize}, frameLock{ATOMIC_FLAG_INIT} {
    resetFrameWithoutLock();
}

Frame::~Frame() noexcept(false) {
    auto count = pinCount.load();
    if (0 != count && -1u != count) {
        throw BufferManagerException(
            "Deleting buffer that is still pinned. pinCount: " + std::to_string(count) +
            " pageIdx: " + std::to_string(pageIdx));
    }
}

void Frame::resetFrameWithoutLock() {
    fileHandlePtr = -1u;
    pageIdx = -1u;
    pinCount = -1u;
    isDirty = false;
    evictTimestamp = 0;
}

bool Frame::acquireFrameLock(bool block) {
    if (block) {
        while (frameLock.test_and_set()) // spinning
            ;
        return true;
    }
    return !frameLock.test_and_set();
}

void Frame::releaseBuffer() {
    int error = madvise(buffer, pageSize, MADV_DONTNEED);
    if (error) {
        throw BufferManagerException("Releasing frame buffer failed with error code " +
                                     std::to_string(error) + ": " +
                                     std::string(std::strerror(errno)));
    }
}

BufferPool::BufferPool(uint64_t maxSize, common::PageSizeClass sizeClass)
    : sizeClass{sizeClass}, clockHand{0} {
    auto pageSize = (page_offset_t)1 << sizeClass;
    auto numFrames = (uint64_t)(ceil((double)maxSize / (double)pageSize));
    uint64_t mmapSize = numFrames * pageSize;
    while (mmapSize > MMAP_REGION_MAX_SIZE) {
        allocateMmapRegion(MMAP_REGION_MAX_SIZE);
        mmapSize -= MMAP_REGION_MAX_SIZE;
    }
    if (mmapSize > 0) {
        allocateMmapRegion(mmapSize);
    }
}

void BufferPool::resize(uint64_t newSize) {
    auto pageSize = (page_offset_t)1 << sizeClass;
    auto currentSize = (uint64_t)frames.size() * pageSize;
    if (currentSize > newSize) {
        throw BufferManagerException("Resizing to a smaller Buffer Pool Size is unsupported.");
    }
    auto extraSize = newSize - currentSize;
    while (extraSize > MMAP_REGION_MAX_SIZE) {
        allocateMmapRegion(MMAP_REGION_MAX_SIZE);
        extraSize -= MMAP_REGION_MAX_SIZE;
    }
    if (extraSize > 0) {
        allocateMmapRegion(extraSize);
    }
}

void BufferPool::allocateMmapRegion(std::size_t regionSize) {
    auto pageSize = (page_offset_t)1 << sizeClass;
    auto numFramesAtOneTime = (std::size_t)(regionSize / pageSize);
    // Create a private anonymous mapping. The mapping is not shared with other processes and not
    // backed by any file, and its content are initialized to zero.
    auto mmapRegion = (uint8_t*)mmap(NULL, regionSize, PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS, -1 /* fd */, 0 /* offset */);
    if (mmapRegion == MAP_FAILED) {
        throw BufferManagerException(
            "Failed to allocate buffer pool with size " + std::to_string(regionSize) + " bytes.");
    }
    for (uint64_t i = 0u; i < numFramesAtOneTime; ++i) {
        frames.emplace_back(
            std::make_unique<Frame>(pageSize, mmapRegion + (uint64_t)(i * pageSize)));
    }
}

void BufferPool::moveClockHand(uint64_t newClockHand) {
    do {
        auto currClockHand = clockHand.load();
        if (currClockHand > newClockHand) {
            return;
        }
        if (clockHand.compare_exchange_strong(
                currClockHand, newClockHand, std::memory_order_seq_cst)) {
            return;
        }
    } while (true);
}

} // namespace storage
} // namespace kuzu

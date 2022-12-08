#pragma once

#include <mutex>
#include <vector>

#include "common/metric.h"
#include "storage/buffer_manager/file_handle.h"

using namespace kuzu::common;

namespace spdlog {
class logger;
}

namespace kuzu {
namespace storage {

struct BufferManagerMetrics {
    uint64_t numPins{0};
    // Number of pinning operations that required eviction from a Frame.
    uint64_t numEvicts{0};
    // Number of failed tries to evict the page from a Frame. This is incremented if either the
    // eviction routine fails to get the lock on the page that is in the Frame or the pinCount of
    // the Frame has increased after taking the locks of Frame and page.
    uint64_t numEvictFails{0};
    // Number of failed tried to evict the page frame a Frame because the Frame has been recently
    // accessed and hence is given a second chance.
    uint64_t numRecentlyAccessedWalkover{0};
    uint64_t numCacheHit{0};
    uint64_t numCacheMiss{0};
    uint64_t numDirtyPageWriteIO{0};
};

// A frame is a unit of buffer space having a fixed size of 4KB, where a single file page is
// read from the disk. Frame also stores other metadata to locate and maintain this buffer in the
// Buffer Manager.
class Frame {
    friend class BufferPool;

public:
    explicit Frame(uint64_t pageSize);
    ~Frame() noexcept(false);

private:
    void resetFrameWithoutLock();
    bool acquireFrameLock(bool block);
    void releaseFrameLock() { frameLock.clear(); }
    void setIsDirty(bool _isDirty) { isDirty = _isDirty; }

private:
    // fileHandlePtr and pageIdx identify the file and the page in file whose data the buffer is
    // maintaining. pageIdx of -1u means that the frame is empty, i.e. it has no data.
    atomic<uint64_t> fileHandlePtr;
    atomic<page_idx_t> pageIdx;
    atomic<uint32_t> pinCount;

    bool recentlyAccessed;
    bool isDirty;
    unique_ptr<uint8_t[]> buffer;
    atomic_flag frameLock;
};

// The BufferPool is a cache of file pages of a fixed size. It provides the high-level functionality
// of pin() and unpin() pages of files in memory and operates via their FileHandles
// to make the page data available in one of the frames. It uses CLOCK replacement policy to evict
// pages from frames, which is an approximate LRU policy that is based of FIFO-like operations.
class BufferPool {
    friend class BufferManager;

public:
    BufferPool(uint64_t pageSize, uint64_t maxSize);

    uint8_t* pin(FileHandle& fileHandle, page_idx_t pageIdx);

    // Pins a new page that has been added to the file. This means that the BufferManager does not
    // need to read the page from the file for now. Ensuring that the given pageIdx is new is the
    // responsibility of the caller.
    uint8_t* pinWithoutReadingFromFile(FileHandle& fileHandle, page_idx_t pageIdx);

    uint8_t* pinWithoutAcquiringPageLock(
        FileHandle& fileHandle, page_idx_t pageIdx, bool doNotReadFromFile);

    void setPinnedPageDirty(FileHandle& fileHandle, page_idx_t pageIdx);

    // The function assumes that the requested page is already pinned.
    void unpin(FileHandle& fileHandle, page_idx_t pageIdx);

    void unpinWithoutAcquiringPageLock(FileHandle& fileHandle, page_idx_t pageIdx);

    void resize(uint64_t newSize);

    // Note: These two functions that remove pages from frames is not designed for concurrency and
    // therefore not tested under concurrency. If this is called while other threads are accessing
    // the BM, it should work safely but this is not tested.
    void removeFilePagesFromFrames(FileHandle& fileHandle);

    void flushAllDirtyPagesInFrames(FileHandle& fileHandle);
    void updateFrameIfPageIsInFrameWithoutPageOrFrameLock(
        FileHandle& fileHandle, uint8_t* newPage, page_idx_t pageIdx);

    void removePageFromFrameWithoutFlushingIfNecessary(FileHandle& fileHandle, page_idx_t pageIdx);

private:
    uint8_t* pin(FileHandle& fileHandle, page_idx_t pageIdx, bool doNotReadFromFile);

    page_idx_t claimAFrame(FileHandle& fileHandle, page_idx_t pageIdx, bool doNotReadFromFile);

    bool fillEmptyFrame(
        page_idx_t frameIdx, FileHandle& fileHandle, page_idx_t pageIdx, bool doNotReadFromFile);

    bool tryEvict(
        page_idx_t frameIdx, FileHandle& fileHandle, page_idx_t pageIdx, bool doNotReadFromFile);

    void moveClockHand(uint64_t newClockHand);
    // Performs 2 actions:
    // 1) Clears the contents of the frame.
    // 2) Unswizzles the pageIdx in the frame.
    void clearFrameAndUnswizzleWithoutLock(
        const unique_ptr<Frame>& frame, FileHandle& fileHandleInFrame, page_idx_t pageIdxInFrame);
    void readNewPageIntoFrame(
        Frame& frame, FileHandle& fileHandle, page_idx_t pageIdx, bool doNotReadFromFile);

    void flushIfDirty(const unique_ptr<Frame>& frame);

    void removePageFromFrame(FileHandle& fileHandle, page_idx_t pageIdx, bool shouldFlush);

private:
    shared_ptr<spdlog::logger> logger;
    uint64_t pageSize;
    vector<unique_ptr<Frame>> bufferCache;
    atomic<uint64_t> clockHand;
    page_idx_t numFrames;

    BufferManagerMetrics bmMetrics;
};

} // namespace storage
} // namespace kuzu

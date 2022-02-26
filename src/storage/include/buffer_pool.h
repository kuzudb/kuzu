#pragma once

#include <mutex>
#include <vector>

#include "nlohmann/json.hpp"

#include "src/common/include/metric.h"
#include "src/storage/include/file_handle.h"

using namespace graphflow::common;
using namespace std;

namespace spdlog {
class logger;
}

namespace graphflow {
namespace storage {

struct BufferManagerMetrics {
    uint64_t numBufferHit;
    uint64_t numBufferMiss;
    uint64_t numReadIO;
    uint64_t numDirtyPageWriteIO;
};

// A frame is a unit of buffer space having a fixed size of 4KB, where a single file page is
// read from the disk. Frame also stores other metadata to locate and maintain this buffer in the
// Buffer Manager.
class Frame {
    friend class BufferPool;

public:
    Frame(uint64_t pageSize);
    ~Frame();

private:
    bool acquireFrameLock(bool block);
    void releaseFrameLock() { frameLock.clear(); }
    void setIsDirty(bool _isDirty) { isDirty = _isDirty; }

private:
    // fileHandle and pageIdx identify the file and the page in file whose data the buffer is
    // maintaining. pageIdx of -1u means that the frame is empty, i.e. it has no data.
    atomic<uint64_t> fileHandle;
    atomic<uint32_t> pageIdx;

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
    BufferPool(uint64_t pageSizeLog2, uint64_t maxSize);

    uint8_t* pin(FileHandle& fileHandle, uint32_t pageIdx);

    // Pins a new page that has been added to the file. This means that the BufferManager does not
    // need to read the page from the file for now. Ensuring that the given pageIdx is new is the
    // responsibility of the caller.
    uint8_t* pinWithoutReadingFromFile(FileHandle& fileHandle, uint32_t pageIdx);

    void setPinnedPageDirty(FileHandle& fileHandle, uint32_t pageIdx);

    // The function assumes that the requested page is already pinned.
    void unpin(FileHandle& fileHandle, uint32_t pageIdx);

    void resize(uint64_t newSize);

private:
    uint8_t* pin(FileHandle& fileHandle, uint32_t pageIdx, bool doNotReadFromFile);

    uint32_t claimAFrame(FileHandle& fileHandle, uint32_t pageIdx, bool doNotReadFromFile);

    bool fillEmptyFrame(
        uint32_t frameIdx, FileHandle& fileHandle, uint32_t pageIdx, bool doNotReadFromFile);

    bool tryEvict(
        uint32_t frameIdx, FileHandle& fileHandle, uint32_t pageIdx, bool doNotReadFromFile);

    void moveClockHand(uint64_t newClockHand);

    void readNewPageIntoFrame(
        Frame& frame, FileHandle& fileHandle, uint32_t pageIdx, bool doNotReadFromFile);

    inline bool isAFrame(uint64_t pageIdx) { return UINT64_MAX != pageIdx; }

private:
    shared_ptr<spdlog::logger> logger;
    uint64_t pageSizeLog2;
    vector<unique_ptr<Frame>> bufferCache;
    atomic<uint64_t> clockHand;
    uint32_t numFrames;

    BufferManagerMetrics bmMetrics;
    atomic<uint64_t> numPins{0};
    // Number of pinning operations that required eviction from a Frame.
    atomic<uint64_t> numEvicts{0};
    // Number of failed tries to evict the page from a Frame. This is incremented if either the
    // eviction routine fails to get the lock on the page that is in the Frame or the pinCount of
    // the Frame has increased after taking the locks of Frame and page.
    atomic<uint64_t> numEvictFails{0};
    // Number of failed tried to evict the page frame a Frame because the Frame has been recently
    // accessed and hence is given a second chance.
    atomic<uint64_t> numRecentlyAccessedWalkover{0};
};

} // namespace storage
} // namespace graphflow

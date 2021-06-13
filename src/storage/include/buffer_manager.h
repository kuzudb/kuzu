#pragma once

#include <mutex>
#include <vector>

#include "nlohmann/json.hpp"

#include "src/storage/include/file_handle.h"

using namespace std;

namespace spdlog {
class logger;
}

namespace graphflow {
namespace storage {

class BufferManager;

// A frame is a unit of buffer space having a fixed size of 4KB, where a single file page is
// read from the disk. Frame also stores other metadata to locate and maintain this buffer in the
// Buffer Manager.
class Frame {
    friend class BufferManager;

public:
    Frame();
    ~Frame();

    bool acquire(bool block);
    void release() { frameLock.clear(); }

private:
    // fileHandle and pageIdx identify the file and the page in file whose data the buffer is
    // maintaining. pageIdx of -1u means that the frame is empty, i.e. it has no data.
    atomic<uint64_t> fileHandle;
    atomic<uint32_t> pageIdx;

    atomic<uint32_t> pinCount;
    bool recentlyAccessed;
    unique_ptr<uint8_t[]> buffer;
    atomic_flag frameLock;
};

// The Buffer Manager is the cache of database file pages. It provides the high-level functionality
// of pin() and unpin() to the Column/Lists in the storage layer, and operates via their FileHandles
// to make the page data available in one of the frames. It uses CLOCK replacement policy to evict
// pages from frames, which is an approximate LRU policy that is based of FIFO-like operations.
class BufferManager {

public:
    BufferManager(uint64_t maxSize);
    ~BufferManager();

    // The function assumes that the requested page is already pinned.
    const uint8_t* get(FileHandle& fileHandle, uint32_t pageIdx);

    const uint8_t* pin(FileHandle& fileHandle, uint32_t pageIdx);

    // The function assumes that the requested page is already pinned.
    void unpin(FileHandle& fileHandle, uint32_t pageIdx);

    unique_ptr<nlohmann::json> debugInfo();

private:
    uint32_t claimAFrame(FileHandle& fileHandle, uint32_t pageIdx);

    bool fillEmptyFrame(uint32_t frameIdx, FileHandle& fileHandle, uint32_t pageIdx);

    bool tryEvict(uint32_t frameIdx, FileHandle& fileHandle, uint32_t pageIdx);

    void moveClockHand(uint64_t newClockHand);

    void readNewPageIntoFrame(Frame& frame, FileHandle& fileHandle, uint32_t pageIdx);

    inline bool isAFrame(uint64_t pageIdx) { return UINT64_MAX != pageIdx; }

private:
    shared_ptr<spdlog::logger> logger;
    vector<Frame> bufferCache;
    atomic<uint64_t> clockHand;
    uint32_t numFrames;

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

#pragma once

#include <mutex>
#include <vector>

#include "common/metric.h"
#include "concurrentqueue.h"
#include "storage/buffer_manager/buffer_pool.h"
#include "storage/buffer_manager/file_handle.h"

namespace spdlog {
class logger;
}

namespace kuzu {
namespace storage {

constexpr common::page_idx_t INVALID_PAGE_IDX = -1u;

struct EvictionCandidate {
    Frame* frame = nullptr;
    common::page_idx_t frameIdx = -1u;
    uint64_t evictTimestamp = -1u;
};

/**
 *
 * The Buffer Manager (BM) is a centralized manager of database memory resources.
 * It provides two main functionalities:
 * (1) it provides the high-level functionality of pin() and unpin() the pages of the database files
 * used by the Column/Lists in the storage layer, and operates via their FileHandles to make the
 * page data available in one of the frames.
 * (2) it supports the MemoryManager (MM) to allocate memory blocks that are not backed by any disk
 * files. Similar to disk files, MM provides in-mem file handles to the BM to pin/unpin pages.
 *
 * Additionally, for disk files, buffer manager provides the functionality to set pinned pages as
 * dirty, which will be safely written back to disk when the page is evicted.
 *
 * Currently the BM has internal BufferPools to cache pages of 2 size: DEFAULT_PAGE_SIZE and
 * LARGE_PAGE_SIZE, both of which are defined in constants.h. Each buffer pool has its own virtual
 * memory space, which are independent to each other. The BM uses mmap to allocate virtual memory
 * space for buffer pools. Each buffer pool has the same max size limit as the BM does. In this way,
 * buffer pools can share the same physical memory space between each other, where the
 * responsibility to handle memory fragmentation is delegated to the OS.
 * We followed Umbra's idea (http://db.in.tum.de/~freitag/papers/p29-neumann-cidr20.pdf) on this.
 *
 * The BM uses queue based replacement policy to evict pages from frames, which is also adopted in
 * Umbra and DuckDB. See comments above `claimAFrame()` for more details.
 *
 * All access to the BM is through a FileHandle. This design is to de-centralize the management of
 * locks and mappings from pages to frames from the BM to each file handle itself.
 * Thus each on-disk file should have a unique FileHandle, and each page size class in MM also has a
 * unique in-mem file-based FileHandle.
 * Users can also unpin their pages and then the BM might spill them to disk. The behavior of what
 * is guaranteed to be kept in memory and what can be spilled to disk is directly determined by the
 * pin/unpin calls of the users of BM.
 *
 * BufferManager supports a special pin function called pinWithoutReadingFromFile. A caller can
 * call the common::page_idx_t newPageIdx = fh::addNewPage() function on the FileHandle fh they
 * have, and then call bm::pinWithoutReadingFromFile(fh, newPageIdx), and the BM will not try to
 * read this page from the file (because the page has not yet been written).
 */
class BufferManager {

public:
    explicit BufferManager(uint64_t maxSize);
    ~BufferManager();

    uint8_t* pin(FileHandle& fileHandle, common::page_idx_t pageIdx);
    uint8_t* pinWithoutReadingFromFile(FileHandle& fileHandle, common::page_idx_t pageIdx);
    uint8_t* pinWithoutAcquiringPageLock(
        FileHandle& fileHandle, common::page_idx_t pageIdx, bool doNotReadFromFile);

    void setPinnedPageDirty(FileHandle& fileHandle, common::page_idx_t pageIdx);

    // The function assumes that the requested page is already pinned.
    void unpin(FileHandle& fileHandle, common::page_idx_t pageIdx);
    void unpinWithoutAcquiringPageLock(FileHandle& fileHandle, common::page_idx_t pageIdx);

    void removeFilePagesFromFrames(FileHandle& fileHandle);
    void flushAllDirtyPagesInFrames(FileHandle& fileHandle);
    void updateFrameIfPageIsInFrameWithoutPageOrFrameLock(
        FileHandle& fileHandle, uint8_t* newPage, common::page_idx_t pageIdx);
    void removePageFromFrameWithoutFlushingIfNecessary(
        FileHandle& fileHandle, common::page_idx_t pageIdx);

    void resize(uint64_t newSize);

private:
    uint8_t* pin(FileHandle& fileHandle, common::page_idx_t pageIdx, bool doNotReadFromFile);
    common::page_idx_t claimAFrame(
        FileHandle& fileHandle, common::page_idx_t pageIdx, bool doNotReadFromFile);
    bool fillEmptyFrame(
        Frame* frame, FileHandle& fileHandle, common::page_idx_t pageIdx, bool doNotReadFromFile);
    bool tryEvict(Frame* frame, common::page_idx_t pageIdx, bool doNotReadFromFile);

    static void clearFrameAndUnSwizzleWithoutLock(
        Frame* frame, FileHandle& fileHandle, common::page_idx_t pageIdx);
    static void readNewPageIntoFrame(
        Frame& frame, FileHandle& fileHandle, common::page_idx_t pageIdx, bool doNotReadFromFile);
    static void flushIfDirty(Frame* frame);
    void removePageFromFrame(FileHandle& fileHandle, common::page_idx_t pageIdx, bool shouldFlush);

    void enEvictionQueue(Frame* frame, common::page_idx_t frameIdx);
    void purgeEvictionQueue();

private:
    std::shared_ptr<spdlog::logger> logger;
    std::vector<std::unique_ptr<BufferPool>> bufferCache;
    std::atomic<uint64_t> usedMemory;
    std::atomic<uint64_t> maxMemory;
    std::atomic<uint64_t> numEvictionQueueInsertions;
    std::unique_ptr<moodycamel::ConcurrentQueue<EvictionCandidate>> evictionQueue;
};

} // namespace storage
} // namespace kuzu

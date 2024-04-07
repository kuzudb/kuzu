#pragma once

#include <functional>
#include <vector>

#include "storage/buffer_manager/bm_file_handle.h"
#include "storage/buffer_manager/locked_queue.h"

namespace kuzu {
namespace storage {

// This class keeps state info for pages potentially can be evicted.
// The page state of a candidate is set to MARKED when it is first enqueued. After enqueued, if the
// candidate was recently accessed, it is no longer immediately evictable. See the state transition
// diagram above `BufferManager` class declaration for more details.
struct EvictionCandidate {
    // If the candidate is Marked and its version is the same as the one kept inside the candidate,
    // it is evictable.
    inline bool isEvictable(uint64_t currPageStateAndVersion) const {
        return PageState::getState(currPageStateAndVersion) == PageState::MARKED &&
               PageState::getVersion(currPageStateAndVersion) == pageVersion;
    }
    // If the candidate was recently read optimistically, it is second chance evictable.
    inline bool isSecondChanceEvictable(uint64_t currPageStateAndVersion) const {
        return PageState::getState(currPageStateAndVersion) == PageState::UNLOCKED &&
               PageState::getVersion(currPageStateAndVersion) == pageVersion;
    }

    BMFileHandle* fileHandle = nullptr;
    common::page_idx_t pageIdx = common::INVALID_PAGE_IDX;
    PageState* pageState = nullptr;
    // The version of the corresponding page at the time the candidate is enqueued.
    uint64_t pageVersion = -1u;

    inline bool operator==(const EvictionCandidate& other) const {
        return fileHandle == other.fileHandle && pageIdx == other.pageIdx &&
               pageState == other.pageState && pageVersion == other.pageVersion;
    }
};

class EvictionQueue {
public:
    EvictionQueue() { queue = std::make_unique<LockedQueue<EvictionCandidate>>(); }

    inline void enqueue(EvictionCandidate& candidate) {
        std::shared_lock sLck{mtx};
        queue->enqueue(candidate);
    }
    inline void enqueue(BMFileHandle* fileHandle, common::page_idx_t pageIdx, PageState* pageState,
        uint64_t pageVersion) {
        std::shared_lock sLck{mtx};
        queue->enqueue(EvictionCandidate{fileHandle, pageIdx, pageState, pageVersion});
    }
    inline bool dequeue(EvictionCandidate& candidate) {
        std::shared_lock sLck{mtx};
        return queue->try_dequeue(candidate);
    }

    void removeNonEvictableCandidates();

    void removeCandidatesForFile(BMFileHandle& fileHandle);

private:
    std::shared_mutex mtx;
    std::unique_ptr<LockedQueue<EvictionCandidate>> queue;
};

/**
 * The Buffer Manager (BM) is a centralized manager of database memory resources.
 * It provides two main functionalities:
 * 1) it provides the high-level functionality to pin() and unpin() the pages of the database files
 * used by storage structures, such as the Column, Lists, or HashIndex in the storage layer, and
 * operates via their BMFileHandle to read/write the page data into/out of one of the frames.
 * 2) it provides optimistic read of pages, which optimistically read unlocked or marked pages
 * without acquiring locks.
 * 3) it supports the MemoryManager (MM) to allocate memory buffers that are not
 * backed by any disk files. Similar to disk files, MM provides in-mem file handles to the BM to
 * pin/unpin pages. Pin happens when MM tries to allocate a new memory buffer, and unpin happens
 * when MM tries to reclaim a memory buffer.
 *
 * Specifically, in BM's context, page is the basic management unit of data in a file. The file can
 * be a disk file, such as a column file, or an in-mem file, such as an temp in-memory file kept by
 * the MM. Frame is the basic management unit of data resides in a VMRegion, namely in a virtual
 * memory space. Each page is uniquely mapped to a frame, and it can be cached into or evicted from
 * the frame. See `VMRegion` for more details.
 *
 * When users unpin their pages, the BM might spill them to disk. The behavior of what is guaranteed
 * to be kept in frame and what can be spilled to disk is directly determined by the pin/unpin
 * calls of the users.
 *
 * Also, BM provides some specialized functionalities for WAL files:
 * 1) it supports the caller to set pinned pages as dirty, which will be safely written back to disk
 * when the pages are evicted;
 * 2) it supports the caller to flush or remove pages from the BM;
 * 3) it supports the caller to directly update the content of a frame.
 *
 * All accesses to the BM are through a FileHandle. This design is to de-centralize the management
 * of page states from the BM to each file handle itself. Thus each on-disk file should have a
 * unique BMFileHandle, and MM also holds a unique BMFileHandle, which is backed by an temp in-mem
 * file, for all memory buffer allocations
 *
 * To start a Database, users need to specify the max size of the memory usage (`maxSize`) in BM.
 * If users don't specify the value, the system will set maxSize to available physical mem *
 * DEFAULT_PHY_MEM_SIZE_RATIO_FOR_BM (defined in constants.h).
 * The BM relies on virtual memory regions mapped through `mmap` to anonymous address spaces.
 * 1) For disk pages, BM allocates a virtual memory region of DEFAULT_VM_REGION_MAX_SIZE (defined in
 * constants.h), which is usually much larger than `maxSize`, and is expected to be large enough to
 * contain all disk pages. Each disk page in database files is directly mapped to a unique
 * PAGE_4KB_SIZE frame in the region.
 * 2) For each BMFileHandle backed by a temp in-mem file in MM, BM allocates a virtual memory region
 * of `maxSize` for it. Each memory buffer is mapped to a unique PAGE_256KB_SIZE frame in that
 * region. Both disk pages and memory buffers are all managed by the BM to make sure that actually
 * used physical memory doesn't go beyond max size specified by users. Currently, the BM uses a
 * queue based replacement policy and the MADV_DONTNEED hint to explicitly control evictions. See
 * comments above `claimAFrame()` for more details.
 *
 * Page states in BM:
 * A page can be in one of the four states: a) LOCKED, b) UNLOCKED, c) MARKED, d) EVICTED.
 * Every page is initialized as in the EVICTED state.
 * The state transition diagram of page X is as follows (oRead refers to optimisticRead):
 * Note: optimistic reads on UNLOCKED pages don't make any changes to pages' states. oRead on
 * UNLOCKED is omitted in the diagram.
 *
 *       7.2. pin(pY): evict pX.                       7.1. pin(pY): tryLock(pX)
 *    |<-------------------------|<------------------------------------------------------------|
 *    |                          |                           4. pin(pX)                        |
 *    |                          |<------------------------------------------------------------|
 *    |         1. pin(pX)       |        5. pin(pX)         6. pin(pY): 2nd chance eviction   |
 * EVICTED ------------------> LOCKED <-------------UNLOCKED ------------------------------> MARKED
 *                               |                      |             3. oRead(pX)             |
 *                               |                      <--------------------------------------|
 *                               |        2. unpin(pX): enqueue pX & increment version         |
 *                               ------------------------------------------------------------->
 *
 * 1. When page pX at EVICTED state and it is pinned, it transits to the Locked state. `pin` will
 * first acquire the exclusive lock on the page, then read the page from disk into its frame. The
 * caller can safely make changes to the page.
 * 2. When the caller finishes changes to the page, it calls `unpin`, which releases the lock on the
 * page, puts the page into the eviction queue, and increments its version. The page now transits to
 * the MARKED state. Note that currently the page is still cached, but it is ready to be evicted.
 * The page version number is used to identify any potential writes on the page. Each time a page
 * transits from LOCKED to MARKED state, we will increment its version. This happens when a page is
 * pinned, then unpinned. During the pin and unpin, we assume the page's content in its
 * corresponding frame might have changed, thus, we increment the version number to forbid stale
 * reads on it;
 * 3. The MARKED page can be optimistically read by the caller, setting the page's state to
 * UNLOCKED. For evicted pages, optimistic reads will trigger pin and unpin to read pages from disk
 * into frames.
 * 4. The MARKED page can be pinned again by the caller, setting the page's state to LOCKED.
 * 5. The UNLOCKED page can also be pinned again by the caller, setting the page's state to LOCKED.
 * 6. During eviction, UNLOCKED pages will be check if they are second chance evictable. If so, they
 * will be set to MARKED, and their eviction candidates will be moved back to the eviction queue.
 * 7. During eviction, if the page is in the MARKED state, it will be LOCKED first (7.1), then
 * removed from its frame, and set to EVICTED (7.2).
 *
 * The design is inspired by vmcache in the paper "Virtual-Memory Assisted Buffer Management"
 * (https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/_my_direct_uploads/vmcache.pdf).
 * We would also like to thank Fadhil Abubaker for doing the initial research and prototyping of
 * Umbra's design in his CS 848 course project:
 * https://github.com/fabubaker/kuzu/blob/umbra-bm/final_project_report.pdf.
 */

class BufferManager {
public:
    enum class PageReadPolicy : uint8_t { READ_PAGE = 0, DONT_READ_PAGE = 1 };

    BufferManager(uint64_t bufferPoolSize, uint64_t maxDBSize);
    ~BufferManager() = default;

    uint8_t* pin(BMFileHandle& fileHandle, common::page_idx_t pageIdx,
        PageReadPolicy pageReadPolicy = PageReadPolicy::READ_PAGE);
    void optimisticRead(BMFileHandle& fileHandle, common::page_idx_t pageIdx,
        const std::function<void(uint8_t*)>& func);
    // The function assumes that the requested page is already pinned.
    void unpin(BMFileHandle& fileHandle, common::page_idx_t pageIdx);

    // Currently, these functions are specifically used only for WAL files.
    void removeFilePagesFromFrames(BMFileHandle& fileHandle);
    void flushAllDirtyPagesInFrames(BMFileHandle& fileHandle);
    void updateFrameIfPageIsInFrameWithoutLock(BMFileHandle& fileHandle, uint8_t* newPage,
        common::page_idx_t pageIdx);
    void removePageFromFrameIfNecessary(BMFileHandle& fileHandle, common::page_idx_t pageIdx);

    // For files that are managed by BM, their FileHandles should be created through this function.
    inline std::unique_ptr<BMFileHandle> getBMFileHandle(const std::string& filePath, uint8_t flags,
        BMFileHandle::FileVersionedType fileVersionedType, common::VirtualFileSystem* vfs,
        common::PageSizeClass pageSizeClass = common::PAGE_4KB) {
        return std::make_unique<BMFileHandle>(filePath, flags, this, pageSizeClass,
            fileVersionedType, vfs);
    }
    inline common::frame_group_idx_t addNewFrameGroup(common::PageSizeClass pageSizeClass) {
        return vmRegions[pageSizeClass]->addNewFrameGroup();
    }
    inline void clearEvictionQueue() { evictionQueue = std::make_unique<EvictionQueue>(); }

private:
    static void verifySizeParams(uint64_t bufferPoolSize, uint64_t maxDBSize);

    bool claimAFrame(BMFileHandle& fileHandle, common::page_idx_t pageIdx,
        PageReadPolicy pageReadPolicy);
    // Return number of bytes freed.
    uint64_t tryEvictPage(EvictionCandidate& candidate);

    void cachePageIntoFrame(BMFileHandle& fileHandle, common::page_idx_t pageIdx,
        PageReadPolicy pageReadPolicy);
    void flushIfDirtyWithoutLock(BMFileHandle& fileHandle, common::page_idx_t pageIdx);
    void removePageFromFrame(BMFileHandle& fileHandle, common::page_idx_t pageIdx,
        bool shouldFlush);

    void addToEvictionQueue(BMFileHandle* fileHandle, common::page_idx_t pageIdx,
        PageState* pageState);

    inline uint64_t reserveUsedMemory(uint64_t size) { return usedMemory.fetch_add(size); }
    inline uint64_t freeUsedMemory(uint64_t size) {
        KU_ASSERT(usedMemory.load() >= size);
        return usedMemory.fetch_sub(size);
    }

    inline uint8_t* getFrame(BMFileHandle& fileHandle, common::page_idx_t pageIdx) {
        return vmRegions[fileHandle.getPageSizeClass()]->getFrame(fileHandle.getFrameIdx(pageIdx));
    }
    inline void releaseFrameForPage(BMFileHandle& fileHandle, common::page_idx_t pageIdx) {
        vmRegions[fileHandle.getPageSizeClass()]->releaseFrame(fileHandle.getFrameIdx(pageIdx));
    }

private:
    std::atomic<uint64_t> usedMemory;
    std::atomic<uint64_t> bufferPoolSize;
    std::atomic<uint64_t> numEvictionQueueInsertions;
    // Each VMRegion corresponds to a virtual memory region of a specific page size. Currently, we
    // hold two sizes of PAGE_4KB and PAGE_256KB.
    std::vector<std::unique_ptr<VMRegion>> vmRegions;
    std::unique_ptr<EvictionQueue> evictionQueue;
};

} // namespace storage
} // namespace kuzu

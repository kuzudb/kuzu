#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

#include "common/types/types.h"
#include "storage/enums/page_read_policy.h"
#include "storage/file_handle.h"

namespace kuzu {
namespace common {
class VirtualFileSystem;
};
namespace testing {
class EmptyBufferManagerTest;
};
namespace storage {
class ChunkedNodeGroup;
class Spiller;

// This class keeps state info for pages potentially can be evicted.
// The page state of a candidate is set to MARKED when it is first enqueued. After enqueued, if the
// candidate was recently accessed, it is no longer immediately evictable. See the state transition
// diagram above `BufferManager` class declaration for more details.
struct EvictionCandidate {
    friend class EvictionQueue;

    // If the candidate is Marked it is evictable.
    static bool isEvictable(uint64_t currPageStateAndVersion) {
        return PageState::getState(currPageStateAndVersion) == PageState::MARKED;
    }
    // If the candidate was recently read optimistically, it is second chance evictable.
    static bool isSecondChanceEvictable(uint64_t currPageStateAndVersion) {
        return PageState::getState(currPageStateAndVersion) == PageState::UNLOCKED;
    }

    bool operator==(const EvictionCandidate& other) const {
        return fileIdx == other.fileIdx && pageIdx == other.pageIdx;
    }

    // Returns false if the candidate was not empty, or if another thread set the value first
    bool set(const EvictionCandidate& newCandidate);

    uint32_t fileIdx = UINT32_MAX;
    common::page_idx_t pageIdx = common::INVALID_PAGE_IDX;
};

// A circular buffer queue storing eviction candidates
// One candidate should be stored for each page currently in memory
class EvictionQueue {
public:
    static constexpr auto EMPTY = EvictionCandidate{UINT32_MAX, common::INVALID_PAGE_IDX};
    explicit EvictionQueue(uint64_t capacity)
        : insertCursor{0}, evictionCursor{0}, size{0}, capacity{capacity},
          data{std::make_unique<std::atomic<EvictionCandidate>[]>(capacity)} {}

    bool insert(uint32_t fileIndex, common::page_idx_t pageIndex);

    // Produces the next non-empty candidate to be tried for eviction
    // Note that it is still possible (though unlikely) for another thread
    // to evict this candidate, so it is not guaranteed to be empty
    // The PageState should be locked, and then the atomic checked against the version used
    // when locking the pagestate to make sure there wasn't a data race
    std::atomic<EvictionCandidate>* next();
    void removeCandidatesForFile(uint32_t fileIndex);
    void clear(std::atomic<EvictionCandidate>& candidate);

    uint64_t getSize() const { return size; }
    uint64_t getCapacity() const { return capacity; }

private:
    std::atomic<uint64_t> insertCursor;
    std::atomic<uint64_t> evictionCursor;
    std::atomic<uint64_t> size;
    const uint64_t capacity;
    std::unique_ptr<std::atomic<EvictionCandidate>[]> data;
};

/**
 * The Buffer Manager (BM) is a centralized manager of database memory resources.
 * It provides two main functionalities:
 * 1) it provides the high-level functionality to pin() and unpin() the pages of the database files
 * used by storage structures, such as the Column, Lists, or HashIndex in the storage layer, and
 * operates via their FileHandle to read/write the page data into/out of one of the frames.
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
 * unique FileHandle, and MM also holds a unique FileHandle, which is backed by an temp in-mem
 * file, for all memory buffer allocations
 *
 * To start a Database, users need to specify the max size of the memory usage (`maxSize`) in BM.
 * If users don't specify the value, the system will set maxSize to available physical mem *
 * DEFAULT_PHY_MEM_SIZE_RATIO_FOR_BM (defined in constants.h).
 * The BM relies on virtual memory regions mapped through `mmap` to anonymous address spaces.
 * 1) For disk pages, BM allocates a virtual memory region of DEFAULT_VM_REGION_MAX_SIZE (defined in
 * constants.h), which is usually much larger than `maxSize`, and is expected to be large enough to
 * contain all disk pages. Each disk page in database files is directly mapped to a unique
 * PAGE_SIZE frame in the region.
 * 2) For each FileHandle backed by a temp in-mem file in MM, BM allocates a virtual memory region
 * of `maxSize` for it. Each memory buffer is mapped to a unique TEMP_PAGE_SIZE frame in that
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
    friend class FileHandle;
    friend class MemoryManager;

public:
    BufferManager(const std::string& databasePath, const std::string& spillToDiskPath,
        uint64_t bufferPoolSize, uint64_t maxDBSize, common::VirtualFileSystem* vfs, bool readOnly);
    ~BufferManager();

    // Currently, these functions are specifically used only for WAL files.
    void removeFilePagesFromFrames(FileHandle& fileHandle);
    void updateFrameIfPageIsInFrameWithoutLock(common::file_idx_t fileIdx, const uint8_t* newPage,
        common::page_idx_t pageIdx);

    // For files that are managed by BM, their FileHandles should be created through this function.
    FileHandle* getFileHandle(const std::string& filePath, uint8_t flags,
        common::VirtualFileSystem* vfs, main::ClientContext* context,
        common::PageSizeClass pageSizeClass = common::REGULAR_PAGE) {
        fileHandles.emplace_back(std::unique_ptr<FileHandle>(new FileHandle(filePath, flags, this,
            fileHandles.size(), pageSizeClass, vfs, context)));
        return fileHandles.back().get();
    }

    uint64_t getUsedMemory() const { return usedMemory; }

    void getSpillerOrSkip(std::function<void(Spiller&)> func) {
        if (spiller) {
            return func(*spiller);
        }
    }

private:
    uint8_t* pin(FileHandle& fileHandle, common::page_idx_t pageIdx,
        PageReadPolicy pageReadPolicy = PageReadPolicy::READ_PAGE);
    void optimisticRead(FileHandle& fileHandle, common::page_idx_t pageIdx,
        const std::function<void(uint8_t*)>& func);
    // The function assumes that the requested page is already pinned.
    void unpin(FileHandle& fileHandle, common::page_idx_t pageIdx);
    uint8_t* getFrame(FileHandle& fileHandle, common::page_idx_t pageIdx) const {
        return vmRegions[fileHandle.getPageSizeClass()]->getFrame(fileHandle.getFrameIdx(pageIdx));
    }
    common::frame_group_idx_t addNewFrameGroup(common::PageSizeClass pageSizeClass) {
        return vmRegions[pageSizeClass]->addNewFrameGroup();
    }
    void removePageFromFrameIfNecessary(FileHandle& fileHandle, common::page_idx_t pageIdx);

    static void verifySizeParams(uint64_t bufferPoolSize, uint64_t maxDBSize);

    // Reclaims used memory until the given size to reserve is available
    // The specified amount of memory will be recorded as being used
    bool reserve(uint64_t sizeToReserve);
    bool claimAFrame(FileHandle& fileHandle, common::page_idx_t pageIdx,
        PageReadPolicy pageReadPolicy);
    // Return number of bytes freed.
    uint64_t tryEvictPage(std::atomic<EvictionCandidate>& candidate);

    void cachePageIntoFrame(FileHandle& fileHandle, common::page_idx_t pageIdx,
        PageReadPolicy pageReadPolicy);
    void removePageFromFrame(FileHandle& fileHandle, common::page_idx_t pageIdx, bool shouldFlush);

    uint64_t freeUsedMemory(uint64_t size);

    void releaseFrameForPage(FileHandle& fileHandle, common::page_idx_t pageIdx) {
        vmRegions[fileHandle.getPageSizeClass()]->releaseFrame(fileHandle.getFrameIdx(pageIdx));
    }

    uint64_t evictPages();

private:
    std::atomic<uint64_t> bufferPoolSize;
    EvictionQueue evictionQueue;
    // Total memory used
    std::atomic<uint64_t> usedMemory;
    // Amount of memory used which cannot be evicted
    std::atomic<uint64_t> nonEvictableMemory;
    // Each VMRegion corresponds to a virtual memory region of a specific page size. Currently, we
    // hold two sizes of REGULAR_PAGE and TEMP_PAGE.
    std::vector<std::unique_ptr<VMRegion>> vmRegions;
    std::vector<std::unique_ptr<FileHandle>> fileHandles;
    std::unique_ptr<Spiller> spiller;
    common::VirtualFileSystem* vfs;
};

} // namespace storage
} // namespace kuzu

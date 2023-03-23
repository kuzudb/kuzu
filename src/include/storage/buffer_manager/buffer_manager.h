#pragma once

#include <vector>

#include "concurrentqueue.h"
#include "storage/buffer_manager/bm_file_handle.h"

namespace spdlog {
class logger;
}

namespace kuzu {
namespace storage {

struct EvictionCandidate {
    bool isEvictable() const {
        return pageState->getEvictionTimestamp() == evictionTimestamp &&
               pageState->getPinCount() == 0;
    }

    BMFileHandle* fileHandle;
    PageState* pageState;
    // The eviction timestamp of the corresponding page state at the time the candidate is enqueued.
    uint64_t evictionTimestamp = -1u;
};

class EvictionQueue {
public:
    EvictionQueue() { queue = std::make_unique<moodycamel::ConcurrentQueue<EvictionCandidate>>(); }

    inline void enqueue(
        BMFileHandle* fileHandle, PageState* frameHandle, uint64_t evictionTimestamp) {
        queue->enqueue(EvictionCandidate{fileHandle, frameHandle, evictionTimestamp});
    }
    inline bool dequeue(EvictionCandidate& candidate) { return queue->try_dequeue(candidate); }
    void removeNonEvictableCandidates();

private:
    std::unique_ptr<moodycamel::ConcurrentQueue<EvictionCandidate>> queue;
};

/**
 * The Buffer Manager (BM) is a centralized manager of database memory resources.
 * It provides two main functionalities:
 * 1) it provides the high-level functionality to pin() and unpin() the pages of the database files
 * used by storage structures, such as the Column, Lists, or HashIndex in the storage layer, and
 * operates via their BMFileHandle to read/write the page data into/out of one of the frames.
 * 2) it supports the MemoryManager (MM) to allocate memory buffers that are not backed by
 * any disk files. Similar to disk files, MM provides BMFileHandles backed by temp in-mem files to
 * the BM to pin/unpin pages. Pin happens when MM tries to allocate a new memory buffer, and unpin
 * happens when MM tries to reclaim a memory buffer.
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
 * Also, BM provides some specialized functionalities:
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
 * The design is inspired by vmcache in the paper "Virtual-Memory Assisted Buffer Management"
 * (https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/_my_direct_uploads/vmcache.pdf).
 * We would also like to thank Fadhil Abubaker for doing the initial research and prototyping of
 * Umbra's design in his CS 848 course project:
 * https://github.com/fabubaker/kuzu/blob/umbra-bm/final_project_report.pdf.
 */

class BufferManager {
public:
    enum class PageReadPolicy : uint8_t { READ_PAGE = 0, DONT_READ_PAGE = 1 };

    explicit BufferManager(uint64_t bufferPoolSize);
    ~BufferManager();

    uint8_t* pin(BMFileHandle& fileHandle, common::page_idx_t pageIdx,
        PageReadPolicy pageReadPolicy = PageReadPolicy::READ_PAGE);

    void setPinnedPageDirty(BMFileHandle& fileHandle, common::page_idx_t pageIdx);

    // The function assumes that the requested page is already pinned.
    void unpin(BMFileHandle& fileHandle, common::page_idx_t pageIdx);

    void removeFilePagesFromFrames(BMFileHandle& fileHandle);
    void flushAllDirtyPagesInFrames(BMFileHandle& fileHandle);
    void updateFrameIfPageIsInFrameWithoutLock(
        BMFileHandle& fileHandle, uint8_t* newPage, common::page_idx_t pageIdx);
    void removePageFromFrameIfNecessary(BMFileHandle& fileHandle, common::page_idx_t pageIdx);

    // For files that are managed by BM, their FileHandles should be created through this function.
    inline std::unique_ptr<BMFileHandle> getBMFileHandle(const std::string& filePath, uint8_t flags,
        BMFileHandle::FileVersionedType fileVersionedType,
        common::PageSizeClass pageSizeClass = common::PAGE_4KB) {
        return std::make_unique<BMFileHandle>(
            filePath, flags, this, pageSizeClass, fileVersionedType);
    }
    inline common::frame_group_idx_t addNewFrameGroup(common::PageSizeClass pageSizeClass) {
        return vmRegions[pageSizeClass]->addNewFrameGroup();
    }

private:
    bool claimAFrame(
        BMFileHandle& fileHandle, common::page_idx_t pageIdx, PageReadPolicy pageReadPolicy);
    // Return number of bytes freed.
    uint64_t tryEvictPage(EvictionCandidate& candidate);

    void cachePageIntoFrame(
        BMFileHandle& fileHandle, common::page_idx_t pageIdx, PageReadPolicy pageReadPolicy);
    void flushIfDirtyWithoutLock(BMFileHandle& fileHandle, common::page_idx_t pageIdx);
    void removePageFromFrame(
        BMFileHandle& fileHandle, common::page_idx_t pageIdx, bool shouldFlush);

    void addToEvictionQueue(BMFileHandle* fileHandle, PageState* pageState);

    inline uint64_t reserveUsedMemory(uint64_t size) { return usedMemory.fetch_add(size); }
    inline uint64_t freeUsedMemory(uint64_t size) { return usedMemory.fetch_sub(size); }

    inline uint8_t* getFrame(BMFileHandle& fileHandle, common::page_idx_t pageIdx) {
        return vmRegions[fileHandle.getPageSizeClass()]->getFrame(fileHandle.getFrameIdx(pageIdx));
    }
    inline void releaseFrameForPage(BMFileHandle& fileHandle, common::page_idx_t pageIdx) {
        vmRegions[fileHandle.getPageSizeClass()]->releaseFrame(fileHandle.getFrameIdx(pageIdx));
    }

private:
    std::shared_ptr<spdlog::logger> logger;
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

#pragma once

#include <mutex>
#include <vector>

#include "common/metric.h"
#include "storage/buffer_manager/buffer_pool.h"
#include "storage/buffer_manager/file_handle.h"

namespace spdlog {
class logger;
}

namespace kuzu {
namespace storage {

/**
 *
 * The Buffer Manager (BM) is the cache of database file pages. It provides the high-level
 * functionality of pin() and unpin() the pages of the database files used by the Column/Lists in
 * the storage layer, and operates via their FileHandles to make the page data available in one of
 * the frames. BM can also be used by any operator or other components of the system to acquire
 * memory blocks and ensure that they do not acquire memory directly from the OS. Depending on how
 * the user of the BM pins and unpins pages, operators can ensure either that the memory blocks they
 * acquire are safely spilled to disk and read back or always kept in memory (see below.)
 *
 * Currently the BM has internal BufferPools to cache pages of 2 size: DEFAULT_PAGE_SIZE and
 * LARGE_PAGE_SIZE, both of which are defined in configs.h. We only have a mechanism to control the
 * memory size of each BufferPool. So when the BM of the system is constructed, one pool of memory
 * is allocated to cache files whose pages are of size DEFAULT_PAGE_SIZE, and a separate pool of
 * memory is allocated to cache files whose pages are of size LARGE_PAGE_SIZE. Ideally we should
 * move towards allocating a single pool of memory from which different size pages are allocated.
 * The Umbra paper (http://db.in.tum.de/~freitag/papers/p29-neumann-cidr20.pdf) describes an
 * mmap-based mechanism to do this (where the responsibility to handle memory fragmentation is
 * delegated to the OS).
 *
 * The BM uses CLOCK replacement policy to evict pages from frames, which is an approximate LRU
 * policy that is based of FIFO-like operations.
 *
 * All access to the BM is through a FileHandle. To use the BM to acquire in-memory blocks users can
 * pin pages, which will then lead the BM to put these pages in memory, and then never unpin them
 * and the BM will never spill those pages to disk. However *make sure to unpin these pages*
 * eventually, otherwise this would be a form of internal memory leak. See InMemOverflowBuffer for
 * an example, where this is done during the deconstruction of the InMemOverflowBuffer.
 *
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
    BufferManager(uint64_t maxSizeForDefaultPagePool, uint64_t maxSizeForLargePagePool);
    ~BufferManager();

    uint8_t* pin(FileHandle& fileHandle, common::page_idx_t pageIdx);

    // The caller should ensure that the given pageIdx is indeed a new page, so should not be read
    // from disk
    uint8_t* pinWithoutReadingFromFile(FileHandle& fileHandle, common::page_idx_t pageIdx);

    inline uint8_t* pinWithoutAcquiringPageLock(
        FileHandle& fileHandle, common::page_idx_t pageIdx, bool doNotReadFromFile) {
        return fileHandle.isLargePaged() ? bufferPoolLargePages->pinWithoutAcquiringPageLock(
                                               fileHandle, pageIdx, doNotReadFromFile) :
                                           bufferPoolDefaultPages->pinWithoutAcquiringPageLock(
                                               fileHandle, pageIdx, doNotReadFromFile);
    }

    void setPinnedPageDirty(FileHandle& fileHandle, common::page_idx_t pageIdx);

    // The function assumes that the requested page is already pinned.
    void unpin(FileHandle& fileHandle, common::page_idx_t pageIdx);
    inline void unpinWithoutAcquiringPageLock(FileHandle& fileHandle, common::page_idx_t pageIdx) {
        return fileHandle.isLargePaged() ?
                   bufferPoolLargePages->unpinWithoutAcquiringPageLock(fileHandle, pageIdx) :
                   bufferPoolDefaultPages->unpinWithoutAcquiringPageLock(fileHandle, pageIdx);
    }

    void resize(uint64_t newSizeForDefaultPagePool, uint64_t newSizeForLargePagePool);

    void removeFilePagesFromFrames(FileHandle& fileHandle);

    void flushAllDirtyPagesInFrames(FileHandle& fileHandle);
    void updateFrameIfPageIsInFrameWithoutPageOrFrameLock(
        FileHandle& fileHandle, uint8_t* newPage, common::page_idx_t pageIdx);

    void removePageFromFrameIfNecessary(FileHandle& fileHandle, common::page_idx_t pageIdx);

private:
    std::shared_ptr<spdlog::logger> logger;
    std::unique_ptr<BufferPool> bufferPoolDefaultPages;
    std::unique_ptr<BufferPool> bufferPoolLargePages;
};

} // namespace storage
} // namespace kuzu

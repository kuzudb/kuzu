#include "storage/buffer_manager/buffer_manager.h"

#include <sys/mman.h>

#include "common/constants.h"
#include "common/exception.h"
#include "spdlog/spdlog.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

// In this function, we try to remove as more as possible candidates that are not evictable from the
// eviction queue until we hit a candidate that is evictable.
// Two kinds of candidates are not evictable: 1) it is currently pinned; 2) it has been recently
// visited.
// To identify those recently accessed candidates, we use the eviction timestamp. If the
// eviction timestamp of a candidate is different from the timestamp in its corresponding pageState,
// it means that the candidate has been recently visited and we should not evict it. The idea is
// that eviction timestamp is a logical per-page timestamp starting from 0, and is incremented each
// time the page is pushed into the eviction queue. For example, the first time p5 is pushed into
// the eviction queue, it will end up with a timestamp 1. When we push a page into the queue, we
// create an EvictionCandidate object for the page. Let's call this object c0 when p5 is first
// pushed. c0 will consist of (ptr to p5, 1), where the latter is the eviction timestamp at the time
// c0 is put into the queue. Suppose p5 is put into the eviction queue again (e.g., because it was
// pinned and unpinned). At this point we create another EvictionCandidate object c1 (ptr to p5, 2)
// where the latter eviction timestamp is now incremented by 1, which makes c1 now not evictable.
// This idea is inspired by DuckDB's queue-based eviction implementation.
void EvictionQueue::removeNonEvictableCandidates() {
    EvictionCandidate evictionCandidate;
    while (true) {
        if (!queue->try_dequeue(evictionCandidate)) {
            break;
        }
        if (evictionCandidate.pageState->getPinCount() != 0 ||
            evictionCandidate.pageState->getEvictionTimestamp() !=
                evictionCandidate.evictionTimestamp) {
            // Remove the candidate from the eviction queue if it is still pinned or if it has
            // been recently visited.
            continue;
        } else {
            queue->enqueue(evictionCandidate);
            break;
        }
    }
}

BufferManager::BufferManager(uint64_t bufferPoolSize)
    : logger{LoggerUtils::getLogger(common::LoggerConstants::LoggerEnum::BUFFER_MANAGER)},
      usedMemory{0}, bufferPoolSize{bufferPoolSize}, numEvictionQueueInsertions{0} {
    logger->info("Done initializing buffer manager.");
    vmRegions.resize(2);
    vmRegions[0] = std::make_unique<VMRegion>(
        PageSizeClass::PAGE_4KB, BufferPoolConstants::DEFAULT_VM_REGION_MAX_SIZE);
    vmRegions[1] = std::make_unique<VMRegion>(PageSizeClass::PAGE_256KB, bufferPoolSize);
    evictionQueue = std::make_unique<EvictionQueue>();
}

BufferManager::~BufferManager() = default;

// Important Note: Pin returns a raw pointer to the frame. This is potentially very dangerous and
// trusts the caller is going to protect this memory space.
// Important responsibilities for the caller are:
// (1) The caller should know the page size and not read/write beyond these boundaries.
// (2) If the given FileHandle is not a (temporary) in-memory file and the caller writes to the
// frame, caller should make sure to call setFrameDirty to let the BufferManager know that the page
// should be flushed to disk if it is evicted.
// (3) If multiple threads are writing to the page, they should coordinate separately because they
// both get access to the same piece of memory.
uint8_t* BufferManager::pin(
    BMFileHandle& fileHandle, common::page_idx_t pageIdx, PageReadPolicy pageReadPolicy) {
    fileHandle.acquirePageLock(pageIdx, LockMode::SPIN);
    auto retVal = pinWithoutAcquiringPageLock(fileHandle, pageIdx, pageReadPolicy);
    fileHandle.releasePageLock(pageIdx);
    return retVal;
}

uint8_t* BufferManager::pinWithoutAcquiringPageLock(
    BMFileHandle& fileHandle, common::page_idx_t pageIdx, PageReadPolicy pageReadPolicy) {
    auto pageState = fileHandle.getPageState(pageIdx);
    if (pageState->isInFrame()) {
        pageState->incrementPinCount();
    } else {
        if (!claimAFrame(fileHandle, pageIdx, pageReadPolicy)) {
            pageState->releaseLock();
            throw BufferManagerException("Failed to claim a frame.");
        }
    }
    return getFrame(fileHandle, pageIdx);
}

// Important Note: The caller should make sure that they have pinned the page before calling this.
void BufferManager::setPinnedPageDirty(BMFileHandle& fileHandle, page_idx_t pageIdx) {
    fileHandle.acquirePageLock(pageIdx, LockMode::SPIN);
    auto pageState = fileHandle.getPageState(pageIdx);
    if (pageState && pageState->getPinCount() >= 1) {
        pageState->setDirty();
        fileHandle.releasePageLock(pageIdx);
    } else {
        fileHandle.releasePageLock(pageIdx);
        throw BufferManagerException("If a page is not in memory or is not pinned, cannot set "
                                     "it to isDirty = true. filePath: " +
                                     fileHandle.getFileInfo()->path +
                                     " pageIdx: " + std::to_string(pageIdx) + ".");
    }
}

void BufferManager::unpin(BMFileHandle& fileHandle, page_idx_t pageIdx) {
    fileHandle.acquirePageLock(pageIdx, LockMode::SPIN);
    unpinWithoutAcquiringPageLock(fileHandle, pageIdx);
    fileHandle.releasePageLock(pageIdx);
}

void BufferManager::unpinWithoutAcquiringPageLock(
    BMFileHandle& fileHandle, common::page_idx_t pageIdx) {
    auto pageState = fileHandle.getPageState(pageIdx);
    // `count` is the value of `pinCount` before sub.
    auto count = pageState->decrementPinCount();
    assert(count >= 1);
    if (count == 1) {
        addToEvictionQueue(&fileHandle, pageState);
    }
}

// This function tries to load the given page into a frame. Due to our design of mmap, each page is
// uniquely mapped to a frame. Thus, claiming a frame is equivalent to ensuring enough physical
// memory is available.
// First, we reserve the memory for the page, which increments the atomic counter `usedMemory`.
// Then, we check if there is enough memory available. If not, we evict pages until we have enough
// or we can find no more pages to be evicted.
// Lastly, we double check if the needed memory is available. If not, we free the memory we reserved
// and return false, otherwise, we load the page to its corresponding frame and return true.
bool BufferManager::claimAFrame(
    BMFileHandle& fileHandle, common::page_idx_t pageIdx, PageReadPolicy pageReadPolicy) {
    page_offset_t pageSizeToClaim = fileHandle.getPageSize();
    // Reserve the memory for the page.
    auto currentUsedMem = reserveUsedMemory(pageSizeToClaim);
    uint64_t claimedMemory = 0;
    // Evict pages if necessary until we have enough memory.
    while ((currentUsedMem + pageSizeToClaim - claimedMemory) > bufferPoolSize.load()) {
        EvictionCandidate evictionCandidate;
        if (!evictionQueue->dequeue(evictionCandidate)) {
            // Cannot find more pages to be evicted. Free the memory we reserved and return false.
            freeUsedMemory(pageSizeToClaim);
            return false;
        }
        if (!evictionCandidate.isEvictable()) {
            continue;
        }
        // We found a page whose pin count can be 0, and potentially haven't been accessed since
        // enqueued. We try to evict the page from its frame by calling `tryEvictPage`, which will
        // check if the page's pin count is actually 0 and the page has not been accessed recently,
        // if so, we evict the page from its frame.
        claimedMemory += tryEvictPage(evictionCandidate);
        currentUsedMem = usedMemory.load();
    }
    if ((currentUsedMem + pageSizeToClaim - claimedMemory) > bufferPoolSize.load()) {
        // Cannot claim the memory needed. Free the memory we reserved and return false.
        freeUsedMemory(pageSizeToClaim);
        return false;
    }
    // Have enough memory available now, load the page into its corresponding frame.
    cachePageIntoFrame(fileHandle, pageIdx, pageReadPolicy);
    freeUsedMemory(claimedMemory);
    return true;
}

void BufferManager::addToEvictionQueue(BMFileHandle* fileHandle, PageState* pageState) {
    auto timestampBeforeEvict = pageState->incrementEvictionTimestamp();
    if (++numEvictionQueueInsertions == BufferPoolConstants::EVICTION_QUEUE_PURGING_INTERVAL) {
        evictionQueue->removeNonEvictableCandidates();
        numEvictionQueueInsertions = 0;
    }
    evictionQueue->enqueue(fileHandle, pageState, timestampBeforeEvict + 1);
}

uint64_t BufferManager::tryEvictPage(EvictionCandidate& candidate) {
    auto& pageState = *candidate.pageState;
    if (!pageState.acquireLock(LockMode::NON_BLOCKING)) {
        return 0;
    }
    // We check pinCount and evictionTimestamp again after acquiring the lock on page currently
    // residing in the frame. At this point in time, no other thread can change the pinCount and the
    // evictionTimestamp.
    if (!candidate.isEvictable()) {
        pageState.releaseLock();
        return 0;
    }
    // Else, flush out the frame into the file page if the frame is dirty. Then remove the page
    // from the frame and release the lock on it.
    flushIfDirtyWithoutLock(*candidate.fileHandle, pageState.getPageIdx());
    auto numBytesFreed = candidate.fileHandle->getPageSize();
    releaseFrameForPage(*candidate.fileHandle, pageState.getPageIdx());
    pageState.resetState();
    pageState.releaseLock();
    return numBytesFreed;
}

void BufferManager::cachePageIntoFrame(
    BMFileHandle& fileHandle, common::page_idx_t pageIdx, PageReadPolicy pageReadPolicy) {
    auto pageState = fileHandle.getPageState(pageIdx);
    pageState->setPinCount(1);
    pageState->clearDirty();
    if (pageReadPolicy == PageReadPolicy::READ_PAGE) {
        FileUtils::readFromFile(fileHandle.getFileInfo(), (void*)getFrame(fileHandle, pageIdx),
            fileHandle.getPageSize(), pageIdx * fileHandle.getPageSize());
    }
    pageState->setInFrame(pageIdx);
}

void BufferManager::flushIfDirtyWithoutLock(BMFileHandle& fileHandle, common::page_idx_t pageIdx) {
    auto pageState = fileHandle.getPageState(pageIdx);
    if (pageState->isDirty()) {
        FileUtils::writeToFile(fileHandle.getFileInfo(), getFrame(fileHandle, pageIdx),
            fileHandle.getPageSize(), pageIdx * fileHandle.getPageSize());
    }
}

void BufferManager::removeFilePagesFromFrames(BMFileHandle& fileHandle) {
    for (auto pageIdx = 0u; pageIdx < fileHandle.getNumPages(); ++pageIdx) {
        removePageFromFrame(fileHandle, pageIdx, false /* do not flush */);
    }
}

void BufferManager::flushAllDirtyPagesInFrames(BMFileHandle& fileHandle) {
    for (auto pageIdx = 0u; pageIdx < fileHandle.getNumPages(); ++pageIdx) {
        removePageFromFrame(fileHandle, pageIdx, true /* flush */);
    }
}

void BufferManager::updateFrameIfPageIsInFrameWithoutLock(
    BMFileHandle& fileHandle, uint8_t* newPage, page_idx_t pageIdx) {
    auto pageState = fileHandle.getPageState(pageIdx);
    if (pageState) {
        memcpy(getFrame(fileHandle, pageIdx), newPage, BufferPoolConstants::PAGE_4KB_SIZE);
    }
}

void BufferManager::removePageFromFrameIfNecessary(BMFileHandle& fileHandle, page_idx_t pageIdx) {
    if (pageIdx >= fileHandle.getNumPages()) {
        return;
    }
    removePageFromFrame(fileHandle, pageIdx, false /* do not flush */);
}

// NOTE: We assume the page is not pinned here.
void BufferManager::removePageFromFrame(
    BMFileHandle& fileHandle, common::page_idx_t pageIdx, bool shouldFlush) {
    fileHandle.acquirePageLock(pageIdx, LockMode::SPIN);
    auto pageState = fileHandle.getPageState(pageIdx);
    if (pageState && pageState->isInFrame()) {
        if (shouldFlush) {
            flushIfDirtyWithoutLock(fileHandle, pageIdx);
        }
        fileHandle.clearPageState(pageIdx);
        releaseFrameForPage(fileHandle, pageIdx);
        freeUsedMemory(fileHandle.getPageSize());
    }
    fileHandle.releasePageLock(pageIdx);
}

} // namespace storage
} // namespace kuzu

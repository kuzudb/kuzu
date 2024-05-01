#include "storage/buffer_manager/buffer_manager.h"

#include <cstring>

#include "common/constants.h"
#include "common/exception/buffer_manager.h"

#if defined(_WIN32)
#include <exception>

#include <eh.h>
#include <windows.h>
#include <winnt.h>
#endif

using namespace kuzu::common;

namespace kuzu {
namespace storage {
// In this function, we try to remove as many as possible candidates that are not evictable from the
// eviction queue until we hit a candidate that is evictable.
// 1) If the candidate page's version has changed, which means the page was pinned and unpinned, we
// remove the candidate from the queue.
// 2) If the candidate page's state is UNLOCKED, and its page version hasn't changed, which means
// the page was optimistically read, we give a second chance to evict the page by marking the page
// as MARKED, and moving the candidate to the back of the queue.
// 3) If the candidate page's state is LOCKED, we remove the candidate from the queue.
void EvictionQueue::removeNonEvictableCandidates() {
    std::shared_lock sLck{mtx};
    while (true) {
        EvictionCandidate evictionCandidate;
        if (!queue->try_dequeue(evictionCandidate)) {
            break;
        }
        auto pageStateAndVersion = evictionCandidate.pageState->getStateAndVersion();
        if (evictionCandidate.isEvictable(pageStateAndVersion)) {
            queue->enqueue(evictionCandidate);
            break;
        } else if (evictionCandidate.isSecondChanceEvictable(pageStateAndVersion)) {
            // The page was optimistically read, mark it as MARKED, and enqueue to be evicted later.
            evictionCandidate.pageState->tryMark(pageStateAndVersion);
            queue->enqueue(evictionCandidate);
            continue;
        } else {
            // Cases to remove the candidate from the queue:
            // 1) The page is currently LOCKED (it is currently pinned), remove the candidate from
            // the queue.
            // 2) The page's version number has changed (it was pinned and unpinned), another
            // candidate exists for this page in the queue. remove the candidate from the queue.
            continue;
        }
    }
}

void EvictionQueue::removeCandidatesForFile(kuzu::storage::BMFileHandle& fileHandle) {
    std::unique_lock xLck{mtx};
    EvictionCandidate candidate;
    uint64_t loopedCandidateIdx = 0;
    auto numCandidatesInQueue = queue->size();
    while (loopedCandidateIdx < numCandidatesInQueue && queue->try_dequeue(candidate)) {
        if (candidate.fileHandle != &fileHandle) {
            queue->enqueue(candidate);
        }
        loopedCandidateIdx++;
    }
}

BufferManager::BufferManager(uint64_t bufferPoolSize, uint64_t maxDBSize)
    : usedMemory{0}, bufferPoolSize{bufferPoolSize}, numEvictionQueueInsertions{0} {
    verifySizeParams(bufferPoolSize, maxDBSize);
    vmRegions.resize(2);
    vmRegions[0] = std::make_unique<VMRegion>(PageSizeClass::PAGE_4KB, maxDBSize);
    vmRegions[1] = std::make_unique<VMRegion>(PageSizeClass::PAGE_256KB, bufferPoolSize);
    evictionQueue = std::make_unique<EvictionQueue>();
}

void BufferManager::verifySizeParams(uint64_t bufferPoolSize, uint64_t maxDBSize) {
    if (bufferPoolSize < BufferPoolConstants::PAGE_4KB_SIZE) {
        throw BufferManagerException("The given buffer pool size should be at least 4KB.");
    }
    if (maxDBSize < BufferPoolConstants::PAGE_4KB_SIZE * StorageConstants::PAGE_GROUP_SIZE) {
        throw BufferManagerException(
            "The given max db size should be at least " +
            std::to_string(BufferPoolConstants::PAGE_4KB_SIZE * StorageConstants::PAGE_GROUP_SIZE) +
            " bytes.");
    }
    if ((maxDBSize & (maxDBSize - 1)) != 0) {
        throw BufferManagerException("The given max db size should be a power of 2.");
    }
}

// Important Note: Pin returns a raw pointer to the frame. This is potentially very dangerous and
// trusts the caller is going to protect this memory space.
// Important responsibilities for the caller are:
// (1) The caller should know the page size and not read/write beyond these boundaries.
// (2) If the given FileHandle is not a (temporary) in-memory file and the caller writes to the
// frame, caller should make sure to call setFrameDirty to let the BufferManager know that the page
// should be flushed to disk if it is evicted.
// (3) If multiple threads are writing to the page, they should coordinate separately because they
// both get access to the same piece of memory.
uint8_t* BufferManager::pin(BMFileHandle& fileHandle, page_idx_t pageIdx,
    PageReadPolicy pageReadPolicy) {
    auto pageState = fileHandle.getPageState(pageIdx);
    while (true) {
        auto currStateAndVersion = pageState->getStateAndVersion();
        switch (PageState::getState(currStateAndVersion)) {
        case PageState::EVICTED: {
            if (pageState->tryLock(currStateAndVersion)) {
                if (!claimAFrame(fileHandle, pageIdx, pageReadPolicy)) {
                    pageState->unlock();
                    throw BufferManagerException("Failed to claim a frame.");
                }
                return getFrame(fileHandle, pageIdx);
            }
        } break;
        case PageState::UNLOCKED:
        case PageState::MARKED: {
            if (pageState->tryLock(currStateAndVersion)) {
                return getFrame(fileHandle, pageIdx);
            }
        } break;
        case PageState::LOCKED: {
            continue;
        }
        default: {
            KU_UNREACHABLE;
        }
        }
    }
}

#if defined(WIN32)
class AccessViolation : public std::exception {
public:
    AccessViolation(const uint8_t* location) : location{location} {}

    const uint8_t* location;
};

class ScopedTranslator {
    const _se_translator_function old;

public:
    ScopedTranslator(_se_translator_function newTranslator)
        : old{_set_se_translator(newTranslator)} {}
    ~ScopedTranslator() { _set_se_translator(old); }
};

void handleAccessViolation(unsigned int exceptionCode, PEXCEPTION_POINTERS exceptionRecord) {
    if (exceptionCode == EXCEPTION_ACCESS_VIOLATION
        // exception was from a read
        && exceptionRecord->ExceptionRecord->ExceptionInformation[0] == 0) [[likely]] {
        throw AccessViolation(
            (const uint8_t*)exceptionRecord->ExceptionRecord->ExceptionInformation[1]);
    }
    // Needs to not be an Exception so that it can't be caught by regular exception handling
    // And is seems like throwing integer error codes is treated similarly to hardware
    // exceptions with /EHa
    throw exceptionCode;
}
#endif

// Returns true if the function completes successfully
inline bool try_func(const std::function<void(uint8_t*)>& func, uint8_t* frame,
    const std::vector<std::unique_ptr<VMRegion>>& vmRegions, common::PageSizeClass pageSizeClass) {
#if defined(_WIN32)
    try {
#endif
        func(frame);
#if defined(_WIN32)
    } catch (AccessViolation& exc) {
        // If we encounter an acess violation within the VM region,
        // the page was decomitted by another thread
        // and is no longer valid memory
        if (vmRegions[pageSizeClass]->contains(exc.location)) {
            return false;
        } else {
            throw EXCEPTION_ACCESS_VIOLATION;
        }
    }
#else
    (void)pageSizeClass;
    (void)vmRegions;
#endif
    return true;
}

void BufferManager::optimisticRead(BMFileHandle& fileHandle, page_idx_t pageIdx,
    const std::function<void(uint8_t*)>& func) {
    auto pageState = fileHandle.getPageState(pageIdx);
#if defined(_WIN32)
    // Change the Structured Exception handling just for the scope of this function
    auto translator = ScopedTranslator(handleAccessViolation);
#endif
    while (true) {
        auto currStateAndVersion = pageState->getStateAndVersion();
        switch (PageState::getState(currStateAndVersion)) {
        case PageState::UNLOCKED: {
            if (!try_func(func, getFrame(fileHandle, pageIdx), vmRegions,
                    fileHandle.getPageSizeClass())) {
                continue;
            }
            if (pageState->getStateAndVersion() == currStateAndVersion) {
                return;
            }
        } break;
        case PageState::MARKED: {
            // If the page is marked, we try to switch to unlocked. If we succeed, we read the page.
            if (pageState->tryClearMark(currStateAndVersion)) {
                if (try_func(func, getFrame(fileHandle, pageIdx), vmRegions,
                        fileHandle.getPageSizeClass())) {
                    return;
                }
            }
        } break;
        case PageState::EVICTED: {
            pin(fileHandle, pageIdx, PageReadPolicy::READ_PAGE);
            unpin(fileHandle, pageIdx);
        } break;
        default: {
            // When locked, continue the spinning.
            continue;
        }
        }
    }
}

void BufferManager::unpin(BMFileHandle& fileHandle, page_idx_t pageIdx) {
    auto pageState = fileHandle.getPageState(pageIdx);
    pageState->unlock();
    addToEvictionQueue(&fileHandle, pageIdx, pageState);
}

// This function tries to load the given page into a frame. Due to our design of mmap, each page is
// uniquely mapped to a frame. Thus, claiming a frame is equivalent to ensuring enough physical
// memory is available.
// First, we reserve the memory for the page, which increments the atomic counter `usedMemory`.
// Then, we check if there is enough memory available. If not, we evict pages until we have enough
// or we can find no more pages to be evicted.
// Lastly, we double check if the needed memory is available. If not, we free the memory we reserved
// and return false, otherwise, we load the page to its corresponding frame and return true.
bool BufferManager::claimAFrame(BMFileHandle& fileHandle, page_idx_t pageIdx,
    PageReadPolicy pageReadPolicy) {
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
        auto pageStateAndVersion = evictionCandidate.pageState->getStateAndVersion();
        if (!evictionCandidate.isEvictable(pageStateAndVersion)) {
            if (evictionCandidate.isSecondChanceEvictable(pageStateAndVersion)) {
                evictionCandidate.pageState->tryMark(pageStateAndVersion);
                evictionQueue->enqueue(evictionCandidate);
            }
            continue;
        }
        // We found a page that potentially hasn't been accessed since enqueued. We try to evict the
        // page from its frame by calling `tryEvictPage`, which will check if the page's version has
        // changed, if not, we evict the page from its frame.
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

void BufferManager::addToEvictionQueue(BMFileHandle* fileHandle, page_idx_t pageIdx,
    PageState* pageState) {
    auto currStateAndVersion = pageState->getStateAndVersion();
    if (++numEvictionQueueInsertions == BufferPoolConstants::EVICTION_QUEUE_PURGING_INTERVAL) {
        evictionQueue->removeNonEvictableCandidates();
        numEvictionQueueInsertions = 0;
    }
    pageState->tryMark(currStateAndVersion);
    evictionQueue->enqueue(fileHandle, pageIdx, pageState,
        PageState::getVersion(currStateAndVersion));
}

uint64_t BufferManager::tryEvictPage(EvictionCandidate& candidate) {
    auto& pageState = *candidate.pageState;
    auto currStateAndVersion = pageState.getStateAndVersion();
    // We check if the page is evictable again. Note that if the page's state or version has
    // changed after the check, `tryLock` will fail, and we will abort the eviction of this page.
    if (!candidate.isEvictable(currStateAndVersion) || !pageState.tryLock(currStateAndVersion)) {
        return 0;
    }
    // At this point, the page is LOCKED. Next, flush out the frame into the file page if the frame
    // is dirty. Finally remove the page from the frame and reset the page to EVICTED.
    flushIfDirtyWithoutLock(*candidate.fileHandle, candidate.pageIdx);
    auto numBytesFreed = candidate.fileHandle->getPageSize();
    releaseFrameForPage(*candidate.fileHandle, candidate.pageIdx);
    pageState.resetToEvicted();
    return numBytesFreed;
}

void BufferManager::cachePageIntoFrame(BMFileHandle& fileHandle, page_idx_t pageIdx,
    PageReadPolicy pageReadPolicy) {
    auto pageState = fileHandle.getPageState(pageIdx);
    pageState->clearDirty();
    if (pageReadPolicy == PageReadPolicy::READ_PAGE) {
        fileHandle.getFileInfo()->readFromFile((void*)getFrame(fileHandle, pageIdx),
            fileHandle.getPageSize(), pageIdx * fileHandle.getPageSize());
    }
}

void BufferManager::flushIfDirtyWithoutLock(BMFileHandle& fileHandle, page_idx_t pageIdx) {
    auto pageState = fileHandle.getPageState(pageIdx);
    if (pageState->isDirty()) {
        fileHandle.getFileInfo()->writeFile(getFrame(fileHandle, pageIdx), fileHandle.getPageSize(),
            pageIdx * fileHandle.getPageSize());
        pageState->clearDirtyWithoutLock();
    }
}

void BufferManager::removeFilePagesFromFrames(BMFileHandle& fileHandle) {
    evictionQueue->removeCandidatesForFile(fileHandle);
    for (auto pageIdx = 0u; pageIdx < fileHandle.getNumPages(); ++pageIdx) {
        removePageFromFrame(fileHandle, pageIdx, false /* do not flush */);
    }
}

void BufferManager::flushAllDirtyPagesInFrames(BMFileHandle& fileHandle) {
    for (auto pageIdx = 0u; pageIdx < fileHandle.getNumPages(); ++pageIdx) {
        flushIfDirtyWithoutLock(fileHandle, pageIdx);
    }
}

void BufferManager::updateFrameIfPageIsInFrameWithoutLock(BMFileHandle& fileHandle,
    uint8_t* newPage, page_idx_t pageIdx) {
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

// NOTE: We assume the page is not pinned (locked) here.
void BufferManager::removePageFromFrame(BMFileHandle& fileHandle, page_idx_t pageIdx,
    bool shouldFlush) {
    auto pageState = fileHandle.getPageState(pageIdx);
    if (PageState::getState(pageState->getStateAndVersion()) == PageState::EVICTED) {
        return;
    }
    pageState->spinLock(pageState->getStateAndVersion());
    if (shouldFlush) {
        flushIfDirtyWithoutLock(fileHandle, pageIdx);
    }
    releaseFrameForPage(fileHandle, pageIdx);
    freeUsedMemory(fileHandle.getPageSize());
    pageState->resetToEvicted();
}

} // namespace storage
} // namespace kuzu

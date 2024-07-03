#include "storage/buffer_manager/buffer_manager.h"

#include <atomic>
#include <cstring>

#include "common/assert.h"
#include "common/constants.h"
#include "common/exception/buffer_manager.h"
#include "common/types/types.h"
#include "storage/buffer_manager/bm_file_handle.h"

#if defined(_WIN32)
#include <exception>

#include <eh.h>
#include <windows.h>
#include <winnt.h>
#endif

using namespace kuzu::common;

namespace kuzu {
namespace storage {

bool EvictionQueue::insert(uint32_t fileIndex, common::page_idx_t pageIndex) {
    EvictionCandidate candidate{fileIndex, pageIndex};
    while (size < capacity) {
        // Weak is fine since spurious failure is acceptable.
        // The slot can always be filled later.
        auto emptyCandidate = EMPTY;
        if (data[insertCursor.fetch_add(1, std::memory_order_relaxed) % capacity]
                .compare_exchange_weak(emptyCandidate, candidate)) {
            size++;
            return true;
        }
    }
    return false;
}

std::atomic<EvictionCandidate>* EvictionQueue::next() {
    std::atomic<EvictionCandidate>* candidate;
    do {
        // Since the buffer pool size is a power of two (as is the page size), (UINT64_MAX + 1) %
        // size == 0, which means no entries will be skipped when the cursor overflows
        candidate = &data[evictionCursor.fetch_add(1, std::memory_order_relaxed) % capacity];
    } while (candidate->load() == EMPTY);
    return candidate;
}

void EvictionQueue::removeCandidatesForFile(uint32_t fileIndex) {
    if (size == 0) {
        return;
    }
    for (uint64_t i = 0; i < capacity; i++) {
        auto candidate = data[i].load();
        if (candidate.fileIdx == fileIndex && data[i].compare_exchange_strong(candidate, EMPTY)) {
            if (size-- == 1) {
                return;
            }
        }
    }
}

void EvictionQueue::clear(std::atomic<EvictionCandidate>& candidate) {
    auto nonEmpty = candidate.load();
    if (nonEmpty != EMPTY && candidate.compare_exchange_strong(nonEmpty, EMPTY)) {
        size--;
        return;
    }
    KU_UNREACHABLE;
}

BufferManager::BufferManager(uint64_t bufferPoolSize, uint64_t maxDBSize)
    : bufferPoolSize{bufferPoolSize},
      evictionQueue{bufferPoolSize / BufferPoolConstants::PAGE_4KB_SIZE},
      usedMemory{evictionQueue.getCapacity() * sizeof(EvictionCandidate)} {
    verifySizeParams(bufferPoolSize, maxDBSize);
    vmRegions.resize(2);
    vmRegions[0] = std::make_unique<VMRegion>(PageSizeClass::PAGE_4KB, maxDBSize);
    vmRegions[1] = std::make_unique<VMRegion>(PageSizeClass::PAGE_256KB, bufferPoolSize);
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
                if (!evictionQueue.insert(fileHandle.getFileIndex(), pageIdx)) {
                    throw BufferManagerException(
                        "Eviction queue is full! This should be impossible.");
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
            // If the page is marked, we try to switch to unlocked.
            pageState->tryClearMark(currStateAndVersion);
            continue;
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
}

// evicts up to 64 pages and returns the space reclaimed
uint64_t BufferManager::evictPages() {
    const size_t BATCH_SIZE = 64;
    std::array<std::atomic<EvictionCandidate>*, BATCH_SIZE> evictionCandidates;
    size_t evictablePages = 0;
    size_t pagesTried = 0;
    uint64_t claimedMemory = 0;

    // Try each page at least twice.
    // E.g. if the vast majority of pages are unmarked and unlocked,
    // the first pass will mark them and the second pass, if insufficient marked pages
    // are found, will evict the first batch.
    auto failureLimit = evictionQueue.getSize() * 2;
    while (evictablePages < BATCH_SIZE && pagesTried < failureLimit) {
        evictionCandidates[evictablePages] = evictionQueue.next();
        pagesTried++;
        auto evictionCandidate = evictionCandidates[evictablePages]->load();
        if (evictionCandidate == EvictionQueue::EMPTY) {
            continue;
        }
        auto* pageState =
            fileHandles[evictionCandidate.fileIdx]->getPageState(evictionCandidate.pageIdx);
        auto pageStateAndVersion = pageState->getStateAndVersion();
        if (!evictionCandidate.isEvictable(pageStateAndVersion)) {
            if (evictionCandidate.isSecondChanceEvictable(pageStateAndVersion)) {
                pageState->tryMark(pageStateAndVersion);
            }
            continue;
        }
        evictablePages++;
    }

    for (size_t i = 0; i < evictablePages; i++) {
        claimedMemory += tryEvictPage(*evictionCandidates[i]);
    }
    return claimedMemory;
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
    if (!reserve(pageSizeToClaim)) {
        return false;
    }
    cachePageIntoFrame(fileHandle, pageIdx, pageReadPolicy);
    return true;
}

bool BufferManager::reserve(uint64_t sizeToReserve) {
    // Reserve the memory for the page.
    auto currentUsedMem = reserveUsedMemory(sizeToReserve);
    uint64_t totalClaimedMemory = 0;
    // Evict pages if necessary until we have enough memory.
    while ((currentUsedMem + sizeToReserve - totalClaimedMemory) > bufferPoolSize.load()) {
        auto memoryClaimed = evictPages();
        if (memoryClaimed == 0) {
            // Cannot find more pages to be evicted. Free the memory we reserved and return false.
            freeUsedMemory(sizeToReserve + totalClaimedMemory);
            return false;
        }
        totalClaimedMemory += memoryClaimed;
        currentUsedMem = usedMemory.load();
    }
    if ((currentUsedMem + sizeToReserve - totalClaimedMemory) > bufferPoolSize.load()) {
        // Cannot claim the memory needed. Free the memory we reserved and return false.
        freeUsedMemory(sizeToReserve + totalClaimedMemory);
        return false;
    }
    // Have enough memory available now
    freeUsedMemory(totalClaimedMemory);
    return true;
}

uint64_t BufferManager::tryEvictPage(std::atomic<EvictionCandidate>& _candidate) {
    auto candidate = _candidate.load();
    // Page must have been evicted by another thread already
    if (candidate.pageIdx == INVALID_PAGE_IDX) {
        return 0;
    }
    auto& pageState = *fileHandles[candidate.fileIdx]->getPageState(candidate.pageIdx);
    auto currStateAndVersion = pageState.getStateAndVersion();
    // We check if the page is evictable again. Note that if the page's state or version has
    // changed after the check, `tryLock` will fail, and we will abort the eviction of this page.
    if (!candidate.isEvictable(currStateAndVersion) || !pageState.tryLock(currStateAndVersion)) {
        return 0;
    }
    // The pageState was locked, but another thread already evicted this candidate and unlocked it
    // before the lock occurred
    if (_candidate.load() != candidate) {
        pageState.unlock();
        return 0;
    }
    // At this point, the page is LOCKED, and we have exclusive access to the eviction candidate.
    // Next, flush out the frame into the file page if the frame
    // is dirty. Finally remove the page from the frame and reset the page to EVICTED.
    auto& fileHandle = *fileHandles[candidate.fileIdx];
    flushIfDirtyWithoutLock(fileHandle, candidate.pageIdx);
    auto numBytesFreed = fileHandle.getPageSize();
    releaseFrameForPage(fileHandle, candidate.pageIdx);
    pageState.resetToEvicted();
    evictionQueue.clear(_candidate);
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
    evictionQueue.removeCandidatesForFile(fileHandle.getFileIndex());
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

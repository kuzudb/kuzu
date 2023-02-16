#include "storage/buffer_manager/buffer_manager.h"

#include "common/constants.h"
#include "common/exception.h"
#include "common/utils.h"
#include "spdlog/spdlog.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

BufferManager::BufferManager(uint64_t maxMemory)
    : logger{LoggerUtils::getLogger(common::LoggerConstants::LoggerEnum::BUFFER_MANAGER)},
      usedMemory{0}, maxMemory{maxMemory}, numEvictionQueueInsertions{0} {
    logger->info("Done Initializing Buffer Manager.");
    for (auto sizeClass : BufferPoolConstants::PAGE_SIZE_CLASSES) {
        bufferCache.push_back(std::make_unique<BufferPool>(maxMemory, sizeClass));
    }
    evictionQueue = std::make_unique<moodycamel::ConcurrentQueue<EvictionCandidate>>();
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
uint8_t* BufferManager::pin(FileHandle& fileHandle, page_idx_t pageIdx) {
    return pin(fileHandle, pageIdx, false /* reading page from file */);
}

// Important Note: This function will pin a page but if the page was not yet in a frame, it will
// not read it from the file. So this can be used if the page is a new page of a file, or a page
// of a temporary file that is being re-used and its contents is not important.
//
// If this is the new page of a file: the caller should call this function immediately after a new
// page is added FileHandle, ensuring that no other thread can try to pin the newly created page
// (with serious side effects). See the detailed explanation in FileHandle::addNewPage() for
// details.
uint8_t* BufferManager::pinWithoutReadingFromFile(
    FileHandle& fileHandle, common::page_idx_t pageIdx) {
    return pin(fileHandle, pageIdx, true /* not reading page from file */);
}

uint8_t* BufferManager::pinWithoutAcquiringPageLock(
    FileHandle& fileHandle, common::page_idx_t pageIdx, bool doNotReadFromFile) {
    auto pageClassIdx = fileHandle.getPageSizeClassIdx();
    auto frameIdx = fileHandle.getFrameIdx(pageIdx);
    if (FileHandle::isAFrame(frameIdx)) {
        auto& frame = bufferCache[pageClassIdx]->frames[frameIdx];
        frame->pinCount.fetch_add(1);
        return frame->buffer;
    } else {
        frameIdx = claimAFrame(fileHandle, pageIdx, doNotReadFromFile);
        fileHandle.swizzle(pageIdx, frameIdx);
        return bufferCache[pageClassIdx]->frames[frameIdx]->buffer;
    }
}

// Important Note: The caller should make sure that they have pinned the page before calling this.
void BufferManager::setPinnedPageDirty(FileHandle& fileHandle, page_idx_t pageIdx) {
    auto pageClassIdx = fileHandle.getPageSizeClassIdx();
    fileHandle.acquirePageLock(pageIdx, true /*block*/);
    auto frameIdx = fileHandle.getFrameIdx(pageIdx);
    if (!FileHandle::isAFrame((frameIdx)) ||
        (bufferCache[pageClassIdx]->frames[frameIdx]->pinCount.load() < 1)) {
        fileHandle.releasePageLock(pageIdx);
        throw BufferManagerException("If a page is not in memory or is not pinned, cannot set "
                                     "it to isDirty = true.filePath: " +
                                     fileHandle.getFileInfo()->path +
                                     " pageIdx: " + std::to_string(pageIdx) + ".");
    }
    bufferCache[pageClassIdx]->frames[frameIdx]->setIsDirty(true /* isDirty */);
    fileHandle.releasePageLock(pageIdx);
}

void BufferManager::unpin(FileHandle& fileHandle, page_idx_t pageIdx) {
    fileHandle.acquirePageLock(pageIdx, true /* block */);
    unpinWithoutAcquiringPageLock(fileHandle, pageIdx);
    fileHandle.releasePageLock(pageIdx);
}

void BufferManager::unpinWithoutAcquiringPageLock(
    FileHandle& fileHandle, common::page_idx_t pageIdx) {
    auto frameIdx = fileHandle.getFrameIdx(pageIdx);
    auto& frame = bufferCache[fileHandle.getPageSizeClassIdx()]->frames[frameIdx];
    // `count` is the value of `pinCount` before sub.
    auto count = frame->pinCount.fetch_sub(1);
    assert(count >= 1);
    if (count == 1) {
        enEvictionQueue(frame.get(), frameIdx);
    }
}

void BufferManager::removeFilePagesFromFrames(FileHandle& fileHandle) {
    for (auto pageIdx = 0u; pageIdx < fileHandle.getNumPages(); ++pageIdx) {
        removePageFromFrame(fileHandle, pageIdx, false /* do not flush */);
    }
}

void BufferManager::flushAllDirtyPagesInFrames(FileHandle& fileHandle) {
    for (auto pageIdx = 0u; pageIdx < fileHandle.getNumPages(); ++pageIdx) {
        removePageFromFrame(fileHandle, pageIdx, true /* flush */);
    }
}

void BufferManager::updateFrameIfPageIsInFrameWithoutPageOrFrameLock(
    FileHandle& fileHandle, uint8_t* newPage, page_idx_t pageIdx) {
    auto frameIdx = fileHandle.getFrameIdx(pageIdx);
    if (FileHandle::isAFrame(frameIdx)) {
        memcpy(bufferCache[fileHandle.getPageSizeClassIdx()]->frames[frameIdx]->buffer, newPage,
            BufferPoolConstants::DEFAULT_PAGE_SIZE);
    }
}

void BufferManager::removePageFromFrameWithoutFlushingIfNecessary(
    FileHandle& fileHandle, page_idx_t pageIdx) {
    if (pageIdx >= fileHandle.getNumPages()) {
        return;
    }
    removePageFromFrame(fileHandle, pageIdx, false /* do not flush */);
}

void BufferManager::resize(uint64_t newSize) {
    for (auto& bufferPool : bufferCache) {
        bufferPool->resize(newSize);
    }
    maxMemory = newSize;
}

uint8_t* BufferManager::pin(
    FileHandle& fileHandle, common::page_idx_t pageIdx, bool doNotReadFromFile) {
    fileHandle.acquirePageLock(pageIdx, true /*block*/);
    auto retVal = pinWithoutAcquiringPageLock(fileHandle, pageIdx, doNotReadFromFile);
    fileHandle.releasePageLock(pageIdx);
    return retVal;
}

// Find pages to evict, so we can claim a frame.
//
// If there is not enough memory available:
//   First, we try to reuse a page with the same size from the eviction queue. Meanwhile, looping
//   through the evict queue to evict pages and free as much memory as possible.
//   - if the queue is empty. we throw an exception that there is no more frames can be claimed.
//   - else, we first see if we can find an evict candidate page to be reused.
//       a) the page was accessed recently. the candidate page object is stale, skip it.
//       b) the page is same size as needed, try to evict it and reuse it.
//       c) the page size is not matched with the needed one. try to evict it. but don't claim it
//       after eviction.
// Now, we have enough memory available, try to find an empty frame from the buffer pool to claim.
common::page_idx_t BufferManager::claimAFrame(
    FileHandle& fileHandle, common::page_idx_t pageIdx, bool doNotReadFromFile) {
    page_offset_t pageSizeToClaim = fileHandle.getPageSize();
    purgeEvictionQueue();
    EvictionCandidate evictionCandidate;
    while (usedMemory.load() + pageSizeToClaim > maxMemory.load()) {
        if (!evictionQueue->try_dequeue(evictionCandidate)) {
            throw BufferManagerException("No more frames can be claimed.");
        }
        if (evictionCandidate.evictTimestamp != evictionCandidate.frame->evictTimestamp) {
            // This page has been recently accessed. Skip it.
            continue;
        }
        if (evictionCandidate.frame->pinCount.load() == 0) {
            if (evictionCandidate.frame->pageSize == pageSizeToClaim) {
                if (tryEvict(evictionCandidate.frame, pageIdx, doNotReadFromFile)) {
                    // Find a page with same size as needed. Evict it and claim it.
                    return evictionCandidate.frameIdx;
                } else {
                    continue;
                }
            } else {
                // Try to evict the page with different size. But don't claim it.
                tryEvict(evictionCandidate.frame, INVALID_PAGE_IDX, true /* doNotReadFromFile */);
                continue;
            }
        }
    }
    auto bufferPool = bufferCache[fileHandle.getPageSizeClassIdx()].get();
    auto numFrames = bufferPool->frames.size();
    auto localClockHand = bufferPool->clockHand.load();
    for (auto i = 0u; i < numFrames; ++i) {
        auto frameIdx = (localClockHand + i) % numFrames;
        auto frame = bufferPool->frames[frameIdx].get();
        auto pinCount = frame->pinCount.load();
        if (pinCount == -1u && fillEmptyFrame(frame, fileHandle, pageIdx, doNotReadFromFile)) {
            bufferPool->moveClockHand(localClockHand + i + 1);
            return frameIdx;
        }
    }
    throw BufferManagerException("Cannot find a frame to claim.");
}

bool BufferManager::fillEmptyFrame(
    Frame* frame, FileHandle& fileHandle, common::page_idx_t pageIdx, bool doNotReadFromFile) {
    if (!frame->acquireFrameLock(false)) {
        return false;
    }
    if (-1u == frame->pinCount.load()) {
        readNewPageIntoFrame(*frame, fileHandle, pageIdx, doNotReadFromFile);
        usedMemory += fileHandle.getPageSize();
        frame->releaseFrameLock();
        return true;
    }
    frame->releaseFrameLock();
    return false;
}

void BufferManager::removePageFromFrame(
    FileHandle& fileHandle, common::page_idx_t pageIdx, bool shouldFlush) {
    fileHandle.acquirePageLock(pageIdx, true /*block*/);
    auto frameIdx = fileHandle.getFrameIdx(pageIdx);
    if (FileHandle::isAFrame(frameIdx)) {
        auto& frame = bufferCache[fileHandle.getPageSizeClassIdx()]->frames[frameIdx];
        frame->acquireFrameLock(true /* block */);
        if (shouldFlush) {
            flushIfDirty(frame.get());
        }
        clearFrameAndUnSwizzleWithoutLock(frame.get(), fileHandle, pageIdx);
        frame->releaseBuffer();
        usedMemory -= frame->pageSize;
        frame->releaseFrameLock();
    }
    fileHandle.releasePageLock(pageIdx);
}

void BufferManager::enEvictionQueue(Frame* frame, common::page_idx_t frameIdx) {
    frame->evictTimestamp++;
    if (++numEvictionQueueInsertions == BufferPoolConstants::PURGE_EVICTION_QUEUE_INTERVAL) {
        purgeEvictionQueue();
        numEvictionQueueInsertions = 0;
    }
    evictionQueue->enqueue(EvictionCandidate{frame, frameIdx, frame->evictTimestamp});
}

bool BufferManager::tryEvict(Frame* frame, common::page_idx_t pageIdx, bool doNotReadFromFile) {
    if (!frame->acquireFrameLock(false)) {
        return false;
    }
    auto pageIdxInFrame = frame->pageIdx.load();
    auto fileHandleInFrame = reinterpret_cast<FileHandle*>(frame->fileHandlePtr.load());
    if (!fileHandleInFrame->acquirePageLock(pageIdxInFrame, false)) {
        frame->releaseFrameLock();
        return false;
    }
    // We check pinCount again after acquiring the lock on page currently residing in the frame. At
    // this point in time, no other thread can change the pinCount.
    if (0u != frame->pinCount.load()) {
        fileHandleInFrame->releasePageLock(pageIdxInFrame);
        frame->releaseFrameLock();
        return false;
    }
    // Else, flush out the frame into the file page if the frame is dirty. Then remove the page from
    // the frame and release the lock on it.
    flushIfDirty(frame);
    clearFrameAndUnSwizzleWithoutLock(frame, *fileHandleInFrame, pageIdxInFrame);
    fileHandleInFrame->releasePageLock(pageIdxInFrame);
    if (pageIdx == INVALID_PAGE_IDX) {
        usedMemory -= frame->pageSize;
        frame->releaseBuffer();
    } else {
        readNewPageIntoFrame(*frame, *fileHandleInFrame, pageIdx, doNotReadFromFile);
    }
    frame->releaseFrameLock();
    return true;
}

void BufferManager::purgeEvictionQueue() {
    EvictionCandidate evictionCandidate;
    while (true) {
        if (!evictionQueue->try_dequeue(evictionCandidate)) {
            break;
        }
        if (evictionCandidate.frame->pinCount.load() != 0 ||
            evictionCandidate.frame->evictTimestamp != evictionCandidate.evictTimestamp) {
            // Remove the frame from the eviction queue if it is still pinned or if it has been
            // recently visited.
            continue;
        } else {
            evictionQueue->enqueue(evictionCandidate);
            break;
        }
    }
}

void BufferManager::readNewPageIntoFrame(
    Frame& frame, FileHandle& fileHandle, common::page_idx_t pageIdx, bool doNotReadFromFile) {
    frame.pinCount.store(1);
    frame.isDirty = false;
    frame.pageIdx.store(pageIdx);
    frame.fileHandlePtr.store(reinterpret_cast<uint64_t>(&fileHandle));
    if (!doNotReadFromFile) {
        fileHandle.readPage(frame.buffer, pageIdx);
    }
}

void BufferManager::clearFrameAndUnSwizzleWithoutLock(
    Frame* frame, FileHandle& fileHandle, common::page_idx_t pageIdx) {
    frame->resetFrameWithoutLock();
    fileHandle.unswizzle(pageIdx);
}

void BufferManager::flushIfDirty(Frame* frame) {
    auto fileHandleInFrame = reinterpret_cast<FileHandle*>(frame->fileHandlePtr.load());
    auto pageIdxInFrame = frame->pageIdx.load();
    if (frame->isDirty) {
        fileHandleInFrame->writePage(frame->buffer, pageIdxInFrame);
    }
}

} // namespace storage
} // namespace kuzu

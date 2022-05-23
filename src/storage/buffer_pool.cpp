
#include "spdlog/spdlog.h"

#include "src/common/include/configs.h"
#include "src/common/include/exception.h"
#include "src/common/include/utils.h"
#include "src/storage/include/buffer_manager.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

Frame::Frame(uint64_t pageSize) : frameLock(ATOMIC_FLAG_INIT) {
    resetFrameWithoutLock();
    buffer = make_unique<uint8_t[]>(pageSize);
}

Frame::~Frame() {
    auto count = pinCount.load();
    if (0 != count && -1u != count) {
        throw BufferManagerException("Deleting buffer that is still pinned. pinCount: " +
                                     to_string(count) + " pageIdx: " + to_string(pageIdx));
    }
}

void Frame::resetFrameWithoutLock() {
    fileHandlePtr = -1u;
    pageIdx = -1u;
    pinCount = -1u;
    recentlyAccessed = false;
    isDirty = false;
}

bool Frame::acquireFrameLock(bool block) {
    if (block) {
        while (frameLock.test_and_set()) // spinning
            ;
        return true;
    }
    return !frameLock.test_and_set();
}

BufferPool::BufferPool(uint64_t pageSize, uint64_t maxSize)
    : logger{LoggerUtils::getOrCreateSpdLogger("buffer_manager")}, pageSize{pageSize}, clockHand{0},
      numFrames((uint32_t)(ceil((double)maxSize / (double)pageSize))) {
    assert(pageSize == DEFAULT_PAGE_SIZE || pageSize == LARGE_PAGE_SIZE);
    for (auto i = 0u; i < numFrames; ++i) {
        bufferCache.emplace_back(make_unique<Frame>(pageSize));
    }
    logger->info("Initializing Buffer Pool.");
    logger->info("BufferPool Size {}B, #{}byte-pages {}.", maxSize, pageSize,
        ceil((double)maxSize / (double)pageSize));
    logger->info("Done Initializing Buffer Pool.");
}

void BufferPool::resize(uint64_t newSize) {
    if ((numFrames * pageSize) > newSize) {
        throw BufferManagerException("Resizing to a smaller Buffer Pool Size is unsupported!");
    }
    auto newNumFrames = (uint32_t)(ceil((double)newSize / (double)pageSize));
    for (auto i = 0u; i < newNumFrames - numFrames; ++i) {
        bufferCache.emplace_back(make_unique<Frame>(pageSize));
    }
    numFrames = newNumFrames;
    logger->info("Resizing buffer pool.");
    logger->info("New buffer pool size {}B, #{}byte-pages {}.", newSize, pageSize, newNumFrames);
    logger->info("Done resizing buffer pool.");
}


uint8_t* BufferPool::pinWithoutReadingFromFile(FileHandle& fileHandle, uint32_t pageIdx) {
    return pin(fileHandle, pageIdx, true /* do not read page from file */);
}

void BufferPool::removeFilePagesFromFrames(FileHandle& fileHandle) {
    for (int pageIdx = 0; pageIdx < fileHandle.numPages; ++pageIdx) {
        removePageFromFrameOrFlushIfDirty(fileHandle, pageIdx, true /* is removing */);
    }
}

void BufferPool::removePageFromFrameOrFlushIfDirty(
    FileHandle& fileHandle, uint64_t pageIdx, bool isRemoving) {
    fileHandle.acquirePageLock(pageIdx, true /*block*/);
    auto frameIdx = fileHandle.getFrameIdx(pageIdx);
    if (FileHandle::isAFrame(frameIdx)) {
        auto& frame = bufferCache[frameIdx];
        frame->acquireFrameLock(true /* block */);
        if (isRemoving) {
            frame->resetFrameWithoutLock();
            fileHandle.unswizzle(pageIdx);
        } else {
            flushIfDirtyWithoutPageOrFrameLock(frame);
        }
        frame->releaseFrameLock();
    }
    fileHandle.releasePageLock(pageIdx);
}

void BufferPool::flushAllDirtyPagesInFrames(FileHandle& fileHandle) {
    for (int pageIdx = 0; pageIdx < fileHandle.numPages; ++pageIdx) {
        removePageFromFrameOrFlushIfDirty(fileHandle, pageIdx, false /* is flushing */);
    }
}

void BufferPool::updateFrameIfPageIsInFrameWithoutPageOrFrameLock(
    FileHandle& fileHandle, uint8_t* newPage, uint64_t pageIdx) {
    auto frameIdx = fileHandle.getFrameIdx(pageIdx);
    if (FileHandle::isAFrame(frameIdx)) {
        memcpy(bufferCache[frameIdx]->buffer.get(), newPage, DEFAULT_PAGE_SIZE);
    }
}

uint8_t* BufferPool::pin(FileHandle& fileHandle, uint32_t pageIdx, bool doNotReadFromFile) {
    fileHandle.acquirePageLock(pageIdx, true /*block*/);
    auto frameIdx = fileHandle.getFrameIdx(pageIdx);
    if (FileHandle::isAFrame(frameIdx)) {
        auto& frame = bufferCache[frameIdx];
        frame->pinCount.fetch_add(1);
        frame->recentlyAccessed = true;
        bmMetrics.numCacheHit += 1;
    } else {
        frameIdx = claimAFrame(fileHandle, pageIdx, doNotReadFromFile);
        fileHandle.swizzle(pageIdx, frameIdx);
        if (!doNotReadFromFile) {
            bmMetrics.numCacheMiss += 1;
        }
    }
    fileHandle.releasePageLock(pageIdx);
    bmMetrics.numPins += 1;
    return bufferCache[frameIdx]->buffer.get();
}

void BufferPool::setPinnedPageDirty(FileHandle& fileHandle, uint32_t pageIdx) {
    fileHandle.acquirePageLock(pageIdx, true /*block*/);
    auto frameIdx = fileHandle.getFrameIdx(pageIdx);
    if (!FileHandle::isAFrame((frameIdx)) || (bufferCache[frameIdx]->pinCount.load() < 1)) {
        fileHandle.releasePageLock(pageIdx);
        throw BufferManagerException("If a page is not in memory or is not pinned, cannot set "
                                     "it to isDirty = true.filePath: " +
                                     fileHandle.fileInfo->path + " pageIdx: " + to_string(pageIdx) +
                                     ".");
    }
    bufferCache[frameIdx]->setIsDirty(true /* isDirty */);
    fileHandle.releasePageLock(pageIdx);
}

uint32_t BufferPool::claimAFrame(FileHandle& fileHandle, uint32_t pageIdx, bool doNotReadFromFile) {
    auto localClockHand = clockHand.load();
    auto startFrame = localClockHand % numFrames;
    for (auto i = 0u; i < 2 * numFrames; ++i) {
        auto frameIdx = (startFrame + i) % numFrames;
        auto pinCount = bufferCache[frameIdx]->pinCount.load();
        if (-1u == pinCount && fillEmptyFrame(frameIdx, fileHandle, pageIdx, doNotReadFromFile)) {
            moveClockHand(localClockHand + i + 1);
            return frameIdx;
        } else if (0u == pinCount && tryEvict(frameIdx, fileHandle, pageIdx, doNotReadFromFile)) {
            moveClockHand(localClockHand + i + 1);
            return frameIdx;
        }
    }
    throw BufferManagerException("Cannot find a frame to evict from.");
}

bool BufferPool::fillEmptyFrame(
    uint32_t frameIdx, FileHandle& fileHandle, uint32_t pageIdx, bool doNotReadFromFile) {
    auto& frame = bufferCache[frameIdx];
    if (!frame->acquireFrameLock(false)) {
        return false;
    }
    if (-1u == frame->pinCount.load()) {
        readNewPageIntoFrame(*frame, fileHandle, pageIdx, doNotReadFromFile);
        frame->releaseFrameLock();
        return true;
    }
    frame->releaseFrameLock();
    return false;
}

bool BufferPool::tryEvict(
    uint32_t frameIdx, FileHandle& fileHandle, uint32_t pageIdx, bool doNotReadFromFile) {
    auto& frame = bufferCache[frameIdx];
    if (frame->recentlyAccessed) {
        frame->recentlyAccessed = false;
        bmMetrics.numRecentlyAccessedWalkover += 1;
        return false;
    }
    if (!frame->acquireFrameLock(false)) {
        return false;
    }
    auto pageIdxInFrame = frame->pageIdx.load();
    auto fileHandleInFrame = reinterpret_cast<FileHandle*>(frame->fileHandlePtr.load());
    if (!fileHandleInFrame->acquirePageLock(pageIdxInFrame, false)) {
        bmMetrics.numEvictFails += 1;
        frame->releaseFrameLock();
        return false;
    }
    // We check pinCount again after acquiring the lock on page currently residing in the frame. At
    // this point in time, no other thread can change the pinCount.
    if (0u != frame->pinCount.load()) {
        bmMetrics.numEvictFails += 1;
        fileHandleInFrame->releasePageLock(pageIdxInFrame);
        frame->releaseFrameLock();
        return false;
    }
    // Else, flush out the frame into the file page if the frame is dirty. Then remove the page from
    // the frame and release the lock on it.
    flushIfDirtyWithoutPageOrFrameLock(frame);
    // Update the frame information and release the lock on frame.
    readNewPageIntoFrame(*frame, fileHandle, pageIdx, doNotReadFromFile);
    frame->releaseFrameLock();
    bmMetrics.numEvicts += 1;
    return true;
}
void BufferPool::flushIfDirtyWithoutPageOrFrameLock(const unique_ptr<Frame>& frame) {
    auto fileHandleInFrame = reinterpret_cast<FileHandle*>(frame->fileHandlePtr.load());
    auto pageIdxInFrame = frame->pageIdx.load();
    if (frame->isDirty) {
        bmMetrics.numDirtyPageWriteIO += 1;
        fileHandleInFrame->writePage(frame->buffer.get(), pageIdxInFrame);
    }
    fileHandleInFrame->unswizzle(pageIdxInFrame);
    fileHandleInFrame->releasePageLock(pageIdxInFrame);
}

void BufferPool::readNewPageIntoFrame(
    Frame& frame, FileHandle& fileHandle, uint32_t pageIdx, bool doNotReadFromFile) {
    frame.pinCount.store(1);
    frame.recentlyAccessed = true;
    frame.isDirty = false;
    frame.pageIdx.store(pageIdx);
    frame.fileHandlePtr.store(reinterpret_cast<uint64_t>(&fileHandle));
    if (!doNotReadFromFile) {
        fileHandle.readPage(frame.buffer.get(), pageIdx);
    }
}

void BufferPool::moveClockHand(uint64_t newClockHand) {
    do {
        auto currClockHand = clockHand.load();
        if (currClockHand > newClockHand) {
            return;
        }
        if (clockHand.compare_exchange_strong(currClockHand, newClockHand, memory_order_seq_cst)) {
            return;
        }
    } while (true);
}

void BufferPool::unpin(FileHandle& fileHandle, uint32_t pageIdx) {
    fileHandle.acquirePageLock(pageIdx, true /*block*/);
    auto& frame = bufferCache[fileHandle.getFrameIdx(pageIdx)];
    // `count` is the value of `pinCount` before sub.
    auto count = frame->pinCount.fetch_sub(1);
    assert(count >= 1);
    fileHandle.releasePageLock(pageIdx);
}
} // namespace storage
} // namespace graphflow

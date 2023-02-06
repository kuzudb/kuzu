#include "storage/buffer_manager/buffer_pool.h"

#include <sys/mman.h>

#include "common/configs.h"
#include "common/exception.h"
#include "common/utils.h"
#include "spdlog/spdlog.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

Frame::Frame(page_offset_t pageSize, std::uint8_t* buffer)
    : frameLock{ATOMIC_FLAG_INIT}, pageSize{pageSize}, buffer{buffer} {
    resetFrameWithoutLock();
}

Frame::~Frame() noexcept(false) {
    auto count = pinCount.load();
    if (0 != count && -1u != count) {
        throw BufferManagerException(
            "Deleting buffer that is still pinned. pinCount: " + std::to_string(count) +
            " pageIdx: " + std::to_string(pageIdx));
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

void Frame::releaseBuffer() {
    int error = madvise(buffer, pageSize, MADV_DONTNEED);
    if (error) {
        throw BufferManagerException("Releasing frame buffer failed with error code " +
                                     std::to_string(error) + ": " +
                                     std::string(std::strerror(errno)));
    }
}

BufferPool::BufferPool(uint64_t pageSize, uint64_t maxSize)
    : logger{LoggerUtils::getOrCreateLogger("buffer_manager")}, pageSize{pageSize}, clockHand{0},
      numFrames((page_idx_t)(ceil((double)maxSize / (double)pageSize))) {
    assert(pageSize == DEFAULT_PAGE_SIZE || pageSize == LARGE_PAGE_SIZE);
    auto mmapRegion = (u_int8_t*)mmap(
        NULL, (numFrames * pageSize), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    for (auto i = 0u; i < numFrames; ++i) {
        auto buffer = mmapRegion + (i * pageSize);
        bufferCache.emplace_back(std::make_unique<Frame>(pageSize, buffer));
    }
    logger->info("Initialize buffer pool with the max size {}B, #{}byte-pages {}.", maxSize,
        pageSize, numFrames);
}

void BufferPool::resize(uint64_t newSize) {
    if ((numFrames * pageSize) > newSize) {
        throw BufferManagerException("Resizing to a smaller Buffer Pool Size is unsupported.");
    }
    auto newNumFrames = (page_idx_t)(ceil((double)newSize / (double)pageSize));
    assert(newNumFrames < UINT32_MAX);
    auto mmapRegion =
        (u_int8_t*)mmap(NULL, newSize, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    for (auto i = 0u; i < newNumFrames - numFrames; ++i) {
        auto buffer = mmapRegion + (i * pageSize);
        bufferCache.emplace_back(std::make_unique<Frame>(pageSize, buffer));
    }
    numFrames = newNumFrames;
    logger->info(
        "Resize buffer pool's max size to {}B, #{}byte-pages {}.", newSize, pageSize, newNumFrames);
}

uint8_t* BufferPool::pin(FileHandle& fileHandle, page_idx_t pageIdx) {
    return pin(fileHandle, pageIdx, false /* read page from file */);
}

uint8_t* BufferPool::pinWithoutReadingFromFile(FileHandle& fileHandle, page_idx_t pageIdx) {
    return pin(fileHandle, pageIdx, true /* do not read page from file */);
}

void BufferPool::removeFilePagesFromFrames(FileHandle& fileHandle) {
    for (auto pageIdx = 0u; pageIdx < fileHandle.numPages; ++pageIdx) {
        removePageFromFrame(fileHandle, pageIdx, false /* do not flush */);
    }
}

void BufferPool::removePageFromFrame(FileHandle& fileHandle, page_idx_t pageIdx, bool shouldFlush) {
    fileHandle.acquirePageLock(pageIdx, true /*block*/);
    auto frameIdx = fileHandle.getFrameIdx(pageIdx);
    if (FileHandle::isAFrame(frameIdx)) {
        auto& frame = bufferCache[frameIdx];
        frame->acquireFrameLock(true /* block */);
        if (shouldFlush) {
            flushIfDirty(frame);
        }
        clearFrameAndUnswizzleWithoutLock(frame, fileHandle, pageIdx);
        frame->releaseBuffer();
        frame->releaseFrameLock();
    }
    fileHandle.releasePageLock(pageIdx);
}

void BufferPool::removePageFromFrameWithoutFlushingIfNecessary(
    FileHandle& fileHandle, page_idx_t pageIdx) {
    if (pageIdx >= fileHandle.numPages) {
        return;
    }
    removePageFromFrame(fileHandle, pageIdx, false /* do not flush */);
}

void BufferPool::flushAllDirtyPagesInFrames(FileHandle& fileHandle) {
    for (auto pageIdx = 0u; pageIdx < fileHandle.numPages; ++pageIdx) {
        removePageFromFrame(fileHandle, pageIdx, true /* flush */);
    }
}

void BufferPool::updateFrameIfPageIsInFrameWithoutPageOrFrameLock(
    FileHandle& fileHandle, uint8_t* newPage, page_idx_t pageIdx) {
    auto frameIdx = fileHandle.getFrameIdx(pageIdx);
    if (FileHandle::isAFrame(frameIdx)) {
        memcpy(bufferCache[frameIdx]->buffer, newPage, DEFAULT_PAGE_SIZE);
    }
}

uint8_t* BufferPool::pin(FileHandle& fileHandle, page_idx_t pageIdx, bool doNotReadFromFile) {
    fileHandle.acquirePageLock(pageIdx, true /*block*/);
    auto retVal = pinWithoutAcquiringPageLock(fileHandle, pageIdx, doNotReadFromFile);
    fileHandle.releasePageLock(pageIdx);
    return retVal;
}

uint8_t* BufferPool::pinWithoutAcquiringPageLock(
    FileHandle& fileHandle, page_idx_t pageIdx, bool doNotReadFromFile) {
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
    bmMetrics.numPins += 1;
    return bufferCache[fileHandle.getFrameIdx(pageIdx)]->buffer;
}

void BufferPool::setPinnedPageDirty(FileHandle& fileHandle, page_idx_t pageIdx) {
    fileHandle.acquirePageLock(pageIdx, true /*block*/);
    auto frameIdx = fileHandle.getFrameIdx(pageIdx);
    if (!FileHandle::isAFrame((frameIdx)) || (bufferCache[frameIdx]->pinCount.load() < 1)) {
        fileHandle.releasePageLock(pageIdx);
        throw BufferManagerException("If a page is not in memory or is not pinned, cannot set "
                                     "it to isDirty = true.filePath: " +
                                     fileHandle.fileInfo->path +
                                     " pageIdx: " + std::to_string(pageIdx) + ".");
    }
    bufferCache[frameIdx]->setIsDirty(true /* isDirty */);
    fileHandle.releasePageLock(pageIdx);
}

page_idx_t BufferPool::claimAFrame(
    FileHandle& fileHandle, page_idx_t pageIdx, bool doNotReadFromFile) {
    auto localClockHand = clockHand.load();
    auto startFrame = localClockHand % numFrames;
    for (auto i = 0u; i < 2 * numFrames; ++i) {
        auto frameIdx = (startFrame + i) % numFrames;
        auto pinCount = bufferCache[frameIdx]->pinCount.load();
        if ((-1u == pinCount && fillEmptyFrame(frameIdx, fileHandle, pageIdx, doNotReadFromFile)) ||
            (0u == pinCount && tryEvict(frameIdx, fileHandle, pageIdx, doNotReadFromFile))) {
            moveClockHand(localClockHand + i + 1);
            return frameIdx;
        }
    }
    throw BufferManagerException("Cannot find a frame to evict from.");
}

bool BufferPool::fillEmptyFrame(
    page_idx_t frameIdx, FileHandle& fileHandle, page_idx_t pageIdx, bool doNotReadFromFile) {
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
    page_idx_t frameIdx, FileHandle& fileHandle, page_idx_t pageIdx, bool doNotReadFromFile) {
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
    flushIfDirty(frame);
    clearFrameAndUnswizzleWithoutLock(frame, *fileHandleInFrame, pageIdxInFrame);
    fileHandleInFrame->releasePageLock(pageIdxInFrame);
    // Update the frame information and release the lock on frame.
    readNewPageIntoFrame(*frame, fileHandle, pageIdx, doNotReadFromFile);
    frame->releaseFrameLock();
    bmMetrics.numEvicts += 1;
    return true;
}

void BufferPool::flushIfDirty(const std::unique_ptr<Frame>& frame) {
    auto fileHandleInFrame = reinterpret_cast<FileHandle*>(frame->fileHandlePtr.load());
    auto pageIdxInFrame = frame->pageIdx.load();
    if (frame->isDirty) {
        bmMetrics.numDirtyPageWriteIO += 1;
        fileHandleInFrame->writePage(frame->buffer, pageIdxInFrame);
    }
}

void BufferPool::clearFrameAndUnswizzleWithoutLock(
    const std::unique_ptr<Frame>& frame, FileHandle& fileHandleInFrame, page_idx_t pageIdxInFrame) {
    frame->resetFrameWithoutLock();
    fileHandleInFrame.unswizzle(pageIdxInFrame);
}

void BufferPool::readNewPageIntoFrame(
    Frame& frame, FileHandle& fileHandle, page_idx_t pageIdx, bool doNotReadFromFile) {
    frame.pinCount.store(1);
    frame.recentlyAccessed = true;
    frame.isDirty = false;
    frame.pageIdx.store(pageIdx);
    frame.fileHandlePtr.store(reinterpret_cast<uint64_t>(&fileHandle));
    if (!doNotReadFromFile) {
        fileHandle.readPage(frame.buffer, pageIdx);
    }
}

void BufferPool::moveClockHand(uint64_t newClockHand) {
    do {
        auto currClockHand = clockHand.load();
        if (currClockHand > newClockHand) {
            return;
        }
        if (clockHand.compare_exchange_strong(
                currClockHand, newClockHand, std::memory_order_seq_cst)) {
            return;
        }
    } while (true);
}

void BufferPool::unpin(FileHandle& fileHandle, page_idx_t pageIdx) {
    fileHandle.acquirePageLock(pageIdx, true /*block*/);
    unpinWithoutAcquiringPageLock(fileHandle, pageIdx);
    fileHandle.releasePageLock(pageIdx);
}

void BufferPool::unpinWithoutAcquiringPageLock(FileHandle& fileHandle, page_idx_t pageIdx) {
    auto& frame = bufferCache[fileHandle.getFrameIdx(pageIdx)];
    // `count` is the value of `pinCount` before sub.
    auto count = frame->pinCount.fetch_sub(1);
    assert(count >= 1);
}

} // namespace storage
} // namespace kuzu

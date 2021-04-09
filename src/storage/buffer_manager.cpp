#include "src/storage/include/buffer_manager.h"

#include "spdlog/sinks/stdout_sinks.h"

#include "src/common/include/configs.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

Frame::Frame()
    : fileHandle{-1u}, pageIdx{-1u}, pinCount{-1u}, recentlyAccessed{false},
      frameLock(ATOMIC_FLAG_INIT) {
    buffer = make_unique<uint8_t[]>(1 << 12);
}

Frame::~Frame() {
    auto count = pinCount.load();
    if (0 != count && -1u != pinCount) {
        throw invalid_argument("Deleting buffer that is still pinned.");
    }
}

bool Frame::acquire(bool block) {
    if (block) {
        while (frameLock.test_and_set()) // spinning
            ;
        return true;
    }
    return !frameLock.test_and_set();
}

BufferManager::BufferManager(uint64_t maxSize)
    : logger{spdlog::stdout_logger_mt("buffer_manager")},
      bufferCache{maxSize / PAGE_SIZE}, clockHand{0}, numFrames{(uint32_t)(maxSize / PAGE_SIZE)} {
    logger->info("Initializing Buffer Manager.");
    logger->info("BufferPool Size {}B, #4KB-pages {}.", maxSize, maxSize / PAGE_SIZE);
    logger->info("Done Initializing Buffer Manager.");
}

const uint8_t* BufferManager::get(FileHandle& fileHandle, uint32_t pageIdx) {
    auto frameIdx = fileHandle.getFrameIdx(pageIdx);
    return bufferCache[frameIdx].buffer.get();
}

const uint8_t* BufferManager::pin(FileHandle& fileHandle, uint32_t pageIdx) {
    fileHandle.acquire(pageIdx, true /*block*/);
    auto frameIdx = fileHandle.getFrameIdx(pageIdx);
    if (isAFrame(frameIdx)) {
        auto& frame = bufferCache[frameIdx];
        frame.pinCount.fetch_add(1);
        frame.recentlyAccessed = true;
    } else {
        frameIdx = claimAFrame(fileHandle, pageIdx);
        fileHandle.swizzle(pageIdx, frameIdx);
    }
    fileHandle.release(pageIdx);
    numPins.fetch_add(1, memory_order_relaxed);
    return bufferCache[frameIdx].buffer.get();
}

uint32_t BufferManager::claimAFrame(FileHandle& fileHandle, uint32_t pageIdx) {
    auto localClockHand = clockHand.load();
    auto startFrame = localClockHand % numFrames;
    for (auto i = 0u; i < 2 * numFrames; ++i) {
        auto frameIdx = (startFrame + i) % numFrames;
        auto pinCount = bufferCache[frameIdx].pinCount.load();
        if (-1u == pinCount && fillEmptyFrame(frameIdx, fileHandle, pageIdx)) {
            moveClockHand(localClockHand + i + 1);
            return frameIdx;
        } else if (0u == pinCount && tryEvict(frameIdx, fileHandle, pageIdx)) {
            moveClockHand(localClockHand + i + 1);
            return frameIdx;
        }
    }
    throw invalid_argument("Cannot find a frame to evict from.");
}

bool BufferManager::fillEmptyFrame(uint32_t frameIdx, FileHandle& fileHandle, uint32_t pageIdx) {
    auto& frame = bufferCache[frameIdx];
    if (!frame.acquire(false)) {
        return false;
    }
    if (-1u == frame.pinCount.load()) {
        readNewPageIntoFrame(frame, fileHandle, pageIdx);
        frame.release();
        return true;
    }
    frame.release();
    return false;
}

bool BufferManager::tryEvict(uint32_t frameIdx, FileHandle& fileHandle, uint32_t pageIdx) {
    auto& frame = bufferCache[frameIdx];
    if (frame.recentlyAccessed) {
        frame.recentlyAccessed = false;
        numRecentlyAccessedWalkover.fetch_add(1, memory_order_relaxed);
        return false;
    }
    if (!frame.acquire(false)) {
        return false;
    }
    auto pageIdxInFrame = frame.pageIdx.load();
    auto fileHandleInFrame = reinterpret_cast<FileHandle*>(frame.fileHandle.load());
    if (!fileHandleInFrame->acquire(pageIdxInFrame, false)) {
        numEvictFails.fetch_add(1, memory_order_relaxed);
        frame.release();
        return false;
    }
    // We check pinCount again after acquiring the lock on page currently residing in the frame. At
    // this point in time, no other thread can change the pinCount.
    if (!(0u == frame.pinCount.load())) {
        numEvictFails.fetch_add(1, memory_order_relaxed);
        fileHandleInFrame->release(pageIdxInFrame);
        frame.release();
        return false;
    }
    // Else, remove the page from the frame and release the lock on it.
    fileHandleInFrame->unswizzle(pageIdxInFrame);
    fileHandleInFrame->release(pageIdxInFrame);
    // Update the frame information and release the lock on frame.
    readNewPageIntoFrame(frame, fileHandle, pageIdx);
    frame.release();
    numEvicts.fetch_add(1, memory_order_relaxed);
    return true;
}

void BufferManager::readNewPageIntoFrame(Frame& frame, FileHandle& fileHandle, uint32_t pageIdx) {
    frame.pinCount.store(1);
    frame.pageIdx.store(pageIdx);
    frame.fileHandle.store(reinterpret_cast<uint64_t>(&fileHandle));
    fileHandle.readPage(frame.buffer.get(), pageIdx);
}

void BufferManager::moveClockHand(uint64_t newClockHand) {
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

void BufferManager::unpin(FileHandle& fileHandle, uint32_t pageIdx) {
    fileHandle.acquire(pageIdx, true /*block*/);
    auto& frame = bufferCache[fileHandle.getFrameIdx(pageIdx)];
    frame.pinCount.fetch_sub(1);
    fileHandle.release(pageIdx);
}

unique_ptr<nlohmann::json> BufferManager::debugInfo() {
    auto numFailsPerEvict = ((double)numEvictFails.load()) / numEvicts.load();
    auto numCacheHits = numPins.load() - numEvicts.load();
    return make_unique<nlohmann::json>(nlohmann::json{{"BufferManager",
        {{"maxPages", numFrames}, {"numPins", numPins.load()}, {"numEvicts", numEvicts.load()},
            {"numEvictFails", numEvictFails.load()}, {"numFailsPerEvict", numFailsPerEvict},
            {"numCacheHits", numCacheHits},
            {"numRecentlyAccessedWalkover", numRecentlyAccessedWalkover.load()}}}});
}

} // namespace storage
} // namespace graphflow

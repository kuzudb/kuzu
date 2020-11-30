#include "src/storage/include/buffer_manager.h"

#include <iostream>

#include "src/common/include/configs.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

Frame::Frame() {
    fileHandle = nullptr;
    pinCount = 0;
    recentlyAccessed = false;
    buffer = make_unique<char[]>(PAGE_SIZE);
}

Frame::~Frame() {
    if (pinCount != 0) {
        throw invalid_argument("Cannot delete a Frame that is pinned.");
    }
}

BufferManager::BufferManager(uint64_t maxSize)
    : clockHand(0), maxPages(maxSize / PAGE_SIZE), usedPages(0) {
    for (auto i = 0u; i < maxPages; i++) {
        bufferPool.push_back(make_unique<Frame>());
    }
}

const char* BufferManager::pin(FileHandle& fileHandle, uint32_t pageIdx) {
    lock_guard lck(bufferManagerMutex);
    Frame* frame;
    auto pageIdxEntry = fileHandle.getPageIdxEntry(pageIdx);
    if (isAFrame(pageIdxEntry)) {
        frame = reinterpret_cast<Frame*>(pageIdxEntry);
        frame->recentlyAccessed = true;
    } else {
        if (usedPages >= maxPages) {
            frame = bufferPool[evict()].get();
        } else {
            frame = bufferPool[usedPages].get();
        }
        readNewPageIntoFrame(frame, fileHandle, pageIdx);
        usedPages++;
    }
    frame->pinCount++;
    return frame->buffer.get();
}

void BufferManager::unpin(FileHandle& fileHandle, uint32_t pageIdx) {
    lock_guard lck(bufferManagerMutex);
    auto pageIdxEntry = fileHandle.getPageIdxEntry(pageIdx);
    if (isAFrame(pageIdxEntry)) {
        reinterpret_cast<Frame*>(pageIdxEntry)->pinCount--;
    }
}

uint64_t BufferManager::evict() {
    auto i = 0u;
    while (0 != bufferPool[clockHand]->pinCount || bufferPool[clockHand]->recentlyAccessed) {
        i++;
        clockHand = (clockHand + 1) % maxPages;
        if (i >= maxPages) {
            throw invalid_argument(
                "Cannot evict a page. All pages are either pinned or recentlyAccessed.");
        }
    }
    auto frame = bufferPool[clockHand].get();
    frame->fileHandle->unswizzle(frame->pageIdx);
    frame = nullptr;
    usedPages--;
    auto toRet = clockHand;
    clockHand = (clockHand + 1) % maxPages;
    return toRet;
}

void BufferManager::readNewPageIntoFrame(Frame* frame, FileHandle& fileHandle, uint32_t pageIdx) {
    fileHandle.readPage(frame->buffer.get(), pageIdx);
    frame->pageIdx = pageIdx;
    frame->fileHandle = &fileHandle;
    fileHandle.swizzle(pageIdx, reinterpret_cast<uint64_t>(frame));
}

} // namespace storage
} // namespace graphflow

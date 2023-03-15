#include "storage/buffer_manager/buffer_manager.h"

#include "common/exception.h"
#include "spdlog/spdlog.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

BufferManager::BufferManager(uint64_t maxSizeForDefaultPagePool, uint64_t maxSizeForLargePagePool)
    : logger{LoggerUtils::getLogger(common::LoggerConstants::LoggerEnum::BUFFER_MANAGER)},
      bufferPoolDefaultPages(std::make_unique<BufferPool>(
          BufferPoolConstants::DEFAULT_PAGE_SIZE, maxSizeForDefaultPagePool)),
      bufferPoolLargePages(std::make_unique<BufferPool>(
          BufferPoolConstants::LARGE_PAGE_SIZE, maxSizeForLargePagePool)) {
    logger->info("Done Initializing Buffer Manager.");
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
uint8_t* BufferManager::pin(BufferManagedFileHandle& fileHandle, page_idx_t pageIdx) {
    return fileHandle.isLargePaged() ? bufferPoolLargePages->pin(fileHandle, pageIdx) :
                                       bufferPoolDefaultPages->pin(fileHandle, pageIdx);
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
    BufferManagedFileHandle& fileHandle, page_idx_t pageIdx) {
    return fileHandle.isLargePaged() ?
               bufferPoolLargePages->pinWithoutReadingFromFile(fileHandle, pageIdx) :
               bufferPoolDefaultPages->pinWithoutReadingFromFile(fileHandle, pageIdx);
}

// Important Note: The caller should make sure that they have pinned the page before calling this.
void BufferManager::setPinnedPageDirty(BufferManagedFileHandle& fileHandle, page_idx_t pageIdx) {
    fileHandle.isLargePaged() ? bufferPoolLargePages->setPinnedPageDirty(fileHandle, pageIdx) :
                                bufferPoolDefaultPages->setPinnedPageDirty(fileHandle, pageIdx);
}

void BufferManager::unpin(BufferManagedFileHandle& fileHandle, page_idx_t pageIdx) {
    return fileHandle.isLargePaged() ? bufferPoolLargePages->unpin(fileHandle, pageIdx) :
                                       bufferPoolDefaultPages->unpin(fileHandle, pageIdx);
}

void BufferManager::removeFilePagesFromFrames(BufferManagedFileHandle& fileHandle) {
    fileHandle.isLargePaged() ? bufferPoolLargePages->removeFilePagesFromFrames(fileHandle) :
                                bufferPoolDefaultPages->removeFilePagesFromFrames(fileHandle);
}

void BufferManager::flushAllDirtyPagesInFrames(BufferManagedFileHandle& fileHandle) {
    fileHandle.isLargePaged() ? bufferPoolLargePages->flushAllDirtyPagesInFrames(fileHandle) :
                                bufferPoolDefaultPages->flushAllDirtyPagesInFrames(fileHandle);
}

void BufferManager::updateFrameIfPageIsInFrameWithoutPageOrFrameLock(
    BufferManagedFileHandle& fileHandle, uint8_t* newPage, page_idx_t pageIdx) {
    fileHandle.isLargePaged() ?
        bufferPoolLargePages->updateFrameIfPageIsInFrameWithoutPageOrFrameLock(
            fileHandle, newPage, pageIdx) :
        bufferPoolDefaultPages->updateFrameIfPageIsInFrameWithoutPageOrFrameLock(
            fileHandle, newPage, pageIdx);
}

void BufferManager::removePageFromFrameIfNecessary(
    BufferManagedFileHandle& fileHandle, page_idx_t pageIdx) {
    fileHandle.isLargePaged() ?
        bufferPoolLargePages->removePageFromFrameWithoutFlushingIfNecessary(fileHandle, pageIdx) :
        bufferPoolDefaultPages->removePageFromFrameWithoutFlushingIfNecessary(fileHandle, pageIdx);
}

} // namespace storage
} // namespace kuzu

#include "src/storage/include/buffer_manager.h"

#include "src/common/include/configs.h"
#include "src/common/include/exception.h"
#include "src/common/include/utils.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

BufferManager::BufferManager(uint64_t maxSizeForDefaultPagePool, uint64_t maxSizeForLargePagePool)
    : logger{LoggerUtils::getOrCreateSpdLogger("buffer_manager")},
      bufferPoolDefaultPages(
          make_unique<BufferPool>(DEFAULT_PAGE_SIZE_LOG_2, maxSizeForDefaultPagePool)),
      bufferPoolLargePages(
          make_unique<BufferPool>(LARGE_PAGE_SIZE_LOG_2, maxSizeForLargePagePool)) {
    logger->info("Done Initializing Buffer Manager.");
}

BufferManager::~BufferManager() {
    spdlog::drop("buffer_manager");
}

void BufferManager::resize(uint64_t newSizeForDefaultPagePool, uint64_t newSizeForLargePagePool) {
    bufferPoolDefaultPages->resize(newSizeForDefaultPagePool);
    bufferPoolLargePages->resize(newSizeForLargePagePool);
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
uint8_t* BufferManager::pin(FileHandle& fileHandle, uint32_t pageIdx) {
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
uint8_t* BufferManager::pinWithoutReadingFromFile(FileHandle& fileHandle, uint32_t pageIdx) {
    return fileHandle.isLargePaged() ?
               bufferPoolLargePages->pinWithoutReadingFromFile(fileHandle, pageIdx) :
               bufferPoolDefaultPages->pinWithoutReadingFromFile(fileHandle, pageIdx);
}

// Important Note: The caller should make sure that they have pinned the page before calling this.
const void BufferManager::setPinnedPageDirty(FileHandle& fileHandle, uint32_t pageIdx) {
    fileHandle.isLargePaged() ? bufferPoolLargePages->setPinnedPageDirty(fileHandle, pageIdx) :
                                bufferPoolDefaultPages->setPinnedPageDirty(fileHandle, pageIdx);
}

void BufferManager::unpin(FileHandle& fileHandle, uint32_t pageIdx) {
    return fileHandle.isLargePaged() ? bufferPoolLargePages->unpin(fileHandle, pageIdx) :
                                       bufferPoolDefaultPages->unpin(fileHandle, pageIdx);
}

unique_ptr<nlohmann::json> BufferManager::debugInfo() {
    // TODO(Semih): I'm being intentionally sloppy here because this code will be removed once we
    // remove gfdb_endpoints, so not spending time to clean this up.
    auto numFailsPerEvictDefaultBufferPool =
        ((double)bufferPoolDefaultPages->numEvictFails.load()) /
        bufferPoolDefaultPages->numEvicts.load();
    auto numCacheHitsDefaultBufferPool =
        bufferPoolDefaultPages->numPins.load() - bufferPoolDefaultPages->numEvicts.load();
    auto numFailsPerEvictLargeBufferPool = ((double)bufferPoolLargePages->numEvictFails.load()) /
                                           bufferPoolLargePages->numEvicts.load();
    auto numCacheHitsLargeBufferPool =
        bufferPoolLargePages->numPins.load() - bufferPoolLargePages->numEvicts.load();
    return make_unique<nlohmann::json>(nlohmann::json{{"BufferManager",
        {{"BufferPoolDefaultPages-maxPages", bufferPoolDefaultPages->numFrames},
            {"BufferPoolDefaultPages-numPins", bufferPoolDefaultPages->numPins.load()},
            {"BufferPoolDefaultPages-numEvicts", bufferPoolDefaultPages->numEvicts.load()},
            {"BufferPoolDefaultPages-numEvictFails", bufferPoolDefaultPages->numEvictFails.load()},
            {"BufferPoolDefaultPages-numFailsPerEvict", numFailsPerEvictDefaultBufferPool},
            {"BufferPoolDefaultPages-numCacheHits", numCacheHitsDefaultBufferPool},
            {"BufferPoolDefaultPages-numRecentlyAccessedWalkover",
                bufferPoolDefaultPages->numRecentlyAccessedWalkover.load()},

            {"maxPages", bufferPoolLargePages->numFrames},
            {"BufferPoolLargePages-numPins", bufferPoolLargePages->numPins.load()},
            {"BufferPoolLargePages-numEvicts", bufferPoolLargePages->numEvicts.load()},
            {"BufferPoolLargePages-numEvictFails", bufferPoolLargePages->numEvictFails.load()},
            {"BufferPoolLargePages-numFailsPerEvict", numFailsPerEvictLargeBufferPool},
            {"BufferPoolLargePages-numCacheHits", numCacheHitsLargeBufferPool},
            {"bufferPoolLargePages-numRecentlyAccessedWalkover",
                bufferPoolLargePages->numRecentlyAccessedWalkover.load()}

        }}});
}

} // namespace storage
} // namespace graphflow

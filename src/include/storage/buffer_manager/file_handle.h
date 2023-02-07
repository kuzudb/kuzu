#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "common/configs.h"
#include "common/exception.h"
#include "common/file_utils.h"
#include "common/types/types.h"

namespace spdlog {
class logger;
}

namespace kuzu {
namespace storage {

using lock_t = std::unique_lock<std::mutex>;

class BufferPool;

// FileHandle is the in-memory representation of the file. It holds an open file descriptor to the
// file as well as the state of each page in the file - if it is pinned in the Buffer Manager. File
// Handle is the bridge between a Column/Lists/Index and the Buffer Manager that abstracts the file
// in which that Column/Lists/Index is stored.
// Warning: FileHandle is *NOT* thread-safe. That is multiple parallel threads can access its
// functions that are used by query processor, such as getFrameIdx, swizzle, unswizzle,
// acquirePageLock, and releasePageLock. Similarly, functions that change the internal data
// structures, such as addPage and resetToZeroPagesAndPageCapacity can be accessed concurrently.
// However, the query processor functions and those that change the internal structures *CANNOT* be
// called concurrently. The caller needs to synchronize these calls.
class FileHandle {
    friend class BufferPool;

public:
    constexpr static uint8_t isLargePagedMask{0b0000'0001}; // represents 1st least sig. bit (LSB)
    constexpr static uint8_t isNewInMemoryTmpFileMask{0b0000'0010}; // represents 2nd LSB
    // createIfNotExistsMask only applies to existing db files; tmp i-memory files are not created
    constexpr static uint8_t createIfNotExistsMask{0b0000'0100}; // represents 3rd LSB

    constexpr static uint8_t O_PERSISTENT_FILE_NO_CREATE{0b0000'0000};
    constexpr static uint8_t O_PERSISTENT_FILE_CREATE_NOT_EXISTS{0b0000'0100};
    constexpr static uint8_t O_IN_MEM_TEMP_FILE{0b0000'0011};

    FileHandle(const std::string& path, uint8_t flags);

    // This function is intended to be used after a fileInfo is created and we want the file
    // to have not pages and page locks. Should be called after ensuring that the buffer manager
    // does not hold any of the pages of the file.
    void resetToZeroPagesAndPageCapacity();

    inline void readPage(uint8_t* frame, common::page_idx_t pageIdx) const {
        common::FileUtils::readFromFile(
            fileInfo.get(), frame, getPageSize(), pageIdx * getPageSize());
    }

    inline void writePage(uint8_t* buffer, common::page_idx_t pageIdx) const {
        common::FileUtils::writeToFile(
            fileInfo.get(), buffer, getPageSize(), pageIdx * getPageSize());
    }

    // Warning: Adding a new page does not write a new page directly to a file. Instead, it only
    // creates a pageLock and a spot in the pageIdxToFrameMap. This may have dangerous consequences
    // if a concurrent thread attempts to read the page. This should be ensured by the caller. To be
    // more specific, let the T_W be the writer thread that is adding the page and T_R a concurrent
    // reader thread. The sequence of calls to make sure the page can be read and written safely is
    // this: (1) T_W calls addNewPage() to create a new page, e.g., with pageIdx: 7 (2) T_W calls
    // BufferManager::pinWithoutReadingFromFile(thisFileHandle, 7 /* pageIdx */, ...):
    // pinWithoutReadingFromFile will not try to read the page from the file, so this is safe.
    //
    // After (1) and (2) T_R can now pin pageIdx 7 by calling BufferManager::pin(...).
    //
    // If this sequence is not enforced, there can be serious side effects, including T_R trying to
    // pin a new page, which will trigger the BM to read offsets in the file on disk, where those
    // offsets do not yet exist, which can cause the OS to error. In general code that uses
    // addNewPage should be written to ensure that once T_W calls addNewPage, it should be followed
    // by a BufferManager::pinNewPage call and the code should ensure that no other thread can try
    // to pin the newly added page in between these calls.
    virtual common::page_idx_t addNewPage();
    virtual void removePageIdxAndTruncateIfNecessary(common::page_idx_t pageIdxToRemove);

    inline bool isLargePaged() const { return flags & isLargePagedMask; }

    inline bool isNewTmpFile() const { return flags & isNewInMemoryTmpFileMask; }
    inline bool createFileIfNotExists() const { return flags & createIfNotExistsMask; }
    inline common::page_idx_t getNumPages() const { return numPages; }
    static inline bool isAFrame(common::page_idx_t mappedFrameIdx) {
        return UINT32_MAX != mappedFrameIdx;
    }
    inline common::FileInfo* getFileInfo() const { return fileInfo.get(); }

    inline common::page_idx_t getFrameIdx(common::page_idx_t pageIdx) {
        return pageIdxToFrameMap[pageIdx]->load();
    }

    bool acquirePageLock(common::page_idx_t pageIdx, bool block);

    void releasePageLock(common::page_idx_t pageIdx) { pageLocks[pageIdx]->clear(); }

    inline uint64_t getPageSize() const {
        return isLargePaged() ? common::LARGE_PAGE_SIZE : common::DEFAULT_PAGE_SIZE;
    }

protected:
    virtual common::page_idx_t addNewPageWithoutLock();

    void removePageIdxAndTruncateIfNecessaryWithoutLock(common::page_idx_t pageIdxToRemove);

    void initPageIdxToFrameMapAndLocks();

    void constructExistingFileHandle(const std::string& path);

    void constructNewFileHandle(const std::string& path);

    bool acquire(common::page_idx_t pageIdx);

    inline void swizzle(common::page_idx_t pageIdx, common::page_idx_t swizzledVal) {
        pageIdxToFrameMap[pageIdx]->store(swizzledVal);
    }

    inline void unswizzle(common::page_idx_t pageIdx) {
        pageIdxToFrameMap[pageIdx]->store(UINT32_MAX);
    }

    inline void addNewPageLockAndFramePtrWithoutLock(common::page_idx_t pageIdx) {
        pageLocks[pageIdx] = std::make_unique<std::atomic_flag>();
        pageIdxToFrameMap[pageIdx] = std::make_unique<std::atomic<common::page_idx_t>>(UINT32_MAX);
    }

protected:
    std::shared_ptr<spdlog::logger> logger;
    uint8_t flags;
    std::unique_ptr<common::FileInfo> fileInfo;
    std::vector<std::unique_ptr<std::atomic_flag>> pageLocks;
    std::vector<std::unique_ptr<std::atomic<common::page_idx_t>>> pageIdxToFrameMap;
    uint32_t numPages;
    // This is the maximum number of pages the filehandle can currently support.
    uint32_t pageCapacity;
    // Intended to be used to coordinate calls to functions that change in the internal data
    // structures of the file handle.
    std::shared_mutex fhSharedMutex;
};

} // namespace storage
} // namespace kuzu

#pragma once

#include <atomic>
#include <memory>
#include <shared_mutex>

#include "src/common/include/configs.h"
#include "src/common/include/exception.h"
#include "src/common/include/file_utils.h"

using namespace std;
using namespace graphflow::common;

namespace spdlog {
class logger;
}

namespace graphflow {
namespace storage {

class BufferPool;

// FileHandle is the in-memory representation of the file. It holds an open file descriptor to the
// file as well as the state of each page in the file - if it is pinned in the Buffer Manager. File
// Handle is the bridge between a Column/Lists/Index and the Buffer Manager that abstracts the file
// in which that Column/Lists/Index is stored.
class FileHandle {
    friend class BufferPool;

public:
    constexpr static uint8_t isLargePagedMask{0b0000'0001}; // represents 1st least sig. bit (LSB)
    constexpr static uint8_t isNewTmpFileMask{0b0000'0010}; // represents 2nd LSB
    constexpr static uint8_t O_DefaultPagedExistingDBFile{0b0000'0000};
    constexpr static uint8_t O_LargePageExistingDBFile{0b0000'0001};
    constexpr static uint8_t O_LargePagedTempFile{0b0000'0011};

    explicit FileHandle(const string& path, uint8_t flags);

    ~FileHandle();

    void readPage(uint8_t* frame, uint64_t pageIdx) const;

    void writePage(uint8_t* buffer, uint64_t pageIdx) const;

    // Warning: Adding a new page does not write a new page directly to a file or if the file is in
    // memory to the in-memory buffer of the file. Instead, it only creates pageLock and a spot in
    // the pageIdxToFrameMap. This may have dangerous consequences if a concurrent thread attempts
    // to read the page. This should be ensured by the caller. To be more specific, let the T_W be
    // the writer thread that is adding the page and T_R a concurrent reader thread. The sequence of
    // calls to make sure the page can be read and written safely is this: (1) T_W calls
    // addNewPage() to create a new page, e.g., with pageIdx: 7 (2) T_W calls
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
    uint32_t addNewPage();

    inline bool isLargePaged() const { return flags & isLargePagedMask; }

    inline bool isNewTmpFile() const { return flags & isNewTmpFileMask; }
    inline uint32_t getNumPages() const { return numPages; }

private:
    void constructExistingFileHandle(const string& path);

    void constructNewFileHandle(const string& path);

    inline uint64_t getFrameIdx(uint32_t pageIdx) {
        shared_lock lock(fhSharedMutex);
        return pageIdxToFrameMap[pageIdx]->load();
    }

    bool acquirePageLock(uint32_t pageIdx, bool block);

    bool acquire(uint32_t pageIdx);

    void releasePageLock(uint32_t pageIdx) {
        shared_lock lock(fhSharedMutex);
        pageLocks[pageIdx]->clear();
    }

    inline void swizzle(uint32_t pageIdx, uint64_t swizzledVal) {
        shared_lock lock(fhSharedMutex);
        pageIdxToFrameMap[pageIdx]->store(swizzledVal);
    }

    inline void unswizzle(uint32_t pageIdx) {
        shared_lock lock(fhSharedMutex);
        pageIdxToFrameMap[pageIdx]->store(UINT64_MAX);
    }

    inline uint64_t getPageSize() const {
        return isLargePaged() ? LARGE_PAGE_SIZE : DEFAULT_PAGE_SIZE;
    }
    inline uint64_t getPageSizeLog2() const {
        return isLargePaged() ? LARGE_PAGE_SIZE_LOG_2 : DEFAULT_PAGE_SIZE_LOG_2;
    }

    inline void addNewPageLockAndFramePtr(uint64_t i);

private:
    shared_ptr<spdlog::logger> logger;
    uint8_t flags;
    unique_ptr<FileInfo> fileInfo;
    unique_ptr<atomic<uint64_t>>* pageIdxToFrameMap;
    unique_ptr<atomic_flag>* pageLocks;
    uint32_t numPages;
    // This is the maximum number of pages the filehandle can currently support.
    uint32_t pageCapacity;
    // Intended to be used as a read/write lock
    shared_mutex fhSharedMutex;
};

} // namespace storage
} // namespace graphflow

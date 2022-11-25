#include "storage/buffer_manager/file_handle.h"

#include "common/file_utils.h"
#include "common/utils.h"
#include "spdlog/spdlog.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

FileHandle::FileHandle(const string& path, uint8_t flags)
    : logger{LoggerUtils::getOrCreateLogger("storage")}, flags(flags) {
    logger->trace("FileHandle: Path {}", path);
    if (!isNewTmpFile()) {
        constructExistingFileHandle(path);
    } else {
        constructNewFileHandle(path);
    }
}

void FileHandle::constructExistingFileHandle(const string& path) {
    int openFlags = O_RDWR | ((createFileIfNotExists()) ? O_CREAT : 0x00000000);
    fileInfo = FileUtils::openFile(path, openFlags);
    auto fileLength = FileUtils::getFileSize(fileInfo->fd);
    numPages = ceil((double)fileLength / (double)getPageSize());
    logger->trace("FileHandle[disk]: Size {}B, #{}B-pages {}", fileLength, getPageSize(), numPages);
    pageCapacity = numPages;
    initPageIdxToFrameMapAndLocks();
}

void FileHandle::constructNewFileHandle(const string& path) {
    fileInfo = make_unique<FileInfo>(path, -1 /* no file descriptor for a new in memory file */);
    numPages = 0;
    pageCapacity = 0;
    initPageIdxToFrameMapAndLocks();
}

void FileHandle::resetToZeroPagesAndPageCapacity() {
    unique_lock xlock(fhSharedMutex);
    numPages = 0;
    pageCapacity = 0;
    FileUtils::truncateFileToEmpty(fileInfo.get());
    initPageIdxToFrameMapAndLocks();
}

void FileHandle::initPageIdxToFrameMapAndLocks() {
    pageIdxToFrameMap.resize(pageCapacity);
    pageLocks.resize(pageCapacity);
    for (auto i = 0ull; i < numPages; i++) {
        addNewPageLockAndFramePtrWithoutLock(i);
    }
}

bool FileHandle::acquirePageLock(page_idx_t pageIdx, bool block) {
    if (block) {
        while (!acquire(pageIdx)) {} // spinning
        return true;
    }
    return acquire(pageIdx);
}

bool FileHandle::acquire(page_idx_t pageIdx) {
    if (pageIdx >= pageLocks.size()) {
        throw RuntimeException("pageIdx is >= pageLocks.size()");
    }
    auto retVal = !pageLocks[pageIdx]->test_and_set(memory_order_acquire);
    return retVal;
}

page_idx_t FileHandle::addNewPage() {
    unique_lock xlock(fhSharedMutex);
    return addNewPageWithoutLock();
}

page_idx_t FileHandle::addNewPageWithoutLock() {
    if (numPages == pageCapacity) {
        pageCapacity =
            max(pageCapacity + 1, (uint32_t)(pageCapacity * StorageConfig::ARRAY_RESIZING_FACTOR));
        pageIdxToFrameMap.resize(pageCapacity);
        pageLocks.resize(pageCapacity);
    }
    addNewPageLockAndFramePtrWithoutLock(numPages);
    return numPages++;
}

void FileHandle::removePageIdxAndTruncateIfNecessary(page_idx_t pageIdxToRemove) {
    unique_lock xlock(fhSharedMutex);
    removePageIdxAndTruncateIfNecessaryWithoutLock(pageIdxToRemove);
}

void FileHandle::removePageIdxAndTruncateIfNecessaryWithoutLock(page_idx_t pageIdxToRemove) {
    if (numPages <= pageIdxToRemove) {
        return;
    }
    for (auto pageIdx = pageIdxToRemove; pageIdx < numPages; ++pageIdx) {
        pageIdxToFrameMap[pageIdx].reset();
        pageLocks[pageIdx].reset();
    }
    numPages = pageIdxToRemove;
}

} // namespace storage
} // namespace kuzu

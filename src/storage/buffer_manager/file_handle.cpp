#include "src/storage/buffer_manager/include/file_handle.h"

#include "spdlog/spdlog.h"

#include "src/common/include/file_utils.h"
#include "src/common/include/utils.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

FileHandle::FileHandle(const string& path, uint8_t flags)
    : logger{LoggerUtils::getOrCreateSpdLogger("storage")}, flags(flags) {
    logger->trace("FileHandle: Path {}", path);
    if (!isNewTmpFile()) {
        constructExistingFileHandle(path);
    } else {
        constructNewFileHandle(path);
    }
}

FileHandle::~FileHandle() {
    if (!isNewTmpFile()) {
        FileUtils::closeFile(fileInfo->fd);
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
    lock_t lock(fhMutex);
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
    auto retVal = !pageLocks[pageIdx]->test_and_set(memory_order_acquire);
    return retVal;
}

page_idx_t FileHandle::addNewPage() {
    lock_t lock(fhMutex);
    return addNewPageWithoutLock();
}

page_idx_t FileHandle::addNewPageWithoutLock() {
    if (numPages == pageCapacity) {
        pageCapacity = max(pageCapacity + 1, (uint32_t)(pageCapacity * 1.2));
        pageIdxToFrameMap.resize(pageCapacity);
        pageLocks.resize(pageCapacity);
    }
    addNewPageLockAndFramePtrWithoutLock(numPages);
    return numPages++;
}

void FileHandle::removePageIdxAndTruncateIfNecessary(page_idx_t pageIdxToRemove) {
    lock_t lock(fhMutex);
    removePageIdxAndTruncateIfNecessaryWithoutLock(pageIdxToRemove);
}

void FileHandle::removePageIdxAndTruncateIfNecessaryWithoutLock(page_idx_t pageIdxToRemove) {
    if (numPages <= pageIdxToRemove) {
        return;
    }
    for (int pageIdx = pageIdxToRemove; pageIdx < numPages; ++pageIdx) {
        pageIdxToFrameMap[pageIdx].reset();
        pageLocks[pageIdx].reset();
    }
    numPages = pageIdxToRemove;
}

} // namespace storage
} // namespace graphflow

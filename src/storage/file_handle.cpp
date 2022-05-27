#include "src/storage/include/file_handle.h"

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

bool FileHandle::acquirePageLock(uint32_t pageIdx, bool block) {
    if (block) {
        while (!acquire(pageIdx)) {} // spinning
        return true;
    }
    return acquire(pageIdx);
}

bool FileHandle::acquire(uint32_t pageIdx) {
    auto retVal = !pageLocks[pageIdx]->test_and_set(memory_order_acquire);
    return retVal;
}

void FileHandle::readPage(uint8_t* frame, uint64_t pageIdx) const {
    FileUtils::readFromFile(fileInfo.get(), frame, getPageSize(), pageIdx * getPageSize());
}

void FileHandle::writePage(uint8_t* buffer, uint64_t pageIdx) const {
    FileUtils::writeToFile(fileInfo.get(), buffer, getPageSize(), pageIdx * getPageSize());
}

void FileHandle::addNewPageLockAndFramePtrWithoutLock(uint64_t pageIdx) {
    pageLocks[pageIdx] = make_unique<atomic_flag>();
    pageIdxToFrameMap[pageIdx] = make_unique<atomic<uint64_t>>(UINT64_MAX);
}

uint32_t FileHandle::addNewPage() {
    lock_t lock(fhMutex);
    return addNewPageWithoutLock();
}

uint32_t FileHandle::addNewPageWithoutLock() {
    if (numPages == pageCapacity) {
        auto oldCapacity = pageCapacity;
        pageCapacity = max(pageCapacity + 1, (uint32_t)(pageCapacity * 1.2));
        vector<unique_ptr<atomic<uint64_t>>> newPageIdxToFrameMap(pageCapacity);
        vector<unique_ptr<atomic_flag>> newPageLocks(pageCapacity);
        for (auto i = 0ull; i < oldCapacity; i++) {
            newPageLocks[i] = move(pageLocks[i]);
            newPageIdxToFrameMap[i] = move(pageIdxToFrameMap[i]);
        }
        pageIdxToFrameMap = move(newPageIdxToFrameMap);
        pageLocks = move(newPageLocks);
    }
    addNewPageLockAndFramePtrWithoutLock(numPages);
    return numPages++;
}

void FileHandle::removePageIdxAndTruncateIfNecessary(uint64_t pageIdxToRemove) {
    lock_t lock(fhMutex);
    removePageIdxAndTruncateIfNecessaryWithoutLock(pageIdxToRemove);
}

void FileHandle::removePageIdxAndTruncateIfNecessaryWithoutLock(uint64_t pageIdxToRemove) {
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

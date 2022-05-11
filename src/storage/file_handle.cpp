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
    delete[] pageLocks;
    delete[] pageIdxToFrameMap;
}

void FileHandle::constructExistingFileHandle(const string& path) {
    int flags = O_RDWR | ((createFileIfNotExists()) ? O_CREAT : 0x00000000);
    fileInfo = FileUtils::openFile(path, flags);
    auto fileLength = FileUtils::getFileSize(fileInfo->fd);
    numPages = fileLength >> getPageSizeLog2();
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
    unique_lock lock(fhSharedMutex);
    numPages = 0;
    pageCapacity = 0;
    initPageIdxToFrameMapAndLocks();
}

void FileHandle::initPageIdxToFrameMapAndLocks() {
    pageIdxToFrameMap = new unique_ptr<atomic<uint64_t>>[pageCapacity];
    pageLocks = new unique_ptr<atomic_flag>[pageCapacity];
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
    shared_lock lock(fhSharedMutex);
    auto retVal = !pageLocks[pageIdx]->test_and_set(memory_order_acquire);
    return retVal;
}

void FileHandle::readPage(uint8_t* frame, uint64_t pageIdx) const {
    FileUtils::readFromFile(fileInfo.get(), frame, getPageSize(), pageIdx << getPageSizeLog2());
}

void FileHandle::writePage(uint8_t* buffer, uint64_t pageIdx) const {
    FileUtils::writeToFile(fileInfo.get(), buffer, getPageSize(), pageIdx << getPageSizeLog2());
}

void FileHandle::addNewPageLockAndFramePtrWithoutLock(uint64_t i) {
    pageLocks[i] = make_unique<atomic_flag>();
    pageIdxToFrameMap[i] = make_unique<atomic<uint64_t>>(UINT64_MAX);
}

uint32_t FileHandle::addNewPage() {
    unique_lock lock(fhSharedMutex);
    if (numPages == pageCapacity) {
        auto oldCapacity = pageCapacity;
        pageCapacity = max(pageCapacity + 1, (uint32_t)(pageCapacity * 1.2));
        unique_ptr<atomic<uint64_t>>* newPageIdxToFrameMap =
            new unique_ptr<atomic<uint64_t>>[pageCapacity];
        unique_ptr<atomic_flag>* newPageLocks = new unique_ptr<atomic_flag>[pageCapacity];
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

} // namespace storage
} // namespace graphflow

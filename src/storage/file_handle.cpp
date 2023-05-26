#include "storage/file_handle.h"

#include "common/utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

FileHandle::FileHandle(const std::string& path, uint8_t flags) : flags{flags} {
    if (!isNewTmpFile()) {
        constructExistingFileHandle(path);
    } else {
        constructNewFileHandle(path);
    }
}

void FileHandle::constructExistingFileHandle(const std::string& path) {
    int openFlags = O_RDWR | ((createFileIfNotExists()) ? O_CREAT : 0x00000000);
    fileInfo = FileUtils::openFile(path, openFlags);
    auto fileLength = fileInfo->getFileSize();
    numPages = ceil((double)fileLength / (double)getPageSize());
    pageCapacity = 0;
    while (pageCapacity < numPages) {
        pageCapacity += StorageConstants::PAGE_GROUP_SIZE;
    }
}

void FileHandle::constructNewFileHandle(const std::string& path) {
#ifdef _WIN32
    fileInfo = make_unique<FileInfo>(
        path, (const void*)nullptr /* no file descriptor for a new in memory file */);
#else
    fileInfo = make_unique<FileInfo>(path, -1 /* no file descriptor for a new in memory file */);
#endif
    numPages = 0;
    pageCapacity = 0;
}

page_idx_t FileHandle::addNewPage() {
    std::unique_lock xlock(fhSharedMutex);
    return addNewPageWithoutLock();
}

page_idx_t FileHandle::addNewPageWithoutLock() {
    if (numPages == pageCapacity) {
        // Add a new page group.
        pageCapacity += StorageConstants::PAGE_GROUP_SIZE;
    }
    return numPages++;
}

} // namespace storage
} // namespace kuzu

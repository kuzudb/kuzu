#include "storage/file_handle.h"

#include <fcntl.h>

#include <cmath>
#include <mutex>

#include "common/file_system/virtual_file_system.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

FileHandle::FileHandle(const std::string& path, uint8_t flags, VirtualFileSystem* vfs,
    main::ClientContext* context)
    : flags{flags} {
    if (!isNewTmpFile()) {
        constructExistingFileHandle(path, vfs, context);
    } else {
        constructNewFileHandle(path);
    }
}

void FileHandle::constructExistingFileHandle(const std::string& path, VirtualFileSystem* vfs,
    main::ClientContext* context) {
    int openFlags;
    if (isReadOnlyFile()) {
        openFlags = O_RDONLY;
    } else {
        openFlags = O_RDWR | ((createFileIfNotExists()) ? O_CREAT : 0x00000000);
    }
    fileInfo = vfs->openFile(path, openFlags, context);
    auto fileLength = fileInfo->getFileSize();
    numPages = ceil((double)fileLength / (double)getPageSize());
    pageCapacity = 0;
    while (pageCapacity < numPages) {
        pageCapacity += StorageConstants::PAGE_GROUP_SIZE;
    }
}

void FileHandle::constructNewFileHandle(const std::string& path) {
    fileInfo = std::make_unique<FileInfo>(path, nullptr);
    numPages = 0;
    pageCapacity = 0;
}

page_idx_t FileHandle::addNewPage() {
    return addNewPages(1 /* numNewPages */);
}

page_idx_t FileHandle::addNewPages(page_idx_t numNewPages) {
    std::unique_lock xlock(fhSharedMutex);
    auto numPagesBeforeChange = numPages;
    for (auto i = 0u; i < numNewPages; i++) {
        addNewPageWithoutLock();
    }
    return numPagesBeforeChange;
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

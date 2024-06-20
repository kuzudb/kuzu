#include "storage/file_handle.h"

#include <cmath>
#include <mutex>

#include "common/file_system/virtual_file_system.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

FileHandle::FileHandle(const std::string& path, uint8_t flags, VirtualFileSystem* vfs,
    main::ClientContext* context)
    : flags{flags} {
    constructFileHandle(path, vfs, context);
}

void FileHandle::constructFileHandle(const std::string& path, VirtualFileSystem* vfs,
    main::ClientContext* context) {
    if (isNewTmpFile()) {
        numPages = 0;
        pageCapacity = 0;
        fileInfo = std::make_unique<FileInfo>(path, nullptr /* fileSystem */);
        return;
    }
    int openFlags;
    if (isReadOnlyFile()) {
        openFlags = FileFlags::READ_ONLY;
    } else {
        openFlags = FileFlags::WRITE | FileFlags::READ_ONLY;
        if (createFileIfNotExists()) {
            openFlags |= FileFlags::CREATE_IF_NOT_EXISTS;
        }
    }
    fileInfo = vfs->openFile(path, openFlags, context);
    auto fileLength = fileInfo->getFileSize();
    numPages = ceil(static_cast<double>(fileLength) / static_cast<double>(getPageSize()));
    pageCapacity = 0;
    while (pageCapacity < numPages) {
        pageCapacity += StorageConstants::PAGE_GROUP_SIZE;
    }
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

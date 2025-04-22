#include "storage/file_handle.h"

#include <cmath>

#include "common/file_system/virtual_file_system.h"
#include "storage/buffer_manager/buffer_manager.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

FileHandle::FileHandle(const std::string& path, uint8_t flags, BufferManager* bm,
    uint32_t fileIndex, PageSizeClass pageSizeClass, VirtualFileSystem* vfs,
    main::ClientContext* context)
    : flags{flags}, fileIndex{fileIndex}, numPages{0}, pageCapacity{0}, bm{bm},
      pageSizeClass{pageSizeClass}, pageStates{0, 0}, frameGroupIdxes{0, 0},
      pageManager(std::make_unique<PageManager>(this)) {
    if (!isNewTmpFile()) {
        constructExistingFileHandle(path, vfs, context);
    } else {
        constructNewFileHandle(path);
    }
    pageStates = ConcurrentVector<PageState, StorageConstants::PAGE_GROUP_SIZE,
        TEMP_PAGE_SIZE / sizeof(void*)>{numPages, pageCapacity};
    frameGroupIdxes = ConcurrentVector<page_group_idx_t>{getNumPageGroups(), getNumPageGroups()};
    for (auto i = 0u; i < frameGroupIdxes.size(); i++) {
        frameGroupIdxes[i] = bm->addNewFrameGroup(pageSizeClass);
    }
}

void FileHandle::constructExistingFileHandle(const std::string& path, VirtualFileSystem* vfs,
    main::ClientContext* context) {
    int openFlags = 0;
    if (isReadOnlyFile()) {
        openFlags = FileFlags::READ_ONLY;
    } else {
        openFlags = FileFlags::WRITE | FileFlags::READ_ONLY |
                    ((createFileIfNotExists()) ? FileFlags::CREATE_IF_NOT_EXISTS : 0x00000000);
    }
    fileInfo = vfs->openFile(path, FileOpenFlags{openFlags}, context);
    const auto fileLength = fileInfo->getFileSize();
    numPages = ceil(static_cast<double>(fileLength) / static_cast<double>(getPageSize()));
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
    while (!fhSharedMutex.try_lock()) {}
    const auto numPagesBeforeChange = numPages;
    for (auto i = 0u; i < numNewPages; i++) {
        addNewPageWithoutLock();
    }
    fhSharedMutex.unlock();
    return numPagesBeforeChange;
}

page_idx_t FileHandle::addNewPageWithoutLock() {
    if (numPages == pageCapacity) {
        addNewPageGroupWithoutLock();
    }
    pageStates[numPages].resetToEvicted();
    const auto pageIdx = numPages++;
    if (isInMemoryMode()) {
        bm->pin(*this, pageIdx, PageReadPolicy::DONT_READ_PAGE);
    }
    return pageIdx;
}

void FileHandle::addNewPageGroupWithoutLock() {
    pageCapacity += StorageConstants::PAGE_GROUP_SIZE;
    pageStates.resize(pageCapacity);
    frameGroupIdxes.push_back(bm->addNewFrameGroup(pageSizeClass));
}

uint8_t* FileHandle::pinPage(page_idx_t pageIdx, PageReadPolicy readPolicy) {
    if (isInMemoryMode()) {
        // Already pinned.
        return bm->getFrame(*this, pageIdx);
    }
    return bm->pin(*this, pageIdx, readPolicy);
}

void FileHandle::optimisticReadPage(page_idx_t pageIdx,
    const std::function<void(uint8_t*)>& readOp) {
    if (isInMemoryMode()) {
        KU_ASSERT(
            PageState::getState(getPageState(pageIdx)->getStateAndVersion()) == PageState::LOCKED);
        const auto frame = bm->getFrame(*this, pageIdx);
        readOp(frame);
    } else {
        bm->optimisticRead(*this, pageIdx, readOp);
    }
}

void FileHandle::unpinPage(page_idx_t pageIdx) {
    bm->unpin(*this, pageIdx);
}

void FileHandle::resetToZeroPagesAndPageCapacity() {
    removePageIdxAndTruncateIfNecessary(0 /* pageIdx */);
    if (isInMemoryMode()) {
        for (auto i = 0u; i < numPages; i++) {
            bm->unpin(*this, i);
        }
    } else {
        fileInfo->truncate(0 /* size */);
    }
}

uint8_t* FileHandle::getFrame(page_idx_t pageIdx) {
    KU_ASSERT(pageIdx < numPages);
    return bm->getFrame(*this, pageIdx);
}

void FileHandle::removePageIdxAndTruncateIfNecessary(page_idx_t pageIdx) {
    std::unique_lock xLck{fhSharedMutex};
    if (numPages <= pageIdx) {
        return;
    }
    numPages = pageIdx;
    pageStates.resize(numPages);
    const auto numPageGroups = getNumPageGroups();
    if (numPageGroups == frameGroupIdxes.size()) {
        return;
    }
    KU_ASSERT(numPageGroups < frameGroupIdxes.size());
    frameGroupIdxes.resize(numPageGroups);
    pageCapacity = numPageGroups * StorageConstants::PAGE_GROUP_SIZE;
}

void FileHandle::removePageFromFrameIfNecessary(page_idx_t pageIdx) {
    bm->removePageFromFrameIfNecessary(*this, pageIdx);
}

void FileHandle::flushAllDirtyPagesInFrames() {
    for (auto pageIdx = 0u; pageIdx < numPages; ++pageIdx) {
        flushPageIfDirtyWithoutLock(pageIdx);
    }
}

void FileHandle::flushPageIfDirtyWithoutLock(page_idx_t pageIdx) {
    auto pageState = getPageState(pageIdx);
    if (!isInMemoryMode() && pageState->isDirty()) {
        fileInfo->writeFile(getFrame(pageIdx), getPageSize(), pageIdx * getPageSize());
        pageState->clearDirtyWithoutLock();
    }
}

} // namespace storage
} // namespace kuzu

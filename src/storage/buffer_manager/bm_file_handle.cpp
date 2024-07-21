#include "storage/buffer_manager/bm_file_handle.h"

#include "storage/buffer_manager/buffer_manager.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

BMFileHandle::BMFileHandle(const std::string& path, uint8_t flags, BufferManager* bm,
    uint32_t fileIndex, PageSizeClass pageSizeClass, VirtualFileSystem* vfs,
    main::ClientContext* context)
    : FileHandle{path, flags, vfs, context}, bm{bm}, pageSizeClass{pageSizeClass},
      pageStates{numPages, pageCapacity}, frameGroupIdxes{getNumPageGroups(), getNumPageGroups()},
      fileIndex{fileIndex} {
    for (auto i = 0u; i < frameGroupIdxes.size(); i++) {
        frameGroupIdxes[i] = bm->addNewFrameGroup(pageSizeClass);
    }
}

page_idx_t BMFileHandle::addNewPageWithoutLock() {
    if (numPages == pageCapacity) {
        addNewPageGroupWithoutLock();
    }
    pageStates[numPages].resetToEvicted();
    return numPages++;
}

void BMFileHandle::addNewPageGroupWithoutLock() {
    pageCapacity += StorageConstants::PAGE_GROUP_SIZE;
    pageStates.resize(pageCapacity);
    frameGroupIdxes.push_back(bm->addNewFrameGroup(pageSizeClass));
}

void BMFileHandle::resetToZeroPagesAndPageCapacity() {
    removePageIdxAndTruncateIfNecessary(0 /* pageIdx */);
    fileInfo->truncate(0 /* size */);
}

void BMFileHandle::removePageIdxAndTruncateIfNecessary(page_idx_t pageIdx) {
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

} // namespace storage
} // namespace kuzu

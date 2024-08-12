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

uint8_t* BMFileHandle::pinPage(page_idx_t pageIdx, PageReadPolicy readPolicy) {
    if (isInMemoryMode()) {
        // Already pinned.
        return bm->getFrame(*this, pageIdx);
    }
    return bm->pin(*this, pageIdx, readPolicy);
}

void BMFileHandle::optimisticReadPage(page_idx_t pageIdx,
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

void BMFileHandle::unpinPage(page_idx_t pageIdx) {
    bm->unpin(*this, pageIdx);
}

page_idx_t BMFileHandle::addNewPageWithoutLock() {
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

void BMFileHandle::addNewPageGroupWithoutLock() {
    pageCapacity += StorageConstants::PAGE_GROUP_SIZE;
    pageStates.resize(pageCapacity);
    frameGroupIdxes.push_back(bm->addNewFrameGroup(pageSizeClass));
}

void BMFileHandle::resetToZeroPagesAndPageCapacity() {
    removePageIdxAndTruncateIfNecessary(0 /* pageIdx */);
    if (isInMemoryMode()) {
        for (auto i = 0u; i < numPages; i++) {
            bm->unpin(*this, i);
        }
    } else {
        fileInfo->truncate(0 /* size */);
    }
}

uint8_t* BMFileHandle::getFrame(page_idx_t pageIdx) {
    KU_ASSERT(pageIdx < numPages);
    return bm->getFrame(*this, pageIdx);
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

void BMFileHandle::removePageFromFrameIfNecessary(page_idx_t pageIdx) {
    bm->removePageFromFrameIfNecessary(*this, pageIdx);
}

void BMFileHandle::flushAllDirtyPagesInFrames() {
    for (auto pageIdx = 0u; pageIdx < numPages; ++pageIdx) {
        flushPageIfDirtyWithoutLock(pageIdx);
    }
}

void BMFileHandle::flushPageIfDirtyWithoutLock(page_idx_t pageIdx) {
    auto pageState = getPageState(pageIdx);
    if (!isInMemoryMode() && pageState->isDirty()) {
        fileInfo->writeFile(getFrame(pageIdx), getPageSize(), pageIdx * getPageSize());
        pageState->clearDirtyWithoutLock();
    }
}

} // namespace storage
} // namespace kuzu

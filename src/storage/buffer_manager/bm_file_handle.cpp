#include "storage/buffer_manager/bm_file_handle.h"

#include "storage/buffer_manager/buffer_manager.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

void PageState::setInFrame(common::page_idx_t pageIdx_) {
    pageIdx = 0;
    pageIdx = pageIdx_;
    pageIdx |= IS_IN_FRAME_MASK;
}

bool PageState::acquireLock(LockMode lockMode) {
    if (lockMode == LockMode::SPIN) {
        while (lock.test_and_set()) // spinning
            ;
        return true;
    }
    return !lock.test_and_set();
}

void PageState::resetState() {
    pageIdx = 0;
    pinCount = 0;
    evictionTimestamp = 0;
}

BMFileHandle::BMFileHandle(const std::string& path, uint8_t flags, BufferManager* bm,
    common::PageSizeClass pageSizeClass, FileVersionedType fileVersionedType)
    : FileHandle{path, flags}, bm{bm}, pageSizeClass{pageSizeClass}, fileVersionedType{
                                                                         fileVersionedType} {
    initPageStatesAndGroups();
}

void BMFileHandle::initPageStatesAndGroups() {
    pageStates.resize(pageCapacity);
    for (auto i = 0ull; i < numPages; i++) {
        pageStates[i] = std::make_unique<PageState>();
    }
    auto numPageGroups = getNumPageGroups();
    frameGroupIdxes.resize(numPageGroups);
    pageGroupLocks.resize(numPageGroups);
    pageVersions.resize(numPageGroups);
    for (auto i = 0u; i < numPageGroups; i++) {
        frameGroupIdxes[i] = bm->addNewFrameGroup(pageSizeClass);
        pageGroupLocks[i] = std::make_unique<std::atomic_flag>();
    }
}

common::page_idx_t BMFileHandle::addNewPageWithoutLock() {
    if (numPages == pageCapacity) {
        addNewPageGroupWithoutLock();
    }
    pageStates[numPages] = std::make_unique<PageState>();
    return numPages++;
}

void BMFileHandle::addNewPageGroupWithoutLock() {
    pageCapacity += StorageConstants::PAGE_GROUP_SIZE;
    pageStates.resize(pageCapacity);
    frameGroupIdxes.push_back(bm->addNewFrameGroup(pageSizeClass));
    if (fileVersionedType == FileVersionedType::VERSIONED_FILE) {
        pageGroupLocks.push_back(std::make_unique<std::atomic_flag>());
        pageVersions.emplace_back();
    }
}

void BMFileHandle::createPageVersionGroupIfNecessary(page_idx_t pageIdx) {
    assert(fileVersionedType == FileVersionedType::VERSIONED_FILE);
    // Note that we do not have to acquire an xlock here because this function assumes that prior to
    // calling this function,  pageVersion and pageGroupLocks have been resized correctly.
    std::shared_lock sLck{fhSharedMutex};
    assert(pageIdx < numPages);
    // Although getPageElementCursorForPos is written to get offsets of elements
    // in pages, it simply can be used to find the group/chunk and offset/pos in group/chunk for
    // any chunked data structure.
    auto pageGroupIdxAndPosInGroup =
        PageUtils::getPageElementCursorForPos(pageIdx, StorageConstants::PAGE_GROUP_SIZE);
    // If we have not created a vector of locks and pageVersion array for each page in this
    // group, first create them.
    bool pageGroupLockAcquired = false;
    while (!pageGroupLockAcquired) { // spinning wait
        pageGroupLockAcquired = !pageGroupLocks[pageGroupIdxAndPosInGroup.pageIdx]->test_and_set();
    }
    if (pageVersions[pageGroupIdxAndPosInGroup.pageIdx].empty()) {
        pageVersions[pageGroupIdxAndPosInGroup.pageIdx].resize(
            StorageConstants::PAGE_GROUP_SIZE, UINT32_MAX);
    }
    pageGroupLocks[pageGroupIdxAndPosInGroup.pageIdx]->clear();
}

void BMFileHandle::resetToZeroPagesAndPageCapacity() {
    std::unique_lock xlock(fhSharedMutex);
    numPages = 0;
    pageCapacity = 0;
    FileUtils::truncateFileToEmpty(fileInfo.get());
    initPageStatesAndGroups();
}

void BMFileHandle::removePageIdxAndTruncateIfNecessary(common::page_idx_t pageIdx) {
    std::unique_lock xLck{fhSharedMutex};
    removePageIdxAndTruncateIfNecessaryWithoutLock(pageIdx);
}

void BMFileHandle::removePageIdxAndTruncateIfNecessaryWithoutLock(
    common::page_idx_t pageIdxToRemove) {
    if (numPages <= pageIdxToRemove) {
        return;
    }
    for (auto pageIdx = pageIdxToRemove; pageIdx < numPages; ++pageIdx) {
        pageStates[pageIdx].reset();
    }
    numPages = pageIdxToRemove;
    auto numPageGroups = getNumPageGroups();
    if (numPageGroups == frameGroupIdxes.size()) {
        return;
    }
    assert(numPageGroups < frameGroupIdxes.size());
    frameGroupIdxes.resize(numPageGroups);
    pageGroupLocks.resize(numPageGroups);
    pageVersions.resize(numPageGroups);
}

// This function assumes that the caller has already acquired the lock for originalPageIdx.
bool BMFileHandle::hasWALPageVersionNoPageLock(common::page_idx_t originalPageIdx) {
    assert(fileVersionedType == FileVersionedType::VERSIONED_FILE);
    auto pageGroupIdxAndPosInGroup =
        PageUtils::getPageElementCursorForPos(originalPageIdx, StorageConstants::PAGE_GROUP_SIZE);
    // TODO: and Warning: This can be a major performance bottleneck, where we need to acquire a
    // shared lock for the entire file handle to ensure that another thread cannot be updating
    // pageVersions. We can fix this by instead making pageVersions a linkedList of vectors for
    // each page group (so not a vector of vector, which can be dynamically allocated) and
    // having a lock on each page group.
    std::shared_lock sLck{fhSharedMutex};
    // There is an updated wal page if the PageVersionAndLockInfo for the page group exists
    // and the page version for the page is not null (which is UINT32_MAX).
    auto retVal =
        !pageVersions[pageGroupIdxAndPosInGroup.pageIdx].empty() &&
        (pageVersions[pageGroupIdxAndPosInGroup.pageIdx][pageGroupIdxAndPosInGroup.elemPosInPage] !=
            UINT32_MAX);
    return retVal;
}

void BMFileHandle::clearWALPageVersionIfNecessary(common::page_idx_t pageIdx) {
    {
        std::shared_lock sLck{fhSharedMutex};
        if (numPages <= pageIdx) {
            return;
        }
    }
    createPageVersionGroupIfNecessary(pageIdx);
    setWALPageVersionNoLock(pageIdx, UINT32_MAX);
    // TODO(Guodong): Why do we release lock here? Need to understand how the lock was acquired.
    releasePageLock(pageIdx);
}

// This function assumes that the caller has already acquired the lock for originalPageIdx.
common::page_idx_t BMFileHandle::getWALPageVersionNoPageLock(common::page_idx_t originalPageIdx) {
    assert(fileVersionedType == FileVersionedType::VERSIONED_FILE);
    // See the comment about a shared lock in hasWALPageVersionNoPageLock
    std::shared_lock sLck{fhSharedMutex};
    auto pageGroupIdxAndPosInGroup =
        PageUtils::getPageElementCursorForPos(originalPageIdx, StorageConstants::PAGE_GROUP_SIZE);
    return pageVersions[pageGroupIdxAndPosInGroup.pageIdx][pageGroupIdxAndPosInGroup.elemPosInPage];
}

void BMFileHandle::setWALPageVersion(
    common::page_idx_t originalPageIdx, common::page_idx_t pageIdxInWAL) {
    assert(fileVersionedType == FileVersionedType::VERSIONED_FILE);
    std::shared_lock sLck{fhSharedMutex};
    setWALPageVersionNoLock(originalPageIdx, pageIdxInWAL);
}

void BMFileHandle::setWALPageVersionNoLock(
    common::page_idx_t originalPageIdx, common::page_idx_t pageIdxInWAL) {
    auto pageGroupIdxAndPosInGroup =
        PageUtils::getPageElementCursorForPos(originalPageIdx, StorageConstants::PAGE_GROUP_SIZE);
    pageVersions[pageGroupIdxAndPosInGroup.pageIdx][pageGroupIdxAndPosInGroup.elemPosInPage] =
        pageIdxInWAL;
}

} // namespace storage
} // namespace kuzu

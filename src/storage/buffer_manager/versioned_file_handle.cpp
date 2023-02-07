#include "storage/buffer_manager/versioned_file_handle.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

VersionedFileHandle::VersionedFileHandle(
    const StorageStructureIDAndFName& storageStructureIDAndFName, uint8_t flags)
    : FileHandle(storageStructureIDAndFName.fName, flags),
      storageStructureIDForWALRecord{storageStructureIDAndFName.storageStructureID} {
    resizePageGroupLocksAndPageVersionsToNumPageGroupsWithoutLock();
}

void VersionedFileHandle::createPageVersionGroupIfNecessary(page_idx_t pageIdx) {
    // Note that we do not have to acquire an xlock here because this function assumes that prior to
    // calling this function,  pageVersion and pageGroupLocks have been resized correctly.
    std::shared_lock slock(fhSharedMutex);
    assert(pageIdx < numPages);
    // Although getPageElementCursorForPos is written to get offsets of elements
    // in pages, it simply can be used to find the group/chunk and offset/pos in group/chunk for
    // any chunked data structure.
    auto pageGroupIdxAndPosInGroup =
        PageUtils::getPageElementCursorForPos(pageIdx, MULTI_VERSION_FILE_PAGE_GROUP_SIZE);
    // If we have not created a vector of locks and pageVersion array for each page in this
    // group, first create them.
    bool pageGroupLockAcquired = false;
    while (!pageGroupLockAcquired) { // spinning wait
        pageGroupLockAcquired = !pageGroupLocks[pageGroupIdxAndPosInGroup.pageIdx]->test_and_set();
    }
    if (pageVersions[pageGroupIdxAndPosInGroup.pageIdx].empty()) {
        pageVersions[pageGroupIdxAndPosInGroup.pageIdx].resize(
            MULTI_VERSION_FILE_PAGE_GROUP_SIZE, UINT32_MAX);
    }
    pageGroupLocks[pageGroupIdxAndPosInGroup.pageIdx]->clear();
}

void VersionedFileHandle::setWALPageVersion(page_idx_t originalPageIdx, page_idx_t pageIdxInWAL) {
    std::shared_lock slock(fhSharedMutex);
    setWALPageVersionNoLock(originalPageIdx, pageIdxInWAL);
}

void VersionedFileHandle::setWALPageVersionNoLock(
    page_idx_t originalPageIdx, page_idx_t pageIdxInWAL) {
    auto pageGroupIdxAndPosInGroup =
        PageUtils::getPageElementCursorForPos(originalPageIdx, MULTI_VERSION_FILE_PAGE_GROUP_SIZE);
    pageVersions[pageGroupIdxAndPosInGroup.pageIdx][pageGroupIdxAndPosInGroup.elemPosInPage] =
        pageIdxInWAL;
}

void VersionedFileHandle::clearWALPageVersionIfNecessary(page_idx_t pageIdx) {
    std::shared_lock slock(fhSharedMutex);
    if (numPages <= pageIdx) {
        return;
    }
    slock.unlock();
    createPageVersionGroupIfNecessary(pageIdx);
    setWALPageVersionNoLock(pageIdx, UINT32_MAX);
    releasePageLock(pageIdx);
}

page_idx_t VersionedFileHandle::addNewPage() {
    std::unique_lock xlock(fhSharedMutex);
    return addNewPageWithoutLock();
}

page_idx_t VersionedFileHandle::addNewPageWithoutLock() {
    auto retVal = FileHandle::addNewPageWithoutLock();
    resizePageGroupLocksAndPageVersionsToNumPageGroupsWithoutLock();
    return retVal;
}

void VersionedFileHandle::removePageIdxAndTruncateIfNecessary(page_idx_t pageIdxToRemove) {
    std::unique_lock xlock(fhSharedMutex);
    FileHandle::removePageIdxAndTruncateIfNecessaryWithoutLock(pageIdxToRemove);
    resizePageGroupLocksAndPageVersionsToNumPageGroupsWithoutLock();
}

void VersionedFileHandle::resizePageGroupLocksAndPageVersionsToNumPageGroupsWithoutLock() {
    auto numPageGroups = getNumPageGroups();
    if (pageGroupLocks.size() == numPageGroups) {
        return;
    } else if (pageGroupLocks.size() < numPageGroups) {
        for (auto i = pageGroupLocks.size(); i < getNumPageGroups(); ++i) {
            pageGroupLocks.push_back(std::make_unique<std::atomic_flag>());
        }
    } else {
        pageGroupLocks.resize(numPageGroups);
    }
    pageVersions.resize(getNumPageGroups());
}

} // namespace storage
} // namespace kuzu

#include "src/storage/buffer_manager/include/versioned_file_handle.h"

namespace graphflow {
namespace storage {

VersionedFileHandle::VersionedFileHandle(
    const StorageStructureIDAndFName& storageStructureIDAndFName, uint8_t flags)
    : FileHandle(storageStructureIDAndFName.fName, flags),
      storageStructureIDForWALRecord{storageStructureIDAndFName.storageStructureID} {
    resizePageGroupLocksAndPageVersionsToNumPageGroupsWithoutLock();
}

void VersionedFileHandle::createPageVersionGroupIfNecessary(page_idx_t pageIdx) {
    lock_t lock(fhMutex);
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

void VersionedFileHandle::setUpdatedWALPageVersionNoLock(
    page_idx_t originalPageIdx, page_idx_t pageIdxInWAL) {
    auto pageGroupIdxAndPosInGroup =
        PageUtils::getPageElementCursorForPos(originalPageIdx, MULTI_VERSION_FILE_PAGE_GROUP_SIZE);
    pageVersions[pageGroupIdxAndPosInGroup.pageIdx][pageGroupIdxAndPosInGroup.posInPage] =
        pageIdxInWAL;
}

void VersionedFileHandle::clearUpdatedWALPageVersion(page_idx_t pageIdx) {
    createPageVersionGroupIfNecessary(pageIdx);
    setUpdatedWALPageVersionNoLock(pageIdx, UINT32_MAX);
    releasePageLock(pageIdx);
}

page_idx_t VersionedFileHandle::addNewPage() {
    lock_t lock(fhMutex);
    auto retVal = FileHandle::addNewPageWithoutLock();
    resizePageGroupLocksAndPageVersionsToNumPageGroupsWithoutLock();
    return retVal;
}

void VersionedFileHandle::removePageIdxAndTruncateIfNecessary(page_idx_t pageIdxToRemove) {
    lock_t lock(fhMutex);
    FileHandle::removePageIdxAndTruncateIfNecessaryWithoutLock(pageIdxToRemove);
    resizePageGroupLocksAndPageVersionsToNumPageGroupsWithoutLock();
}

void VersionedFileHandle::resizePageGroupLocksAndPageVersionsToNumPageGroupsWithoutLock() {
    auto numPageGroups = getNumPageGroups();
    if (pageGroupLocks.size() == numPageGroups) {
        return;
    } else if (pageGroupLocks.size() < numPageGroups) {
        for (auto i = pageGroupLocks.size(); i < getNumPageGroups(); ++i) {
            pageGroupLocks.push_back(make_unique<atomic_flag>());
        }
    } else {
        pageGroupLocks.resize(numPageGroups);
    }
    pageVersions.resize(getNumPageGroups());
}

} // namespace storage
} // namespace graphflow

#include "src/storage/include/versioned_file_handle.h"

namespace graphflow {
namespace storage {

VersionedFileHandle::VersionedFileHandle(
    const StorageStructureIDAndFName& storageStructureIDAndFName, uint8_t flags)
    : FileHandle(storageStructureIDAndFName.fName, flags),
      storageStructureIDForWALRecord{storageStructureIDAndFName.storageStructureID} {
    resizePageGroupLocksAndPageVersionsToNumPageGroupsWithoutLock();
}

void VersionedFileHandle::createPageVersionGroupIfNecessary(uint64_t pageIdx) {
    // Although getPageElementCursorForOffset is written to get offsets of elements
    // in pages, it simply can be used to find the group/chunk and offset/pos in group/chunk for
    // any chunked data structure.
    auto pageGroupIdxAndPosInGroup =
        PageUtils::getPageElementCursorForOffset(pageIdx, MULTI_VERSION_FILE_PAGE_GROUP_SIZE);
    // If we have not created a vector of locks and pageVersion array for each page in this
    // group, first create them.
    bool pageGroupLockAcquired = false;
    while (!pageGroupLockAcquired) { // spinning wait
        pageGroupLockAcquired = !pageGroupLocks[pageGroupIdxAndPosInGroup.pageIdx]->test_and_set();
    }
    if (pageVersions[pageGroupIdxAndPosInGroup.pageIdx].empty()) {
        pageVersions[pageGroupIdxAndPosInGroup.pageIdx].resize(
            MULTI_VERSION_FILE_PAGE_GROUP_SIZE, UINT64_MAX);
    }
    pageGroupLocks[pageGroupIdxAndPosInGroup.pageIdx]->clear();
}

void VersionedFileHandle::setUpdatedWALPageVersionNoLock(
    uint64_t originalPageIdx, uint64_t pageIdxInWAL) {
    auto pageGroupIdxAndPosInGroup = PageUtils::getPageElementCursorForOffset(
        originalPageIdx, MULTI_VERSION_FILE_PAGE_GROUP_SIZE);
    pageVersions[pageGroupIdxAndPosInGroup.pageIdx][pageGroupIdxAndPosInGroup.pos] = pageIdxInWAL;
}

void VersionedFileHandle::clearUpdatedWALPageVersion(uint64_t pageIdx) {
    createPageVersionGroupIfNecessary(pageIdx);
    setUpdatedWALPageVersionNoLock(pageIdx, UINT64_MAX);
    releasePageLock(pageIdx);
}

uint32_t VersionedFileHandle::addNewPage() {
    lock_t lock(fhMutex);
    auto retVal = FileHandle::addNewPageWithoutLock();
    resizePageGroupLocksAndPageVersionsToNumPageGroupsWithoutLock();
    return retVal;
}

void VersionedFileHandle::removePageIdxAndTruncateIfNecessary(uint64_t pageIdxToRemove) {
    lock_t lock(fhMutex);
    FileHandle::removePageIdxAndTruncateIfNecessaryWithoutLock(pageIdxToRemove);
    resizePageGroupLocksAndPageVersionsToNumPageGroupsWithoutLock();
}

} // namespace storage
} // namespace graphflow

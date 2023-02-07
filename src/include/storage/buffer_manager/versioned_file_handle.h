#pragma once

#include "common/configs.h"
#include "file_handle.h"
#include "storage/buffer_manager/file_handle.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

// VersionedFileHandle is the in-memory representation of a file whose pages can have multiple
// versions. It extends FileHandle with several data structures to keep track of the pageIdx's of
// the WAL versions of the pages. This is intended to be used by storage structures that maintain
// multiple versions of their pages (backed by WAL pages) and contains the helper functions they
// commonly need.
class VersionedFileHandle : public FileHandle {

public:
    VersionedFileHandle(
        const StorageStructureIDAndFName& storageStructureIDAndFName, uint8_t flags);

    // This function assumes that the caller has already acquired the lock for originalPageIdx.
    inline bool hasWALPageVersionNoPageLock(common::page_idx_t originalPageIdx) {
        auto pageGroupIdxAndPosInGroup = PageUtils::getPageElementCursorForPos(
            originalPageIdx, common::MULTI_VERSION_FILE_PAGE_GROUP_SIZE);
        // TODO: and Warning: This can be a major performance bottleneck, where we need to acquire a
        // shared lock for the entire file handle to ensure that another thread cannot be updating
        // pageVersions. We can fix this by instead making pageVersions a linkedList of vectors for
        // each page group (so not a vector of vector, which can be dynamically allocated) and
        // having a lock on each page group.
        std::shared_lock slock(fhSharedMutex);
        // There is an updated wal page if the PageVersionAndLockInfo for the page group exists
        // and the page version for the page is not null (which is UINT32_MAX).
        auto retVal = !pageVersions[pageGroupIdxAndPosInGroup.pageIdx].empty() &&
                      (pageVersions[pageGroupIdxAndPosInGroup.pageIdx]
                                   [pageGroupIdxAndPosInGroup.elemPosInPage] != UINT32_MAX);
        return retVal;
    }

    // This function assumes that the caller has already acquired the lock for originalPageIdx.
    inline common::page_idx_t getWALPageVersionNoPageLock(common::page_idx_t originalPageIdx) {
        // See the comment about a shared lock in hasWALPageVersionNoPageLock
        std::shared_lock slock(fhSharedMutex);
        auto pageGroupIdxAndPosInGroup = PageUtils::getPageElementCursorForPos(
            originalPageIdx, common::MULTI_VERSION_FILE_PAGE_GROUP_SIZE);
        return pageVersions[pageGroupIdxAndPosInGroup.pageIdx]
                           [pageGroupIdxAndPosInGroup.elemPosInPage];
    }

    void createPageVersionGroupIfNecessary(common::page_idx_t pageIdx);
    void clearWALPageVersionIfNecessary(common::page_idx_t pageIdx);

    void setWALPageVersion(common::page_idx_t originalPageIdx, common::page_idx_t pageIdxInWAL);
    void setWALPageVersionNoLock(
        common::page_idx_t originalPageIdx, common::page_idx_t pageIdxInWAL);

    common::page_idx_t addNewPage() override;
    void removePageIdxAndTruncateIfNecessary(common::page_idx_t pageIdxToRemove) override;
    inline StorageStructureID& getStorageStructureIDIDForWALRecord() {
        return storageStructureIDForWALRecord;
    }

private:
    common::page_idx_t addNewPageWithoutLock() override;
    uint32_t getNumPageGroups() {
        return ceil((double)numPages / common::MULTI_VERSION_FILE_PAGE_GROUP_SIZE);
    }

    void resizePageGroupLocksAndPageVersionsToNumPageGroupsWithoutLock();

private:
    StorageStructureID storageStructureIDForWALRecord;
    std::vector<std::vector<common::page_idx_t>> pageVersions;
    std::vector<std::unique_ptr<std::atomic_flag>> pageGroupLocks;
};

} // namespace storage
} // namespace kuzu

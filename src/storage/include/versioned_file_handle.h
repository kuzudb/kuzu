#pragma once

#include "file_handle.h"

#include "src/common/include/configs.h"
#include "src/storage/include/storage_utils.h"
#include "src/storage/include/wal/wal_record.h"

namespace graphflow {
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
    inline bool hasUpdatedWALPageVersionNoLock(page_idx_t originalPageIdx) {
        auto pageGroupIdxAndPosInGroup = PageUtils::getPageElementCursorForPos(
            originalPageIdx, MULTI_VERSION_FILE_PAGE_GROUP_SIZE);
        // There is an updated wal page if the PageVersionAndLockInfo for the page group exists
        // and the page version for the page is not null (which is UINT32_MAX).
        auto retVal =
            !pageVersions[pageGroupIdxAndPosInGroup.pageIdx].empty() &&
            (pageVersions[pageGroupIdxAndPosInGroup.pageIdx][pageGroupIdxAndPosInGroup.posInPage] !=
                UINT32_MAX);
        return retVal;
    }

    // This function assumes that the caller has already acquired the lock for originalPageIdx.
    inline page_idx_t getUpdatedWALPageVersionNoLock(page_idx_t originalPageIdx) {
        auto pageGroupIdxAndPosInGroup = PageUtils::getPageElementCursorForPos(
            originalPageIdx, MULTI_VERSION_FILE_PAGE_GROUP_SIZE);
        return pageVersions[pageGroupIdxAndPosInGroup.pageIdx][pageGroupIdxAndPosInGroup.posInPage];
    }

    void createPageVersionGroupIfNecessary(page_idx_t pageIdx);
    void clearUpdatedWALPageVersion(page_idx_t pageIdx);

    void setUpdatedWALPageVersionNoLock(page_idx_t originalPageIdx, page_idx_t pageIdxInWAL);

    page_idx_t addNewPage() override;
    void removePageIdxAndTruncateIfNecessary(page_idx_t pageIdxToRemove) override;
    inline StorageStructureID& getStorageStructureIDIDForWALRecord() {
        return storageStructureIDForWALRecord;
    }

private:
    uint32_t getNumPageGroups() {
        return ceil((double)numPages / MULTI_VERSION_FILE_PAGE_GROUP_SIZE);
    }

    void resizePageGroupLocksAndPageVersionsToNumPageGroupsWithoutLock();

private:
    StorageStructureID storageStructureIDForWALRecord;
    vector<vector<page_idx_t>> pageVersions;
    vector<unique_ptr<atomic_flag>> pageGroupLocks;
};

} // namespace storage
} // namespace graphflow

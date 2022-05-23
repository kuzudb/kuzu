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
    inline bool hasUpdatedWALPageVersionNoLock(uint64_t originalPageIdx) {
        auto pageGroupIdxAndPosInGroup = PageUtils::getPageElementCursorForOffset(
            originalPageIdx, MULTI_VERSION_FILE_PAGE_GROUP_SIZE);
        // There is an updated wal page if the PageVersionAndLockInfo for the page group exists
        // and the page version for the page is not null (which is UINT64_MAX).
        auto retVal =
            !pageVersions[pageGroupIdxAndPosInGroup.pageIdx].empty() &&
            (pageVersions[pageGroupIdxAndPosInGroup.pageIdx][pageGroupIdxAndPosInGroup.pos] !=
                UINT64_MAX);
        return retVal;
    }

    // This function assumes that the caller has already acquired the lock for originalPageIdx.
    inline uint64_t getUpdatedWALPageVersionNoLock(uint64_t originalPageIdx) {
        auto pageGroupIdxAndPosInGroup = PageUtils::getPageElementCursorForOffset(
            originalPageIdx, MULTI_VERSION_FILE_PAGE_GROUP_SIZE);
        return pageVersions[pageGroupIdxAndPosInGroup.pageIdx][pageGroupIdxAndPosInGroup.pos];
    }

    void createPageVersionGroupIfNecessary(uint64_t pageIdx);
    void clearUpdatedWALPageVersion(uint64_t pageIdx);

    void setUpdatedWALPageVersionNoLock(uint64_t originalPageIdx, uint64_t pageIdxInWAL);

    uint32_t addNewPage();
    void removePageIdxAndTruncateIfNecessary(uint64_t pageIdxToRemove);
    inline StorageStructureID& getStorageStructureIDIDForWALRecord() {
        return storageStructureIDForWALRecord;
    }

private:
    uint64_t getNumPageGroups() {
        return ceil((double)numPages / MULTI_VERSION_FILE_PAGE_GROUP_SIZE);
    }

    void resizePageGroupLocksAndPageVersionsToNumPageGroupsWithoutLock() {
        auto numPageGroups = getNumPageGroups();
        if (pageGroupLocks.size() == numPageGroups) {
            return;
        } else if (pageGroupLocks.size() < numPageGroups) {
            for (int i = pageGroupLocks.size(); i < getNumPageGroups(); ++i) {
                pageGroupLocks.push_back(make_unique<atomic_flag>());
            }
        } else {
            pageGroupLocks.resize(numPageGroups);
        }
        pageVersions.resize(getNumPageGroups());
    }

private:
    StorageStructureID storageStructureIDForWALRecord;
    vector<vector<uint64_t>> pageVersions;
    vector<unique_ptr<atomic_flag>> pageGroupLocks;
};

} // namespace storage
} // namespace graphflow

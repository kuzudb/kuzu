#pragma once

#include "fcntl.h"

#include "src/common/include/configs.h"
#include "src/common/include/vector/value_vector.h"
#include "src/storage/include/buffer_manager.h"
#include "src/storage/include/file_handle.h"
#include "src/storage/include/storage_utils.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

struct PageLocksAndVersionsPerPageGroup {
    vector<unique_ptr<atomic_flag>> pageLocks;
    vector<uint64_t> pageVersions;

    inline void resizeToPageGroupSize() {
        for (int i = 0; i < PAGE_VERSION_INFO_PAGE_GROUP_SIZE; ++i) {
            pageLocks.push_back(make_unique<atomic_flag>());
        }
        pageVersions.resize(PAGE_VERSION_INFO_PAGE_GROUP_SIZE, UINT64_MAX);
    }

    bool empty() { return pageLocks.empty(); }
};

class PageVersionInfo {

public:
    explicit PageVersionInfo(uint64_t numPages);

    void acquireLockForWritingToPage(uint64_t pageIdx);

    // This function assumes that the caller has already acquired the lock for originalPageIdx.
    void setUpdatedWALPageVersionNoLock(uint64_t originalPageIdx, uint64_t pageIdxInWAL);

    // This function assumes that the caller has already acquired the lock for originalPageIdx.
    inline bool hasUpdatedWALPageVersionNoLock(uint64_t originalPageIdx) {
        auto pageGroupIdxAndPosInGroup = PageUtils::getPageElementCursorForOffset(
            originalPageIdx, PAGE_VERSION_INFO_PAGE_GROUP_SIZE);
        // There is an updated wal page if the PageVersionAndLockInfo for the page group exists
        // and the page version for the page is not null (which is UINT64_MAX).
        return !pageLocksAndVersionsPerPageGroup[pageGroupIdxAndPosInGroup.idx].empty() &&
               (pageLocksAndVersionsPerPageGroup[pageGroupIdxAndPosInGroup.idx]
                       .pageVersions[pageGroupIdxAndPosInGroup.pos] != UINT64_MAX);
    }

    // This function assumes that the caller has already acquired the lock for originalPageIdx.
    inline uint64_t getUpdatedWALPageVersionNoLock(uint64_t originalPageIdx) {
        auto pageGroupIdxAndPosInGroup = PageUtils::getPageElementCursorForOffset(
            originalPageIdx, PAGE_VERSION_INFO_PAGE_GROUP_SIZE);
        return pageLocksAndVersionsPerPageGroup[pageGroupIdxAndPosInGroup.idx]
            .pageVersions[pageGroupIdxAndPosInGroup.pos];
    }

    // This function assumes that the caller has already acquired the lock for the page.
    void releaseLock(uint32_t originalPageIdx);

private:
    uint64_t numPages;
    vector<PageLocksAndVersionsPerPageGroup> pageLocksAndVersionsPerPageGroup;
    vector<unique_ptr<atomic_flag>> pageGroupLocks;
    mutex mtx;
};

} // namespace storage
} // namespace graphflow

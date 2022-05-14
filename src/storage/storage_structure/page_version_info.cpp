#include "src/storage/include/storage_structure/page_version_info.h"

namespace graphflow {
namespace storage {

PageVersionInfo::PageVersionInfo(uint64_t numPages) : numPages{numPages} {
    uint64_t numPageGroups = ceil((double)numPages / PAGE_VERSION_INFO_PAGE_GROUP_SIZE);
    for (int i = 0; i < numPageGroups; ++i) {
        pageGroupLocks.push_back(make_unique<atomic_flag>());
    }
    pageLocksAndVersionsPerPageGroup.resize(numPageGroups);
}

void PageVersionInfo::acquireLockForWritingToPage(uint64_t pageIdx) {
    // Although getPageElementCursorForOffset is written to get offsets of elements
    // in pages, it simply can be used to find the group/chunk and offset/pos in group/chunk for
    // any chunked data structure.
    auto pageGroupIdxAndPosInGroup =
        PageUtils::getPageElementCursorForOffset(pageIdx, PAGE_VERSION_INFO_PAGE_GROUP_SIZE);
    // If we have not created a vector of locks and pageVersion array for each page in this
    // group, first create them.
    if (pageLocksAndVersionsPerPageGroup[pageGroupIdxAndPosInGroup.idx].empty()) {
        bool acquired = false;
        while (!acquired) { // spinning wait
            acquired = !pageGroupLocks[pageGroupIdxAndPosInGroup.idx]->test_and_set();
        }
        // If another thread has not in the meanwhile created these data structures
        if (pageLocksAndVersionsPerPageGroup[pageGroupIdxAndPosInGroup.idx].empty()) {
            pageLocksAndVersionsPerPageGroup[pageGroupIdxAndPosInGroup.idx].resizeToPageGroupSize();
        }
    }

    bool acquired = false;
    while (!acquired) { // spinning wait
        acquired = !pageLocksAndVersionsPerPageGroup[pageGroupIdxAndPosInGroup.idx]
                        .pageLocks[pageGroupIdxAndPosInGroup.pos]
                        ->test_and_set();
    }
}

void PageVersionInfo::clearUpdatedWALPageVersion(uint64_t pageIdx) {
    acquireLockForWritingToPage(pageIdx);
    setUpdatedWALPageVersionNoLock(pageIdx, UINT64_MAX);
    releaseLock(pageIdx);
}

void PageVersionInfo::setUpdatedWALPageVersionNoLock(
    uint64_t originalPageIdx, uint64_t pageIdxInWAL) {
    auto pageGroupIdxAndPosInGroup = PageUtils::getPageElementCursorForOffset(
        originalPageIdx, PAGE_VERSION_INFO_PAGE_GROUP_SIZE);
    pageLocksAndVersionsPerPageGroup[pageGroupIdxAndPosInGroup.idx]
        .pageVersions[pageGroupIdxAndPosInGroup.pos] = pageIdxInWAL;
}

void PageVersionInfo::releaseLock(uint32_t pageIdx) {
    auto pageGroupIdxAndPosInGroup =
        PageUtils::getPageElementCursorForOffset(pageIdx, PAGE_VERSION_INFO_PAGE_GROUP_SIZE);
    pageLocksAndVersionsPerPageGroup[pageGroupIdxAndPosInGroup.idx]
        .pageLocks[pageGroupIdxAndPosInGroup.pos]
        ->clear();
}

} // namespace storage
} // namespace graphflow

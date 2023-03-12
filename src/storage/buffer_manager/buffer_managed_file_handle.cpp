#include "storage/buffer_manager/buffer_managed_file_handle.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

BufferManagedFileHandle::BufferManagedFileHandle(
    const std::string& path, uint8_t flags, FileVersionedType fileVersionedType)
    : FileHandle{path, flags}, fileVersionedType{fileVersionedType} {
    initPageIdxToFrameMapAndLocks();
    if (fileVersionedType == FileVersionedType::VERSIONED_FILE) {
        resizePageGroupLocksAndPageVersionsWithoutLock();
    }
}

void BufferManagedFileHandle::initPageIdxToFrameMapAndLocks() {
    pageIdxToFrameMap.resize(pageCapacity);
    pageLocks.resize(pageCapacity);
    for (auto i = 0ull; i < numPages; i++) {
        pageIdxToFrameMap[i] = std::make_unique<std::atomic<common::page_idx_t>>(UINT32_MAX);
        pageLocks[i] = std::make_unique<std::atomic_flag>();
    }
}

common::page_idx_t BufferManagedFileHandle::addNewPageWithoutLock() {
    if (numPages == pageCapacity) {
        pageCapacity += StorageConstants::PAGE_GROUP_SIZE;
        pageIdxToFrameMap.resize(pageCapacity);
        pageLocks.resize(pageCapacity);
    }
    pageLocks[numPages] = std::make_unique<std::atomic_flag>();
    pageIdxToFrameMap[numPages] = std::make_unique<std::atomic<common::page_idx_t>>(UINT32_MAX);
    auto newPageIdx = numPages++;
    if (fileVersionedType == FileVersionedType::VERSIONED_FILE) {
        resizePageGroupLocksAndPageVersionsWithoutLock();
    }
    return newPageIdx;
}

bool BufferManagedFileHandle::acquirePageLock(page_idx_t pageIdx, bool block) {
    if (block) {
        while (!acquire(pageIdx)) {} // spinning
        return true;
    }
    return acquire(pageIdx);
}

bool BufferManagedFileHandle::acquire(common::page_idx_t pageIdx) {
    if (pageIdx >= pageLocks.size()) {
        throw RuntimeException(
            StringUtils::string_format("pageIdx {} is >= pageLocks.size()", pageIdx));
    }
    auto retVal = !pageLocks[pageIdx]->test_and_set(std::memory_order_acquire);
    return retVal;
}

void BufferManagedFileHandle::createPageVersionGroupIfNecessary(page_idx_t pageIdx) {
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

void BufferManagedFileHandle::resetToZeroPagesAndPageCapacity() {
    std::unique_lock xlock(fhSharedMutex);
    numPages = 0;
    pageCapacity = 0;
    FileUtils::truncateFileToEmpty(fileInfo.get());
    initPageIdxToFrameMapAndLocks();
}

void BufferManagedFileHandle::removePageIdxAndTruncateIfNecessary(common::page_idx_t pageIdx) {
    std::unique_lock xLck{fhSharedMutex};
    removePageIdxAndTruncateIfNecessaryWithoutLock(pageIdx);
    if (fileVersionedType == FileVersionedType::VERSIONED_FILE) {
        resizePageGroupLocksAndPageVersionsWithoutLock();
    }
}

void BufferManagedFileHandle::removePageIdxAndTruncateIfNecessaryWithoutLock(
    common::page_idx_t pageIdxToRemove) {
    if (numPages <= pageIdxToRemove) {
        return;
    }
    for (auto pageIdx = pageIdxToRemove; pageIdx < numPages; ++pageIdx) {
        pageIdxToFrameMap[pageIdx].reset();
        pageLocks[pageIdx].reset();
    }
    numPages = pageIdxToRemove;
}

void BufferManagedFileHandle::resizePageGroupLocksAndPageVersionsWithoutLock() {
    auto numPageGroups = getNumPageGroups();
    if (pageGroupLocks.size() == numPageGroups) {
        return;
    } else if (pageGroupLocks.size() < numPageGroups) {
        for (auto i = pageGroupLocks.size(); i < numPageGroups; ++i) {
            pageGroupLocks.push_back(std::make_unique<std::atomic_flag>());
        }
    } else {
        pageGroupLocks.resize(numPageGroups);
    }
    pageVersions.resize(numPageGroups);
}

// This function assumes that the caller has already acquired the lock for originalPageIdx.
bool BufferManagedFileHandle::hasWALPageVersionNoPageLock(common::page_idx_t originalPageIdx) {
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

void BufferManagedFileHandle::clearWALPageVersionIfNecessary(common::page_idx_t pageIdx) {
    {
        std::shared_lock sLck{fhSharedMutex};
        if (numPages <= pageIdx) {
            return;
        }
    }
    createPageVersionGroupIfNecessary(pageIdx);
    setWALPageVersionNoLock(pageIdx, UINT32_MAX);
    releasePageLock(pageIdx);
}

// This function assumes that the caller has already acquired the lock for originalPageIdx.
common::page_idx_t BufferManagedFileHandle::getWALPageVersionNoPageLock(
    common::page_idx_t originalPageIdx) {
    assert(fileVersionedType == FileVersionedType::VERSIONED_FILE);
    // See the comment about a shared lock in hasWALPageVersionNoPageLock
    std::shared_lock sLck{fhSharedMutex};
    auto pageGroupIdxAndPosInGroup =
        PageUtils::getPageElementCursorForPos(originalPageIdx, StorageConstants::PAGE_GROUP_SIZE);
    return pageVersions[pageGroupIdxAndPosInGroup.pageIdx][pageGroupIdxAndPosInGroup.elemPosInPage];
}

void BufferManagedFileHandle::setWALPageVersion(
    common::page_idx_t originalPageIdx, common::page_idx_t pageIdxInWAL) {
    assert(fileVersionedType == FileVersionedType::VERSIONED_FILE);
    std::shared_lock sLck{fhSharedMutex};
    setWALPageVersionNoLock(originalPageIdx, pageIdxInWAL);
}

void BufferManagedFileHandle::setWALPageVersionNoLock(
    common::page_idx_t originalPageIdx, common::page_idx_t pageIdxInWAL) {
    auto pageGroupIdxAndPosInGroup =
        PageUtils::getPageElementCursorForPos(originalPageIdx, StorageConstants::PAGE_GROUP_SIZE);
    pageVersions[pageGroupIdxAndPosInGroup.pageIdx][pageGroupIdxAndPosInGroup.elemPosInPage] =
        pageIdxInWAL;
}

} // namespace storage
} // namespace kuzu

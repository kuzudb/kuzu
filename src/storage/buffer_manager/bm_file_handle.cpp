#include "storage/buffer_manager/bm_file_handle.h"

#include "storage/buffer_manager/buffer_manager.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

WALPageIdxGroup::WALPageIdxGroup() {
    walPageIdxes.resize(StorageConstants::PAGE_GROUP_SIZE, INVALID_PAGE_IDX);
    walPageIdxLocks.resize(StorageConstants::PAGE_GROUP_SIZE);
    for (auto i = 0u; i < StorageConstants::PAGE_GROUP_SIZE; i++) {
        walPageIdxLocks[i] = std::make_unique<std::mutex>();
    }
}

BMFileHandle::BMFileHandle(const std::string& path, uint8_t flags, BufferManager* bm,
    PageSizeClass pageSizeClass, FileVersionedType fileVersionedType)
    : FileHandle{path, flags}, bm{bm}, pageSizeClass{pageSizeClass}, fileVersionedType{
                                                                         fileVersionedType} {
    initPageStatesAndGroups();
}

BMFileHandle::~BMFileHandle() {
    bm->removeFilePagesFromFrames(*this);
}

void BMFileHandle::initPageStatesAndGroups() {
    pageStates.resize(pageCapacity);
    for (auto i = 0ull; i < numPages; i++) {
        pageStates[i] = std::make_unique<PageState>();
    }
    auto numPageGroups = getNumPageGroups();
    frameGroupIdxes.resize(numPageGroups);
    for (auto i = 0u; i < numPageGroups; i++) {
        frameGroupIdxes[i] = bm->addNewFrameGroup(pageSizeClass);
    }
}

page_idx_t BMFileHandle::addNewPageWithoutLock() {
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
}

page_group_idx_t BMFileHandle::addWALPageIdxGroupIfNecessary(page_idx_t originalPageIdx) {
    assert(fileVersionedType == FileVersionedType::VERSIONED_FILE);
    std::unique_lock xLck{fhSharedMutex};
    assert(originalPageIdx < numPages);
    // Although getPageElementCursorForPos is written to get offsets of elements
    // in pages, it simply can be used to find the group/chunk and offset/pos in group/chunk for
    // any chunked data structure.
    auto [pageGroupIdx, _] =
        PageUtils::getPageElementCursorForPos(originalPageIdx, StorageConstants::PAGE_GROUP_SIZE);
    if (!walPageIdxGroups.contains(pageGroupIdx)) {
        walPageIdxGroups.insert(std::make_pair(pageGroupIdx, std::make_unique<WALPageIdxGroup>()));
    }
    return pageGroupIdx;
}

void BMFileHandle::resetToZeroPagesAndPageCapacity() {
    removePageIdxAndTruncateIfNecessary(0 /* pageIdx */);
    FileUtils::truncateFileToEmpty(fileInfo.get());
}

void BMFileHandle::removePageIdxAndTruncateIfNecessary(page_idx_t pageIdx) {
    std::unique_lock xLck{fhSharedMutex};
    if (numPages <= pageIdx) {
        return;
    }
    numPages = pageIdx;
    pageStates.resize(numPages);
    auto numPageGroups = getNumPageGroups();
    if (numPageGroups == frameGroupIdxes.size()) {
        return;
    }
    assert(numPageGroups < frameGroupIdxes.size());
    frameGroupIdxes.resize(numPageGroups);
    if (numPageGroups == 0) {
        walPageIdxGroups.clear();
    } else {
        for (auto groupIdx = numPageGroups; groupIdx < frameGroupIdxes.size(); groupIdx++) {
            walPageIdxGroups.erase(groupIdx);
        }
    }
    pageCapacity = numPageGroups * StorageConstants::PAGE_GROUP_SIZE;
}

void BMFileHandle::acquireWALPageIdxLock(page_idx_t originalPageIdx) {
    assert(fileVersionedType == FileVersionedType::VERSIONED_FILE);
    std::shared_lock sLck{fhSharedMutex};
    assert(numPages > originalPageIdx);
    auto [pageGroupIdx, pageIdxInGroup] =
        PageUtils::getPageElementCursorForPos(originalPageIdx, StorageConstants::PAGE_GROUP_SIZE);
    assert(walPageIdxGroups.contains(pageGroupIdx));
    walPageIdxGroups[pageGroupIdx]->acquireWALPageIdxLock(pageIdxInGroup);
}

void BMFileHandle::releaseWALPageIdxLock(page_idx_t originalPageIdx) {
    std::shared_lock sLck{fhSharedMutex};
    auto [pageGroupIdx, pageIdxInGroup] =
        PageUtils::getPageElementCursorForPos(originalPageIdx, StorageConstants::PAGE_GROUP_SIZE);
    walPageIdxGroups[pageGroupIdx]->releaseWALPageIdxLock(pageIdxInGroup);
}

bool BMFileHandle::hasWALPageGroup(page_group_idx_t originalPageIdx) {
    assert(fileVersionedType == FileVersionedType::VERSIONED_FILE);
    std::shared_lock sLck{fhSharedMutex};
    auto [pageGroupIdx, pageIdxInGroup] =
        PageUtils::getPageElementCursorForPos(originalPageIdx, StorageConstants::PAGE_GROUP_SIZE);
    return walPageIdxGroups.contains(pageGroupIdx);
}

bool BMFileHandle::hasWALPageVersionNoWALPageIdxLock(page_idx_t originalPageIdx) {
    assert(fileVersionedType == FileVersionedType::VERSIONED_FILE);
    std::shared_lock sLck{fhSharedMutex};
    auto [pageGroupIdx, pageIdxInGroup] =
        PageUtils::getPageElementCursorForPos(originalPageIdx, StorageConstants::PAGE_GROUP_SIZE);
    if (walPageIdxGroups.contains(pageGroupIdx)) {
        return walPageIdxGroups[pageGroupIdx]->getWALVersionPageIdxNoLock(pageIdxInGroup) !=
               INVALID_PAGE_IDX;
    }
    return false;
}

void BMFileHandle::clearWALPageIdxIfNecessary(page_idx_t originalPageIdx) {
    std::shared_lock sLck{fhSharedMutex};
    if (numPages <= originalPageIdx) {
        return;
    }
    auto [pageGroupIdx, pageIdxInGroup] =
        PageUtils::getPageElementCursorForPos(originalPageIdx, StorageConstants::PAGE_GROUP_SIZE);
    if (walPageIdxGroups.contains(pageGroupIdx)) {
        walPageIdxGroups[pageGroupIdx]->acquireWALPageIdxLock(pageIdxInGroup);
        setWALPageIdxNoLock(originalPageIdx, INVALID_PAGE_IDX);
        walPageIdxGroups[pageGroupIdx]->releaseWALPageIdxLock(pageIdxInGroup);
    }
}

// This function assumes that the caller has already acquired the lock for originalPageIdx.
page_idx_t BMFileHandle::getWALPageIdxNoWALPageIdxLock(page_idx_t originalPageIdx) {
    assert(fileVersionedType == FileVersionedType::VERSIONED_FILE);
    // See the comment about a shared lock in hasWALPageVersionNoWALPageIdxLock
    std::shared_lock sLck{fhSharedMutex};
    auto [pageGroupIdx, pageIdxInGroup] =
        PageUtils::getPageElementCursorForPos(originalPageIdx, StorageConstants::PAGE_GROUP_SIZE);
    return walPageIdxGroups[pageGroupIdx]->getWALVersionPageIdxNoLock(pageIdxInGroup);
}

void BMFileHandle::setWALPageIdx(page_idx_t originalPageIdx, page_idx_t pageIdxInWAL) {
    assert(fileVersionedType == FileVersionedType::VERSIONED_FILE);
    std::shared_lock sLck{fhSharedMutex};
    setWALPageIdxNoLock(originalPageIdx, pageIdxInWAL);
}

void BMFileHandle::setWALPageIdxNoLock(page_idx_t originalPageIdx, page_idx_t pageIdxInWAL) {
    auto [pageGroupIdx, pageIdxInGroup] =
        PageUtils::getPageElementCursorForPos(originalPageIdx, StorageConstants::PAGE_GROUP_SIZE);
    assert(walPageIdxGroups.contains(pageGroupIdx));
    walPageIdxGroups[pageGroupIdx]->setWALVersionPageIdxNoLock(pageIdxInGroup, pageIdxInWAL);
}

} // namespace storage
} // namespace kuzu

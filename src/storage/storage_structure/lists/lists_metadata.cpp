#include "src/storage/storage_structure/include/lists/lists_metadata.h"

#include "spdlog/spdlog.h"

#include "src/common/include/utils.h"
#include "src/storage/include/storage_utils.h"

namespace graphflow {
namespace storage {

ListsMetadata::ListsMetadata(const string& listBaseFName, bool isForBuilding) {
    metadataFileHandle = make_unique<FileHandle>(
        listBaseFName + ".metadata", FileHandle::O_DefaultPagedExistingDBFileCreateIfNotExists);
    logger = LoggerUtils::getOrCreateSpdLogger("storage");
    if (!isForBuilding) {
        readFromDisk(listBaseFName);
    } else {
        // When building list metadata during loading, we need to make space for the header of each
        // disk array in listsMetadata (so that these header pages are pre-allocated and not used
        // for another purpose)
        metadataFileHandle->addNewPage(); // CHUNK_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX=0
        metadataFileHandle
            ->addNewPage(); // LARGE_LIST_IDX_TO_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX
        metadataFileHandle->addNewPage(); // CHUNK_PAGE_LIST_HEADER_PAGE_IDX
        // Initialize an empty page lists array for building
        pageLists = make_unique<InMemDiskArray<page_idx_t>>(
            *metadataFileHandle, CHUNK_PAGE_LIST_HEADER_PAGE_IDX, 0);
    }
}

void ListsMetadata::saveToDisk() {
    chunkToPageListHeadIdxMap->saveToDisk();
    pageLists->saveToDisk();
    largeListIdxToPageListHeadIdxMap->saveToDisk();
}

void ListsMetadata::readFromDisk(const string& fName) {
    chunkToPageListHeadIdxMap = make_unique<InMemDiskArray<uint32_t>>(
        *metadataFileHandle, CHUNK_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX);
    pageLists = make_unique<InMemDiskArray<page_idx_t>>(
        *metadataFileHandle, CHUNK_PAGE_LIST_HEADER_PAGE_IDX);
    largeListIdxToPageListHeadIdxMap = make_unique<InMemDiskArray<uint32_t>>(
        *metadataFileHandle, LARGE_LIST_IDX_TO_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX);
}

uint64_t ListsMetadata::getPageIdxFromAPageList(
    InMemDiskArray<page_idx_t>* pageLists, uint32_t pageListHead, uint32_t logicalPageIdx) {
    auto pageListGroupHeadIdx = pageListHead;
    while (PAGE_LIST_GROUP_SIZE <= logicalPageIdx) {
        pageListGroupHeadIdx = (*pageLists)[pageListGroupHeadIdx + PAGE_LIST_GROUP_SIZE];
        logicalPageIdx -= PAGE_LIST_GROUP_SIZE;
    }
    return (*pageLists)[pageListGroupHeadIdx + logicalPageIdx];
}

void ListsMetadata::initChunkPageLists(uint32_t numChunks_) {
    chunkToPageListHeadIdxMap = make_unique<InMemDiskArray<uint32_t>>(
        *metadataFileHandle, CHUNK_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX, numChunks_);
    for (uint64_t chunkIdx = 0; chunkIdx < numChunks_; ++chunkIdx) {
        (*chunkToPageListHeadIdxMap)[chunkIdx] = UINT32_MAX;
    }
}

void ListsMetadata::initLargeListPageLists(uint32_t numLargeLists_) {
    // For each largeList, we store the PageListHeadIdx in pageLists and also the number of elements
    // in the large list.
    largeListIdxToPageListHeadIdxMap = make_unique<InMemDiskArray<uint32_t>>(*metadataFileHandle,
        LARGE_LIST_IDX_TO_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX, (2 * numLargeLists_));
    for (uint64_t largeListIdx = 0; largeListIdx < numLargeLists_; ++largeListIdx) {
        (*largeListIdxToPageListHeadIdxMap)[largeListIdx] = UINT32_MAX;
    }
}

void ListsMetadata::populateChunkPageList(
    uint32_t chunkId, uint32_t numPages_, uint32_t startPageId) {
    if (numPages_ == 0) {
        return;
    }
    (*chunkToPageListHeadIdxMap)[chunkId] = pageLists->header.numElements;
    populatePageIdsInAPageList(numPages_, startPageId);
}

void ListsMetadata::populateLargeListPageList(
    uint32_t largeListIdx, uint32_t numPages_, uint32_t numElements, uint32_t startPageId) {
    cout << "populateLargeListPageList called. numPages_: " << numPages_ << endl;
    assert(numPages_ > 0);
    auto offsetInMap = 2 * largeListIdx;
    (*largeListIdxToPageListHeadIdxMap)[offsetInMap] = pageLists->header.numElements;
    (*largeListIdxToPageListHeadIdxMap)[offsetInMap + 1] = numElements;
    populatePageIdsInAPageList(numPages_, startPageId);
}

void ListsMetadata::populatePageIdsInAPageList(uint32_t numPages, uint32_t startPageId) {
    // calculate the number of pageListGroups to accommodate the pageList completely.
    auto numPageListGroups = numPages / PAGE_LIST_GROUP_SIZE;
    if (0 != numPages % PAGE_LIST_GROUP_SIZE) {
        numPageListGroups++;
    }
    // During the initial allocation, we allocate all the pageListGroups of a pageList contiguously.
    // pageListTailIdx is the id in the pageLists blob where the pageList ends and the next
    // pageLists should start.
    uint32_t pageListHeadIdx = pageLists->header.numElements;
    auto pageListTailIdx =
        pageListHeadIdx + ((PAGE_LIST_GROUP_WITH_NEXT_PTR_SIZE)*numPageListGroups);
    pageLists->setNewNumElementsAndIncreaseCapacityIfNeeded(pageListTailIdx);
    for (auto i = 0u; i < numPageListGroups; i++) {
        auto numPagesInThisGroup = min(PAGE_LIST_GROUP_SIZE, numPages);
        for (auto j = 0u; j < numPagesInThisGroup; j++) {
            (*pageLists)[pageListHeadIdx + j] = startPageId++;
        }
        // if there are empty spots in the pageListGroup, put PAGE_IDX_MAX in them.
        for (auto j = numPagesInThisGroup; j < PAGE_LIST_GROUP_SIZE; j++) {
            (*pageLists)[pageListHeadIdx + j] = PAGE_IDX_MAX;
        }
        numPages -= numPagesInThisGroup;
        pageListHeadIdx += PAGE_LIST_GROUP_SIZE;
        // store the id to the next pageListGroup, if exists, other PAGE_IDX_MAX.
        (*pageLists)[pageListHeadIdx] = (0 == numPages) ? PAGE_IDX_MAX : 1 + pageListHeadIdx;
        pageListHeadIdx++;
    }
}
} // namespace storage
} // namespace graphflow

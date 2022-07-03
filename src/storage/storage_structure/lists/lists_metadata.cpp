#include "src/storage/storage_structure/include/lists/lists_metadata.h"

#include "spdlog/spdlog.h"

#include "src/common/include/utils.h"
#include "src/storage/include/storage_utils.h"

namespace graphflow {
namespace storage {

ListsMetadata::ListsMetadata(const StorageStructureIDAndFName storageStructureIDAndFNameForBaseList)
    : BaseListsMetadata(storageStructureIDAndFNameForBaseList.fName),
      storageStructureIDAndFName(storageStructureIDAndFNameForBaseList) {
    storageStructureIDAndFName.storageStructureID.listFileID.listFileType = ListFileType::METADATA;
    storageStructureIDAndFName.fName = metadataFileHandle->getFileInfo()->path;
    chunkToPageListHeadIdxMap = make_unique<InMemDiskArray<uint32_t>>(
        *metadataFileHandle, CHUNK_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX);
    largeListIdxToPageListHeadIdxMap = make_unique<InMemDiskArray<uint32_t>>(
        *metadataFileHandle, LARGE_LIST_IDX_TO_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX);
    pageLists = make_unique<InMemDiskArray<page_idx_t>>(
        *metadataFileHandle, CHUNK_PAGE_LIST_HEADER_PAGE_IDX);
}

uint64_t BaseListsMetadata::getPageIdxFromAPageList(
    BaseInMemDiskArray<page_idx_t>* pageLists, uint32_t pageListHead, uint32_t logicalPageIdx) {
    auto pageListGroupHeadIdx = pageListHead;
    while (PAGE_LIST_GROUP_SIZE <= logicalPageIdx) {
        pageListGroupHeadIdx = (*pageLists)[pageListGroupHeadIdx + PAGE_LIST_GROUP_SIZE];
        logicalPageIdx -= PAGE_LIST_GROUP_SIZE;
    }
    return (*pageLists)[pageListGroupHeadIdx + logicalPageIdx];
}

ListsMetadataBuilder::ListsMetadataBuilder(const string& listBaseFName)
    : BaseListsMetadata(listBaseFName) {
    // When building list metadata during loading, we need to make space for the header of each
    // disk array in listsMetadata (so that these header pages are pre-allocated and not used
    // for another purpose)
    metadataFileHandle->addNewPage(); // CHUNK_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX=0
    metadataFileHandle->addNewPage(); // LARGE_LIST_IDX_TO_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX
    metadataFileHandle->addNewPage(); // CHUNK_PAGE_LIST_HEADER_PAGE_IDX
    // Initialize an empty page lists array for building
    pageListsBuilder = make_unique<InMemDiskArrayBuilder<page_idx_t>>(
        *metadataFileHandle, CHUNK_PAGE_LIST_HEADER_PAGE_IDX, 0);
}

void ListsMetadataBuilder::saveToDisk() {
    chunkToPageListHeadIdxMapBuilder->saveToDisk();
    largeListIdxToPageListHeadIdxMapBuilder->saveToDisk();
    pageListsBuilder->saveToDisk();
}

void ListsMetadataBuilder::initChunkPageLists(uint32_t numChunks_) {
    chunkToPageListHeadIdxMapBuilder = make_unique<InMemDiskArrayBuilder<uint32_t>>(
        *metadataFileHandle, CHUNK_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX, numChunks_);
    for (uint64_t chunkIdx = 0; chunkIdx < numChunks_; ++chunkIdx) {
        (*chunkToPageListHeadIdxMapBuilder)[chunkIdx] = UINT32_MAX;
    }
}

void ListsMetadataBuilder::initLargeListPageLists(uint32_t numLargeLists_) {
    // For each largeList, we store the PageListHeadIdx in pageLists and also the number of
    // elements in the large list.
    largeListIdxToPageListHeadIdxMapBuilder =
        make_unique<InMemDiskArrayBuilder<uint32_t>>(*metadataFileHandle,
            LARGE_LIST_IDX_TO_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX, (2 * numLargeLists_));
    for (uint64_t largeListIdx = 0; largeListIdx < numLargeLists_; ++largeListIdx) {
        (*largeListIdxToPageListHeadIdxMapBuilder)[largeListIdx] = UINT32_MAX;
    }
}

void ListsMetadataBuilder::populateChunkPageList(
    uint32_t chunkId, uint32_t numPages_, uint32_t startPageId) {
    if (numPages_ == 0) {
        return;
    }
    (*chunkToPageListHeadIdxMapBuilder)[chunkId] = pageListsBuilder->header.numElements;
    populatePageIdsInAPageList(numPages_, startPageId);
}

void ListsMetadataBuilder::populateLargeListPageList(
    uint32_t largeListIdx, uint32_t numPages_, uint32_t numElements, uint32_t startPageId) {
    assert(numPages_ > 0);
    auto offsetInMap = 2 * largeListIdx;
    (*largeListIdxToPageListHeadIdxMapBuilder)[offsetInMap] = pageListsBuilder->header.numElements;
    (*largeListIdxToPageListHeadIdxMapBuilder)[offsetInMap + 1] = numElements;
    populatePageIdsInAPageList(numPages_, startPageId);
}

void ListsMetadataBuilder::populatePageIdsInAPageList(uint32_t numPages, uint32_t startPageId) {
    // calculate the number of pageListGroups to accommodate the pageList completely.
    auto numPageListGroups = numPages / PAGE_LIST_GROUP_SIZE;
    if (0 != numPages % PAGE_LIST_GROUP_SIZE) {
        numPageListGroups++;
    }
    // During the initial allocation, we allocate all the pageListGroups of a pageList
    // contiguously. pageListTailIdx is the id in the pageLists blob where the pageList ends and
    // the next pageLists should start.
    uint32_t pageListHeadIdx = pageListsBuilder->header.numElements;
    auto pageListTailIdx =
        pageListHeadIdx + ((PAGE_LIST_GROUP_WITH_NEXT_PTR_SIZE)*numPageListGroups);
    pageListsBuilder->setNewNumElementsAndIncreaseCapacityIfNeeded(pageListTailIdx);
    for (auto i = 0u; i < numPageListGroups; i++) {
        auto numPagesInThisGroup = min(PAGE_LIST_GROUP_SIZE, numPages);
        for (auto j = 0u; j < numPagesInThisGroup; j++) {
            (*pageListsBuilder)[pageListHeadIdx + j] = startPageId++;
        }
        // if there are empty spots in the pageListGroup, put PAGE_IDX_MAX in them.
        for (auto j = numPagesInThisGroup; j < PAGE_LIST_GROUP_SIZE; j++) {
            (*pageListsBuilder)[pageListHeadIdx + j] = PAGE_IDX_MAX;
        }
        numPages -= numPagesInThisGroup;
        pageListHeadIdx += PAGE_LIST_GROUP_SIZE;
        // store the id to the next pageListGroup, if exists, other PAGE_IDX_MAX.
        (*pageListsBuilder)[pageListHeadIdx] = (0 == numPages) ? PAGE_IDX_MAX : 1 + pageListHeadIdx;
        pageListHeadIdx++;
    }
}
} // namespace storage
} // namespace graphflow

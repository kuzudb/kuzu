#include "storage/storage_structure/lists/lists_metadata.h"

#include "common/utils.h"
#include "spdlog/spdlog.h"
#include "storage/storage_structure/storage_structure_utils.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace storage {

ListsMetadata::ListsMetadata(
    const StorageStructureIDAndFName& storageStructureIDAndFNameForBaseList,
    BufferManager* bufferManager, WAL* wal)
    : BaseListsMetadata(), storageStructureIDAndFName(storageStructureIDAndFNameForBaseList) {
    storageStructureIDAndFName.storageStructureID.listFileID.listFileType = ListFileType::METADATA;
    storageStructureIDAndFName.fName =
        StorageUtils::getListMetadataFName(storageStructureIDAndFNameForBaseList.fName);
    metadataVersionedFileHandle = make_unique<VersionedFileHandle>(
        storageStructureIDAndFName, FileHandle::O_PERSISTENT_FILE_NO_CREATE);
    chunkToPageListHeadIdxMap = make_unique<InMemDiskArray<uint32_t>>(*metadataVersionedFileHandle,
        CHUNK_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX, bufferManager, wal);
    largeListIdxToPageListHeadIdxMap =
        make_unique<InMemDiskArray<uint32_t>>(*metadataVersionedFileHandle,
            LARGE_LIST_IDX_TO_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX, bufferManager, wal);
    pageLists = make_unique<InMemDiskArray<page_idx_t>>(
        *metadataVersionedFileHandle, CHUNK_PAGE_LIST_HEADER_PAGE_IDX, bufferManager, wal);
}

uint64_t BaseListsMetadata::getPageIdxFromAPageList(
    BaseInMemDiskArray<page_idx_t>* pageLists, uint32_t pageListHead, uint32_t idxInPageList) {
    auto pageListGroupHeadIdx = pageListHead;
    while (ListsMetadataConfig::PAGE_LIST_GROUP_SIZE <= idxInPageList) {
        pageListGroupHeadIdx =
            (*pageLists)[pageListGroupHeadIdx + ListsMetadataConfig::PAGE_LIST_GROUP_SIZE];
        idxInPageList -= ListsMetadataConfig::PAGE_LIST_GROUP_SIZE;
    }
    return (*pageLists)[pageListGroupHeadIdx + idxInPageList];
}

ListsMetadataBuilder::ListsMetadataBuilder(const string& listBaseFName) : BaseListsMetadata() {
    metadataFileHandleForBuilding =
        make_unique<FileHandle>(StorageUtils::getListMetadataFName(listBaseFName),
            FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS);
    // When building list metadata during loading, we need to make space for the header of each
    // disk array in listsMetadata (so that these header pages are pre-allocated and not used
    // for another purpose)
    metadataFileHandleForBuilding->addNewPage(); // CHUNK_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX=0
    metadataFileHandleForBuilding
        ->addNewPage(); // LARGE_LIST_IDX_TO_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX
    metadataFileHandleForBuilding->addNewPage(); // CHUNK_PAGE_LIST_HEADER_PAGE_IDX
    // Initialize an empty page lists array for building
    pageListsBuilder = make_unique<InMemDiskArrayBuilder<page_idx_t>>(
        *metadataFileHandleForBuilding, CHUNK_PAGE_LIST_HEADER_PAGE_IDX, 0);
}

void ListsMetadataBuilder::saveToDisk() {
    chunkToPageListHeadIdxMapBuilder->saveToDisk();
    largeListIdxToPageListHeadIdxMapBuilder->saveToDisk();
    pageListsBuilder->saveToDisk();
}

void ListsMetadataBuilder::initChunkPageLists(uint32_t numChunks_) {
    chunkToPageListHeadIdxMapBuilder = make_unique<InMemDiskArrayBuilder<uint32_t>>(
        *metadataFileHandleForBuilding, CHUNK_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX, numChunks_);
    for (uint64_t chunkIdx = 0; chunkIdx < numChunks_; ++chunkIdx) {
        (*chunkToPageListHeadIdxMapBuilder)[chunkIdx] =
            StorageStructureUtils::NULL_CHUNK_OR_LARGE_LIST_HEAD_IDX;
    }
}

void ListsMetadataBuilder::initLargeListPageLists(uint32_t numLargeLists_) {
    // For each largeList, we store the PageListHeadIdx in pageLists and also the number of
    // elements in the large list.
    largeListIdxToPageListHeadIdxMapBuilder =
        make_unique<InMemDiskArrayBuilder<uint32_t>>(*metadataFileHandleForBuilding,
            LARGE_LIST_IDX_TO_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX, (2 * numLargeLists_));
    for (uint64_t largeListIdx = 0; largeListIdx < numLargeLists_; ++largeListIdx) {
        (*largeListIdxToPageListHeadIdxMapBuilder)[largeListIdx] =
            StorageStructureUtils::NULL_CHUNK_OR_LARGE_LIST_HEAD_IDX;
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
    auto numPageListGroups = numPages / ListsMetadataConfig::PAGE_LIST_GROUP_SIZE;
    if (0 != numPages % ListsMetadataConfig::PAGE_LIST_GROUP_SIZE) {
        numPageListGroups++;
    }
    // During the initial allocation, we allocate all the pageListGroups of a pageList
    // contiguously. pageListTailIdx is the id in the pageLists blob where the pageList ends and
    // the next pageLists should start.
    uint32_t pageListHeadIdx = pageListsBuilder->header.numElements;
    auto pageListTailIdx =
        pageListHeadIdx +
        ((ListsMetadataConfig::PAGE_LIST_GROUP_WITH_NEXT_PTR_SIZE)*numPageListGroups);
    pageListsBuilder->resize(pageListTailIdx, false /* setToZero */);
    for (auto i = 0u; i < numPageListGroups; i++) {
        auto numPagesInThisGroup = min(ListsMetadataConfig::PAGE_LIST_GROUP_SIZE, numPages);
        for (auto j = 0u; j < numPagesInThisGroup; j++) {
            (*pageListsBuilder)[pageListHeadIdx + j] = startPageId++;
        }
        // if there are empty spots in the pageListGroup, put StorageConfig::NULL_PAGE_IDX in them.
        for (auto j = numPagesInThisGroup; j < ListsMetadataConfig::PAGE_LIST_GROUP_SIZE; j++) {
            (*pageListsBuilder)[pageListHeadIdx + j] = StorageStructureUtils::NULL_PAGE_IDX;
        }
        numPages -= numPagesInThisGroup;
        pageListHeadIdx += ListsMetadataConfig::PAGE_LIST_GROUP_SIZE;
        // store the id to the next pageListGroup, if exists, otherwise store
        // StorageConfig::NULL_PAGE_IDX.
        (*pageListsBuilder)[pageListHeadIdx] =
            (0 == numPages) ? StorageStructureUtils::NULL_PAGE_IDX : 1 + pageListHeadIdx;
        pageListHeadIdx++;
    }
}
} // namespace storage
} // namespace kuzu

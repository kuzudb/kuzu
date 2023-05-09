#include "storage/storage_structure/lists/lists_metadata.h"

#include "common/utils.h"
#include "spdlog/spdlog.h"
#include "storage/storage_structure/storage_structure_utils.h"
#include "storage/storage_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

ListsMetadata::ListsMetadata(
    const StorageStructureIDAndFName& storageStructureIDAndFNameForBaseList,
    BufferManager* bufferManager, WAL* wal)
    : BaseListsMetadata(), storageStructureIDAndFName(storageStructureIDAndFNameForBaseList) {
    storageStructureIDAndFName.storageStructureID.listFileID.listFileType = ListFileType::METADATA;
    storageStructureIDAndFName.fName =
        StorageUtils::getListMetadataFName(storageStructureIDAndFNameForBaseList.fName);
    metadataVersionedFileHandle = bufferManager->getBMFileHandle(storageStructureIDAndFName.fName,
        FileHandle::O_PERSISTENT_FILE_NO_CREATE, BMFileHandle::FileVersionedType::VERSIONED_FILE);
    chunkToPageListHeadIdxMap = std::make_unique<InMemDiskArray<uint32_t>>(
        *metadataVersionedFileHandle, storageStructureIDAndFName.storageStructureID,
        CHUNK_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX, bufferManager, wal);
    pageLists = std::make_unique<InMemDiskArray<page_idx_t>>(*metadataVersionedFileHandle,
        storageStructureIDAndFName.storageStructureID, CHUNK_PAGE_LIST_HEADER_PAGE_IDX,
        bufferManager, wal);
}

uint64_t BaseListsMetadata::getPageIdxFromAPageList(
    BaseInMemDiskArray<page_idx_t>* pageLists, uint32_t pageListHead, uint32_t idxInPageList) {
    auto pageListGroupHeadIdx = pageListHead;
    while (ListsMetadataConstants::PAGE_LIST_GROUP_SIZE <= idxInPageList) {
        pageListGroupHeadIdx =
            (*pageLists)[pageListGroupHeadIdx + ListsMetadataConstants::PAGE_LIST_GROUP_SIZE];
        idxInPageList -= ListsMetadataConstants::PAGE_LIST_GROUP_SIZE;
    }
    return (*pageLists)[pageListGroupHeadIdx + idxInPageList];
}

ListsMetadataBuilder::ListsMetadataBuilder(const std::string& listBaseFName) : BaseListsMetadata() {
    metadataFileHandleForBuilding =
        std::make_unique<FileHandle>(StorageUtils::getListMetadataFName(listBaseFName),
            FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS);
    // When building list metadata during loading, we need to make space for the header of each
    // disk array in listsMetadata (so that these header pages are pre-allocated and not used
    // for another purpose)
    metadataFileHandleForBuilding->addNewPage(); // CHUNK_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX=0
    metadataFileHandleForBuilding
        ->addNewPage(); // LARGE_LIST_IDX_TO_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX
    metadataFileHandleForBuilding->addNewPage(); // CHUNK_PAGE_LIST_HEADER_PAGE_IDX
    // Initialize an empty page lists array for building
    pageListsBuilder = std::make_unique<InMemDiskArrayBuilder<page_idx_t>>(
        *metadataFileHandleForBuilding, CHUNK_PAGE_LIST_HEADER_PAGE_IDX, 0);
}

void ListsMetadataBuilder::saveToDisk() {
    chunkToPageListHeadIdxMapBuilder->saveToDisk();
    pageListsBuilder->saveToDisk();
}

void ListsMetadataBuilder::initChunkPageLists(uint32_t numChunks_) {
    chunkToPageListHeadIdxMapBuilder = std::make_unique<InMemDiskArrayBuilder<uint32_t>>(
        *metadataFileHandleForBuilding, CHUNK_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX, numChunks_);
    for (uint64_t chunkIdx = 0; chunkIdx < numChunks_; ++chunkIdx) {
        (*chunkToPageListHeadIdxMapBuilder)[chunkIdx] =
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

void ListsMetadataBuilder::populatePageIdsInAPageList(uint32_t numPages, uint32_t startPageId) {
    // calculate the number of pageListGroups to accommodate the pageList completely.
    auto numPageListGroups = numPages / ListsMetadataConstants::PAGE_LIST_GROUP_SIZE;
    if (0 != numPages % ListsMetadataConstants::PAGE_LIST_GROUP_SIZE) {
        numPageListGroups++;
    }
    // During the initial allocation, we allocate all the pageListGroups of a pageList
    // contiguously. pageListTailIdx is the id in the pageLists blob where the pageList ends and
    // the next pageLists should start.
    uint32_t pageListHeadIdx = pageListsBuilder->header.numElements;
    auto pageListTailIdx =
        pageListHeadIdx +
        ((ListsMetadataConstants::PAGE_LIST_GROUP_WITH_NEXT_PTR_SIZE)*numPageListGroups);
    pageListsBuilder->resize(pageListTailIdx, false /* setToZero */);
    for (auto i = 0u; i < numPageListGroups; i++) {
        auto numPagesInThisGroup = std::min(ListsMetadataConstants::PAGE_LIST_GROUP_SIZE, numPages);
        for (auto j = 0u; j < numPagesInThisGroup; j++) {
            (*pageListsBuilder)[pageListHeadIdx + j] = startPageId++;
        }
        // if there are empty spots in the pageListGroup, put StorageConstants::NULL_PAGE_IDX in
        // them.
        for (auto j = numPagesInThisGroup; j < ListsMetadataConstants::PAGE_LIST_GROUP_SIZE; j++) {
            (*pageListsBuilder)[pageListHeadIdx + j] = StorageStructureUtils::NULL_PAGE_IDX;
        }
        numPages -= numPagesInThisGroup;
        pageListHeadIdx += ListsMetadataConstants::PAGE_LIST_GROUP_SIZE;
        // store the id to the next pageListGroup, if exists, otherwise store
        // StorageConstants::NULL_PAGE_IDX.
        (*pageListsBuilder)[pageListHeadIdx] =
            (0 == numPages) ? StorageStructureUtils::NULL_PAGE_IDX : 1 + pageListHeadIdx;
        pageListHeadIdx++;
    }
}
} // namespace storage
} // namespace kuzu

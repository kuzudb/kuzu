#pragma once

#include "storage/storage_structure/disk_array.h"
#include "storage/storage_structure/storage_structure.h"

namespace spdlog {
class logger;
}

namespace kuzu {
namespace storage {

class BaseListsMetadata {

public:
    static constexpr uint64_t CHUNK_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX = 0;
    static constexpr uint64_t LARGE_LIST_IDX_TO_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX = 1;
    static constexpr uint64_t CHUNK_PAGE_LIST_HEADER_PAGE_IDX = 2;

    explicit BaseListsMetadata() {
        logger = common::LoggerUtils::getLogger(common::LoggerConstants::LoggerEnum::STORAGE);
    }

    inline static std::function<uint32_t(uint32_t)> getIdxInPageListToListPageIdxMapper(
        BaseInMemDiskArray<common::page_idx_t>* pageLists, uint32_t pageListsHead) {
        return std::bind(getPageIdxFromAPageList, pageLists, pageListsHead, std::placeholders::_1);
    }

    static uint64_t getPageIdxFromAPageList(BaseInMemDiskArray<common::page_idx_t>* pageLists,
        uint32_t pageListHead, uint32_t idxInPageList);

protected:
    std::shared_ptr<spdlog::logger> logger;
};

class ListsMetadata : public BaseListsMetadata {
    friend class ListsUpdateIterator;

public:
    explicit ListsMetadata(const StorageStructureIDAndFName& storageStructureIDAndFNameForBaseList,
        BufferManager* bufferManager, WAL* wal);

    inline uint64_t getNumElementsInLargeLists(uint64_t largeListIdx) const {
        return (*largeListIdxToPageListHeadIdxMap)[(2 * largeListIdx) + 1];
    };
    // Returns a function that can map the logical pageIdx of a chunk ito its corresponding physical
    // pageIdx in a disk file.
    inline std::function<uint32_t(uint32_t)> getPageMapperForChunkIdx(uint32_t chunkIdx) {
        return getIdxInPageListToListPageIdxMapper(
            pageLists.get(), (*chunkToPageListHeadIdxMap)[chunkIdx]);
    }
    // Returns a function that can map the logical pageIdx of a largeList to its corresponding
    // physical pageIdx in a disk file.
    inline std::function<uint32_t(uint32_t)> getPageMapperForLargeListIdx(uint32_t largeListIdx) {
        return getIdxInPageListToListPageIdxMapper(
            pageLists.get(), (*largeListIdxToPageListHeadIdxMap)[2 * largeListIdx]);
    }

    inline void checkpointInMemoryIfNecessary() {
        chunkToPageListHeadIdxMap->checkpointInMemoryIfNecessary();
        largeListIdxToPageListHeadIdxMap->checkpointInMemoryIfNecessary();
        pageLists->checkpointInMemoryIfNecessary();
    }

    inline void rollbackInMemoryIfNecessary() {
        chunkToPageListHeadIdxMap->rollbackInMemoryIfNecessary();
        largeListIdxToPageListHeadIdxMap->rollbackInMemoryIfNecessary();
        pageLists->rollbackInMemoryIfNecessary();
    }

    inline VersionedFileHandle* getFileHandle() { return metadataVersionedFileHandle.get(); }

private:
    std::unique_ptr<VersionedFileHandle> metadataVersionedFileHandle;
    StorageStructureIDAndFName storageStructureIDAndFName;
    // chunkToPageListHeadIdxMapBuilder holds pointers to the head of pageList of each chunk.
    // For instance, chunkToPageListHeadIdxMapBuilder[3] is a pointer in `pageLists` from where
    // the pageList of chunk 3 begins.
    std::unique_ptr<InMemDiskArray<uint32_t>> chunkToPageListHeadIdxMap;
    // largeListIdxToPageListHeadIdxMapBuilder is similar to chunkToPageListHeadIdxMapBuilder
    // in the sense that it too holds pointers to the head of pageList of each large list. Along
    // with the pointer, this also stores the size of the large list adjacent to the pointer. For
    // instance, the pointer of the head of pageList for largeList 3 is at
    // largeListIdxToPageListHeadIdxMapBuilder[2
    // * 3] and the size is at largeListIdxToPageListHeadIdxMapBuilder[(2 * 3) + 1].
    std::unique_ptr<InMemDiskArray<uint32_t>> largeListIdxToPageListHeadIdxMap;
    // pageLists is a blob that contains the pageList of each chunk. Each pageList is broken
    // into smaller list group of size PAGE_LIST_GROUP_SIZE and chained together. List group may
    // not be contiguous and requires pointer chasing to read the required physical pageId from the
    // list. pageLists is used both to store the list of pages for large lists as well as small
    // lists of each chunk.
    std::unique_ptr<InMemDiskArray<common::page_idx_t>> pageLists;
};

/**
 * Lists Metadata holds the information necessary to locate a list in the collection of disk pages
 * that organizes and stores {@class Lists} data structure. Each object of Lists requires
 * ListsMetadata object that helps in locating.
 *
 * The mapper functions (called, IdxInPageListToListPageIdxMaper) of this class maps the idxs in
 * page lists  to the actual physical location in disk pages. These are the
 * ``logicalToPhysicalPageMapper'' mappers used in the BaseColumnOrList common functions for reading
 * data from disk for columns and lists.
 */
class ListsMetadataBuilder : public BaseListsMetadata {

public:
    explicit ListsMetadataBuilder(const std::string& listBaseFName);

    inline uint64_t getNumElementsInLargeLists(uint64_t largeListIdx) {
        return (*largeListIdxToPageListHeadIdxMapBuilder)[(2 * largeListIdx) + 1];
    };

    // Returns a function that can map the logical pageIdx of a chunk ito its corresponding physical
    // pageIdx in a disk file.
    inline std::function<uint32_t(uint32_t)> getPageMapperForChunkIdx(uint32_t chunkIdx) {
        return getIdxInPageListToListPageIdxMapper(
            pageListsBuilder.get(), (*chunkToPageListHeadIdxMapBuilder)[chunkIdx]);
    }
    // Returns a function that can map the logical pageIdx of a largeList to its corresponding
    // physical pageIdx in a disk file.
    inline std::function<uint32_t(uint32_t)> getPageMapperForLargeListIdx(uint32_t largeListIdx) {
        return getIdxInPageListToListPageIdxMapper(
            pageListsBuilder.get(), (*largeListIdxToPageListHeadIdxMapBuilder)[2 * largeListIdx]);
    }
    void initChunkPageLists(uint32_t numChunks);
    void initLargeListPageLists(uint32_t numLargeLists);

    // To be called only after call to initChunkPageLists(...). Assumes pageList of chunks 0 to
    // chunkId - 1 has already been populated.
    void populateChunkPageList(uint32_t chunkId, uint32_t numPages, uint32_t startPageId);
    // To be called only after call to initLargeListPageLists(...). Assumes pageList of largeLists 0
    // to largeListIdx - 1 has already been populated.
    void populateLargeListPageList(
        uint32_t largeListIdx, uint32_t numPages, uint32_t numElements, uint32_t startPageId);

    void saveToDisk();

private:
    // Creates a new pageList (in pageListGroups of size 3) by enumerating the pageIds sequentially
    // in the list, starting from `startPageId` till `startPageId + numPages - 1`.
    void populatePageIdsInAPageList(uint32_t numPages, uint32_t startPageId);

private:
    std::unique_ptr<FileHandle> metadataFileHandleForBuilding;
    std::unique_ptr<InMemDiskArrayBuilder<uint32_t>> chunkToPageListHeadIdxMapBuilder;
    std::unique_ptr<InMemDiskArrayBuilder<uint32_t>> largeListIdxToPageListHeadIdxMapBuilder;
    std::unique_ptr<InMemDiskArrayBuilder<common::page_idx_t>> pageListsBuilder;
};

} // namespace storage
} // namespace kuzu

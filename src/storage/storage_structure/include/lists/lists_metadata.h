#pragma once

#include "src/storage/storage_structure/include/disk_array.h"
#include "src/storage/storage_structure/include/storage_structure.h"

using namespace std;
using namespace graphflow::common;

namespace spdlog {
class logger;
}

namespace graphflow {
namespace loader {
class LoaderEmptyListsTest;
} // namespace loader
} // namespace graphflow

namespace graphflow {
namespace storage {

/**
 * Lists Metadata holds the information necessary to locate a list in the collection of disk pages
 * that organizes and stores {@class Lists} data structure. Each object of Lists requires
 * ListsMetadata object that helps in locating.
 *
 * The Mapper functions (called, logicalToPhysicalPageMappers) of this class maps the logical
 * location of the list to the actual physical location in disk pages.
 */
class ListsMetadata {
    friend class graphflow::loader::LoaderEmptyListsTest;

    static constexpr uint64_t CHUNK_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX = 0;
    static constexpr uint64_t LARGE_LIST_IDX_TO_PAGE_LIST_HEAD_IDX_MAP_HEADER_PAGE_IDX = 1;
    static constexpr uint64_t CHUNK_PAGE_LIST_HEADER_PAGE_IDX = 2;

public:
    explicit ListsMetadata(const string& listBaseFName, bool isForBuilding);

    inline uint64_t getNumElementsInLargeLists(uint64_t largeListIdx) {
        return (*largeListIdxToPageListHeadIdxMap)[(2 * largeListIdx) + 1];
    };

    // Returns a function that can map the logical pageIdx of a chunk ito its corresponding physical
    // pageIdx in a disk file.
    inline std::function<uint32_t(uint32_t)> getPageMapperForChunkIdx(uint32_t chunkIdx) {
        return getLogicalToPhysicalPageMapper(
            pageLists.get(), (*chunkToPageListHeadIdxMap)[chunkIdx]);
    }
    // Returns a function that can map the logical pageIdx of a largeList to its corresponding
    // physical pageIdx in a disk file.
    inline std::function<uint32_t(uint32_t)> getPageMapperForLargeListIdx(uint32_t largeListIdx) {
        return getLogicalToPhysicalPageMapper(
            pageLists.get(), (*largeListIdxToPageListHeadIdxMap)[2 * largeListIdx]);
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
    void readFromDisk(const string& fName);

    inline static std::function<uint32_t(uint32_t)> getLogicalToPhysicalPageMapper(
        InMemDiskArray<page_idx_t>* pageLists, uint32_t pageListsHead) {
        return bind(getPageIdxFromAPageList, pageLists, pageListsHead, placeholders::_1);
    }

    static uint64_t getPageIdxFromAPageList(
        InMemDiskArray<page_idx_t>* pageLists, uint32_t pageListHead, uint32_t logicalPageIdx);

    // Below functions are to be used only in the loader to create the ListsMetadata object
    // initially.

    // Creates a new pageList (in pageListGroups of size 3) by enumerating the pageIds sequentially
    // in the list, starting from `startPageId` till `startPageId + numPages - 1`.
    void populatePageIdsInAPageList(uint32_t numPages, uint32_t startPageId);

public:
    // All pageLists (defined later) are broken in pieces (called a pageListGroups) of size
    // PAGE_LIST_GROUP_SIZE + 1 each and chained together. In each piece, there are
    // PAGE_LIST_GROUP_SIZE elements of that list and the offset to the next pageListGroups in the
    // blob that contains all pageListGroups of all lists.
    static constexpr uint32_t PAGE_LIST_GROUP_SIZE = 3;
    static constexpr uint32_t PAGE_LIST_GROUP_WITH_NEXT_PTR_SIZE = PAGE_LIST_GROUP_SIZE + 1;

private:
    shared_ptr<spdlog::logger> logger;
    unique_ptr<FileHandle> metadataFileHandle;
    // chunkToPageListHeadIdxMap holds pointers to the head of pageList of each chunk. For instance,
    // chunkToPageListHeadIdxMap[3] is a pointer in `pageLists` from where the pageList of
    // chunk 3 begins.
    unique_ptr<InMemDiskArray<uint32_t>> chunkToPageListHeadIdxMap;
    // largeListIdxToPageListHeadIdxMap is similar to chunkToPageListHeadIdxMap in the sense that it
    // too holds pointers to the head of pageList of each large list. Along with the pointer, this
    // also stores the size of the large list adjacent to the pointer. For instance, the pointer of
    // the head of pageList for largeList 3 is at largeListIdxToPageListHeadIdxMap[2
    // * 3] and the size is at largeListIdxToPageListHeadIdxMap[(2 * 3) + 1].
    unique_ptr<InMemDiskArray<uint32_t>> largeListIdxToPageListHeadIdxMap;
    // pageLists is a blob that contains the pageList of each chunk. Each pageList is broken
    // into smaller list group of size PAGE_LIST_GROUP_SIZE and chained together. List group may
    // not be contiguous and requires pointer chasing to read the required physical pageId from the
    // list. pageLists is used both to store the list of pages for large lists as well as small
    // lists of each chunk.
    unique_ptr<InMemDiskArray<page_idx_t>> pageLists;
};

} // namespace storage
} // namespace graphflow

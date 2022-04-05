#pragma once

#include "src/storage/include/storage_structure/storage_structure.h"

using namespace std;
using namespace graphflow::common;

namespace spdlog {
class logger;
}

namespace graphflow {
namespace loader {

class ListsUtils;
class InMemAdjAndPropertyListsBuilder;
class NodesLoader;

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
    friend class graphflow::loader::ListsUtils;
    friend class graphflow::loader::InMemAdjAndPropertyListsBuilder;
    friend class graphflow::loader::NodesLoader;

public:
    ListsMetadata();
    explicit ListsMetadata(const string& listBaseFName);

    inline uint64_t getNumElementsInLargeLists(uint64_t largeListIdx) {
        return largeListIdxToPageListHeadIdxMap[(2 * largeListIdx) + 1];
    };

    // Returns a function that can map the logical pageIdx of a chunk ito its corresponding physical
    // pageIdx in a disk file.
    inline std::function<uint32_t(uint32_t)> getPageMapperForChunkIdx(uint32_t chunkIdx) {
        return getLogicalToPhysicalPageMapper(
            chunkPageLists.get(), chunkToPageListHeadIdxMap[chunkIdx], chunkIdx);
    }

    // Returns a function that can map the logical pageIdx of a largeList to its corresponding
    // physical pageIdx in a disk file.
    inline std::function<uint32_t(uint32_t)> getPageMapperForLargeListIdx(uint32_t largeListIdx) {
        return getLogicalToPhysicalPageMapper(largeListPageLists.get(),
            largeListIdxToPageListHeadIdxMap[2 * largeListIdx], largeListIdx);
    }

private:
    void saveToDisk(const string& fName);
    void readFromDisk(const string& fName);

    inline static std::function<uint32_t(uint32_t)> getLogicalToPhysicalPageMapper(
        uint32_t* pageLists, uint32_t pageListsHead, uint32_t pageIdx) {
        return bind(getPageIdxFromAPageList, pageLists, pageListsHead, placeholders::_1);
    }

    static uint64_t getPageIdxFromAPageList(
        uint32_t* pageLists, uint32_t pageListHead, uint32_t pageIdx);

    // Below functions are to be used only in the loader to create the ListsMetadata object
    // initially.

    void initChunkPageLists(uint32_t numChunks);
    void initLargeListPageLists(uint32_t numLargeLists);

    // To be called only after call to initChunkPageLists(...). Assumes pageList of chunks 0 to
    // chunkId - 1 has already been populated.
    void populateChunkPageList(uint32_t chunkId, uint32_t numPages, uint32_t startPageId);
    // To be called only after call to initLargeListPageLists(...). Assumes pageList of largeLists 0
    // to largeListIdx - 1 has already been populated.
    void populateLargeListPageList(
        uint32_t largeListIdx, uint32_t numPages, uint32_t numElements, uint32_t startPageId);

    // Creates a new pageList (in pageListGroups of size 3) by enumerating the pageIds sequentially
    // in the list, starting from `startPageId` till `startPageId + numPages - 1`.
    static uint32_t enumeratePageIdsInAPageList(unique_ptr<uint32_t[]>& pageLists,
        uint64_t& pageListsCapacity, uint32_t pageListHead, uint32_t numPages,
        uint32_t startPageId);

    static void increasePageListsCapacityIfNeeded(
        unique_ptr<uint32_t[]>& pageLists, uint64_t& pageListsCapacity, uint32_t requiredCapacity);

public:
    // All pageLists (defined later) are broken in pieces (called a pageListGroups) of size
    // PAGE_LIST_GROUP_SIZE + 1 each and chained together. In each piece, there are
    // PAGE_LIST_GROUP_SIZE elements of that list and the offset to the next pageListGroups in the
    // blob that contains all pageListGroups of all lists.
    static constexpr uint32_t PAGE_LIST_GROUP_SIZE = 3;

    // Metadata file suffixes
    static constexpr char CHUNK_PAGE_LIST_HEAD_IDX_MAP_SUFFIX[] = ".chunks.f1";
    static constexpr char CHUNK_PAGE_LISTS_SUFFIX[] = ".chunks.f2";
    static constexpr char LARGE_LISTS_PAGE_LIST_HEAD_IDX_MAP_SUFFIX[] = ".largeLists.f1";
    static constexpr char LARGE_LISTS_PAGE_LISTS_SUFFIX[] = ".largeLists.f2";

private:
    shared_ptr<spdlog::logger> logger;

    // A chunk's pageList is a list of indexes of physical pages on disk allocated for that chunk.
    // The physical pages in the chunk holds lists of nodes in that chunk. Pages may not be
    // contiguous on disk.
    uint32_t numChunks;
    // chunkToPageListHeadIdxMap holds pointers to the head of pageList of each chunk. For instance,
    // chunkToPageListHeadIdxMap[3] is a pointer in `chunkPageLists` from where the pageList of
    // chunk 3 begins.
    unique_ptr<uint32_t[]> chunkToPageListHeadIdxMap;
    // chunkPageLists is a blob that contains the pageList of each chunk. Each pageList is broken
    // into smaller list group of size PAGE_LIST_GROUP_SIZE and chained together. List group may
    // not be contiguous and requires pointer chasing to read the required physical pageId from the
    // list.
    unique_ptr<uint32_t[]> chunkPageLists;
    // chunkPageListsCapacity holds the total space available to chunkPageLists, irrespective of the
    // space in actual use.
    uint64_t chunkPageListsCapacity;

    // A largeList's pageList is a list of IDs of physical pages on disk allocated for storing data
    // of that large list.
    uint32_t numLargeLists;
    // largeListIdxToPageListHeadIdxMap is similar to chunkToPageListHeadIdxMap in the sense that it
    // too holds pointers to the head of pageList of each large list. Along with the pointer, this
    // also stores the size of the large list adjacent to the pointer. For instance, the pointer of
    // the head of pageList for largeList 3 is at largeListIdxToPageListHeadIdxMap[2
    // * 3] and the size is at largeListIdxToPageListHeadIdxMap[(2 * 3) + 1].
    unique_ptr<uint32_t[]> largeListIdxToPageListHeadIdxMap;
    // largeListPageLists has use and organization identical to chunkPageLists.
    unique_ptr<uint32_t[]> largeListPageLists;
    uint64_t largeListPageListsCapacity;

    // Total number of pages that is used to store the Lists data. This is the sum of number of
    // pages used by all chunks and large lists.
    uint32_t numPages;
};

} // namespace storage
} // namespace graphflow

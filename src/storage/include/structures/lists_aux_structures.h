#pragma once

#include <string>
#include <vector>

#include "spdlog/spdlog.h"

#include "src/common/include/types.h"
#include "src/storage/include/structures/common.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace loader {

class ListsLoaderHelper;
class AdjAndPropertyListsBuilder;
class NodesLoader;

} // namespace loader
} // namespace graphflow

namespace graphflow {
namespace storage {

// Lists Metadata holds the information necessary to locate a list in the collection of disk pages
// thats organizes and stores lists.
class ListsMetadata {
    friend class graphflow::loader::ListsLoaderHelper;
    friend class graphflow::loader::AdjAndPropertyListsBuilder;
    friend class graphflow::loader::NodesLoader;

public:
    ListsMetadata() : logger{spdlog::get("storage")} {};
    ListsMetadata(const string& path);

    inline uint64_t getNumElementsInLargeLists(uint64_t largeListIdx) {
        return largeListIdxToPageListHeadIdxMap[(2 * largeListIdx) + 1];
    };

    uint64_t getPageIdxFromAChunkPageList(uint64_t chunkIdx, uint32_t pageIdx) {
        return getPageIdxFromAPageList(
            chunkPageLists, chunkToPageListHeadIdxMap[chunkIdx], pageIdx);
    }

    uint64_t getPageIdxFromALargeListPageList(uint64_t largeListIdx, uint32_t pageIdx) {
        return getPageIdxFromAPageList(
            largeListPageLists, largeListIdxToPageListHeadIdxMap[2 * largeListIdx], pageIdx);
    }

    unique_ptr<LogicalToPhysicalPageIdxMapper> getPageMapperForChunkIdx(uint32_t chunkIdx) {
        return make_unique<LogicalToPhysicalPageIdxMapper>(
            chunkPageLists, chunkToPageListHeadIdxMap[chunkIdx]);
    }

    unique_ptr<LogicalToPhysicalPageIdxMapper> getPageMapperForLargeListIdx(uint32_t largeListIdx) {
        return make_unique<LogicalToPhysicalPageIdxMapper>(
            largeListPageLists, largeListIdxToPageListHeadIdxMap[2 * largeListIdx]);
    }

private:
    void saveToDisk(const string& fname);
    void readFromDisk(const string& fname);

    uint64_t getPageIdxFromAPageList(
        unique_ptr<uint32_t[]>& pageLists, uint32_t pageListHead, uint32_t pageIdx);

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

    // Creates a new pageList (in pageListGropus of size 3) by enumerating the pageIds sequentially
    // in the list, starting from `startPageId` till `startPageId + numPages - 1`.
    uint32_t enumeratePageIdsInAPageList(unique_ptr<uint32_t[]>& pageLists,
        uint64_t& pageListsCapacity, uint32_t pageListHead, uint32_t numPages,
        uint32_t startPageId);

    void increasePageListsCapacityIfNeeded(
        unique_ptr<uint32_t[]>& pageLists, uint64_t& pageListsCapacity, uint32_t requiredCapacity);

private:
    shared_ptr<spdlog::logger> logger;

    // All lists (pageList, defined later) are broken in pieces (called a pageListGropus) of size 4
    // each and chained together. In each piece, there are 3 elements of that list and the offset to
    // the next pageListGropus in the blob that contains all pageListGropus of all lists.
    static constexpr uint32_t PAGE_LIST_GROUP_SIZE = 3;

    // Metadata file suffixes
    inline static const string CHUNK_PAGE_LIST_HEAD_IDX_MAP_SUFFIX = ".chunks.f1";
    inline static const string CHUNK_PAGE_LISTS_SUFFIX = ".chunks.f2";
    inline static const string LARGE_LISTS_PAGE_LIST_HEAD_IDX_MAP_SUFFIX = ".largeLists.f1";
    inline static const string LARGE_LISTS_PAGE_LISTS_SUFFIX = ".largeLists.f2";

    // A chunk's pageList is a list of IDs of physical pages on disk allocated for that chunk. The
    // physical pages holds the data of lists in that chunk and may or may not be contiguous on
    // disk.
    uint32_t numChunks;
    // chunkToPageListHeadIdxMap holds pointers to the head of pageList of each chunk. For instance,
    // chunkToPageListHeadIdxMap[3] is a pointer in `chunkPageLists` from where the pageList of
    // chunk 3 begins.
    unique_ptr<uint32_t[]> chunkToPageListHeadIdxMap;
    // chunkPageLists is a blob that contains the pageList of each chunk. Each pageList is broken
    // into smaller list group of size 3 and chained together. List group may not be contiguous and
    // requires pointer chasing to read the required physical pageId from the list.
    unique_ptr<uint32_t[]> chunkPageLists;
    // chunkPageListsCapacity holds the total space available to chunkPageLists, irrespective of the
    // space in actaul use.
    uint64_t chunkPageListsCapacity = 0;

    // A largeList's pageList is a list of IDs of physical pages on disk allocated for storing data
    // of that large list.
    uint32_t numLargeLists;
    // largeListIdxToPageListHeadIdxMap is similar to chunkToPageListHeadIdxMap in the sense that it
    // too holds pointers to the head of pageLiss of each large list. Along with the pointer, this
    // also stores the size of the large list adjacent to the pointer. For instance, the pointer of
    // the head of pageList for largeList 3 is at largeListIdxToPageListHeadIdxMap[2
    // * 3] and the size is at largeListIdxToPageListHeadIdxMap[(2 * 3) + 1].
    unique_ptr<uint32_t[]> largeListIdxToPageListHeadIdxMap;
    // largeListPageLists has use and organization identical to chunkPageLists.
    unique_ptr<uint32_t[]> largeListPageLists;
    uint64_t largeListPageListsCapacity = 0;

    // Total number of pages that is used to store the Lists data. This is the sum of number of
    // pages used by all chunks and large lists.
    uint32_t numPages;
};

// ListHeaders holds the headers of all lists in a single Lists structue.
class ListHeaders {
    friend class graphflow::loader::ListsLoaderHelper;
    friend class graphflow::loader::AdjAndPropertyListsBuilder;
    friend class graphflow::loader::NodesLoader;

public:
    ListHeaders() : logger{spdlog::get("storage")} {};
    ListHeaders(string path);

    uint32_t getHeader(node_offset_t offset) { return headers[offset]; };

    static inline bool isALargeList(const uint32_t& header) { return header & 0x80000000; };

    // For small lists.
    static inline uint32_t getSmallListLen(const uint32_t& header) { return header & 0x7ff; };
    static inline uint32_t getSmallListCSROffset(const uint32_t& header) {
        return header >> 11 & 0xfffff;
    };

    // For large lists.
    static inline uint32_t getLargeListIdx(const uint32_t& header) { return header & 0x7fffffff; };

private:
    void init(uint32_t size);

    void saveToDisk(const string& fname);
    void readFromDisk(const string& fname);

private:
    shared_ptr<spdlog::logger> logger;
    // A header of a list is a uint32_t value the describes the following information about the
    // list: 1) type: small or large, 2) location of list in pages.

    // If the list is small, the layout of header is:
    //      1. MSB (31st bit) is reset
    //      2. 30-22 bits denote the index in the chunkPagesMaps[chunkId] where disk page ID is
    //      located, where chunkId = idx of the list / LISTS_CHUNK_SIZE.
    //      3. 21-11 bits denote the offset in the disk page.
    //      4. 10-0  bits denote the number of elements in the list.

    // If list is a large one (during initial data ingest, the criterion for being large is
    // whether or not a list fits in a single page), the layout of header is:
    //      1. MSB (31st bit) is set
    //      2. 30-0  bits denote the idx in the largeListsPagesMaps where the length and the disk
    //      page IDs are located.
    unique_ptr<uint32_t[]> headers;
    uint32_t size;
};

void saveListOfIntsToFile(const string& path, unique_ptr<uint32_t[]>& data, uint32_t listSize);

uint32_t readListOfIntsFromFile(unique_ptr<uint32_t[]>& data, const string& path);

} // namespace storage
} // namespace graphflow

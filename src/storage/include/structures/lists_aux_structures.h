#pragma once

#include <string>
#include <vector>

#include "bitsery/bitsery.h"
#include "spdlog/spdlog.h"

#include "src/common/include/types.h"
#include "src/storage/include/structures/common.h"

using namespace std;
using namespace graphflow::common;

namespace graphflow {
namespace loader {

class AdjAndPropertyListsLoaderHelper;

} // namespace loader
} // namespace graphflow

namespace graphflow {
namespace storage {

// Lists Metadata holds the information necessary to locate a list in the collection of disk pages
// thats organizes and stores lists.
class ListsMetadata {
    friend class graphflow::loader::AdjAndPropertyListsLoaderHelper;
    friend class bitsery::Access;

public:
    ListsMetadata() : logger{spdlog::get("storage")} {};
    ListsMetadata(const string& path);

    inline uint64_t getNumElementsInLargeLists(uint64_t largeListIdx) {
        return largeListsPagesMap[largeListIdx][0];
    };

    uint64_t getPageIdxFromLargeListPagesMap(uint64_t largeListIdx, uint32_t pageIdx) {
        return largeListsPagesMap[largeListIdx][pageIdx + 1];
    }

    uint64_t getPageIdxFromChunkPagesMap(uint64_t chunkIdx, uint32_t pageIdx) {
        return chunksPagesMap[chunkIdx][pageIdx];
    }

    unique_ptr<LogicalToPhysicalPageIdxMapper> getPageMapperForChunkIdx(uint32_t chunckIdx) {
        return make_unique<LogicalToPhysicalPageIdxMapper>(chunksPagesMap[chunckIdx], 0);
    }

    unique_ptr<LogicalToPhysicalPageIdxMapper> getPageMapperForLargeListIdx(uint32_t largeListIdx) {
        return make_unique<LogicalToPhysicalPageIdxMapper>(largeListsPagesMap[largeListIdx], 1);
    }

private:
    template<typename S>
    void serialize(S& s);

    void saveToFile(const string& fname);
    void readFromFile(const string& fname);

private:
    shared_ptr<spdlog::logger> logger;
    // Holds the list of alloted disk page IDs for each chunk (that is a collection of regular
    // adjlists for 512 node offsets). The outer vector holds one vector per chunk, which holds the
    // pageIdxs of the pages used to hold the small adjlists of LISTS_CHUNK_SIZE number of
    // vertices in one chunk.
    vector<vector<uint64_t>> chunksPagesMap;
    // Holds the list of alloted disk page IDs for the corresponding large lists.
    vector<vector<uint64_t>> largeListsPagesMap;
    // total number of pages required for oraganizing and storing the lists.
    uint64_t numPages;
};

// AdjListHeaders holds the headers of all lists in a single AdjLists structue.
class AdjListHeaders {
    friend class graphflow::loader::AdjAndPropertyListsLoaderHelper;
    friend class bitsery::Access;

public:
    AdjListHeaders() : logger{spdlog::get("storage")} {};
    AdjListHeaders(string path);

    uint32_t getHeader(node_offset_t offset) { return headers[offset]; };

    static inline bool isALargeAdjList(const uint32_t& header) { return header & 0x80000000; };

    // For small adjList.
    static inline uint32_t getAdjListLen(const uint32_t& header) { return header & 0x7ff; };
    static inline uint32_t getCSROffset(const uint32_t& header) { return header >> 11 & 0xfffff; };

    // For large adjList.
    static inline uint32_t getLargeAdjListIdx(const uint32_t& header) {
        return header & 0x7fffffff;
    };

private:
    template<typename S>
    void serialize(S& s);

    void saveToFile(const string& fname);
    void readFromFile(const string& fname);

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
    vector<uint32_t> headers;
};

} // namespace storage
} // namespace graphflow

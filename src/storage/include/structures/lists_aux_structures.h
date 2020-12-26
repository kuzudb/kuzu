#pragma once

#include <string>
#include <vector>

#include "bitsery/bitsery.h"

#include "src/common/include/types.h"

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
// thats organizes and stores those lists.
class ListsMetadata {
    friend class graphflow::loader::AdjAndPropertyListsLoaderHelper;
    friend class bitsery::Access;

public:
    ListsMetadata() = default;
    ListsMetadata(const string& path) { readFromFile(path); };

    inline uint64_t getPageIdx(const uint32_t& chunkIDx, const uint32_t& pageIdxInChunk) {
        return chunksPagesMap[chunkIDx][pageIdxInChunk];
    };

private:
    template<typename S>
    void serialize(S& s);

    void saveToFile(const string& fname);
    void readFromFile(const string& fname);

private:
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
    AdjListHeaders() = default;
    AdjListHeaders(string path) { readFromFile(path); };

    uint32_t getHeader(node_offset_t offset) { return headers[offset]; };

    static inline bool isALargeAdjList(const uint32_t& header) { return header & 0x80000000; };

    // These functions returns the expected value only if the header is of a small adjLists.
    static inline uint32_t getAdjListLen(const uint32_t& header) { return header & 0x7ff; };
    static inline uint32_t getCSROffset(const uint32_t& header) { return header >> 11; };

private:
    template<typename S>
    void serialize(S& s);

    void saveToFile(const string& fname);
    void readFromFile(const string& fname);

private:
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

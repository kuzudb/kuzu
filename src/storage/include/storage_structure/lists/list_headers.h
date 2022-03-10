#pragma once

#include <memory>

#include "src/common/types/include/types_include.h"

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
 * ListHeaders holds the headers of all lists in a single Lists data structure.
 *
 * A header of a list is a unsigned integer values. Each value describes the following
 * information about the list: 1) type: small or large, 2) location of list in pages.
 *
 * If the list is small, the layout of header is:
 *      1. MSB (31st bit) is reset
 *      2. 30-22 bits denote the index in the chunkPagesMaps[chunkId] where disk page ID is
 *      located, where chunkId = idx of the list / LISTS_CHUNK_SIZE.
 *      3. 21-11 bits denote the offset in the disk page.
 *      4. 10-0  bits denote the number of elements in the list.
 *
 * If list is a large one (during initial data ingest, the criterion for being large is
 * whether or not a list fits in a single page), the layout of header is:
 *      1. MSB (31st bit) is set
 *      2. 30-0  bits denote the idx in the largeListsPagesMaps where the length and the disk
 *      page IDs are located.
 * */
class ListHeaders {
    friend class graphflow::loader::ListsUtils;
    friend class graphflow::loader::InMemAdjAndPropertyListsBuilder;
    friend class graphflow::loader::NodesLoader;

public:
    ListHeaders();
    ListHeaders(const string& listBaseFName);

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

    void saveToDisk(const string& fName);
    void readFromDisk(const string& fName);

private:
    shared_ptr<spdlog::logger> logger;

    unique_ptr<uint32_t[]> headers;
    uint32_t size;
};

} // namespace storage
} // namespace graphflow

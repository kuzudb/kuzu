#pragma once

#include <memory>

#include "src/common/types/include/types_include.h"
#include "src/storage/storage_structure/include/disk_array.h"

using namespace std;
using namespace graphflow::common;

namespace spdlog {
class logger;
}

namespace graphflow {
namespace storage {

/**
 * ListHeaders holds the headers of all lists in a single Lists data structure, which implements a
 * chunked CSR, where each chunk stores StorageConfig::LISTS_CHUNK_SIZE many lists.
 *
 * A header of a list is a unsigned integer values. Each value describes the following
 * information about the list: 1) type: small or large, 2) location of list in pages.
 *
 * If the list is small, the layout of header is:
 *      1. MSB (31st bit) is 0
 *      2. 30-11 bits denote the "logical" CSR offset in the chunk. Using Lists::numElementsPerPage
 *      (which is stored in the base class StorageStructure), this offset can be turned into a
 *      logical pageIdx, which can later be turned into a physical page index in the .lists file.
 *      This logic is encapsulated in Lists::getListInfo(node_offset_t nodeOffset) function.
 *      4. 10-0  bits denote the number of elements in the list.
 *
 * If list is a large one (during initial data ingest, the criterion for being large is
 * whether or not a list fits in a single page), the layout of header is:
 *      1. MSB (31st bit) is 1
 *      2. 30-0  bits denote the idx in the largeListsPagesMaps where the length and the disk
 *      page IDs are located.
 * */
class ListHeaders {

public:
    static constexpr uint64_t LIST_HEADERS_HEADER_PAGE_IDX = 0;

    explicit ListHeaders(const string& fName, uint64_t numElements);
    explicit ListHeaders(const string& listBaseFName);

    inline list_header_t getHeader(node_offset_t offset) { return (*headers)[offset]; };
    inline void setHeader(node_offset_t offset, list_header_t header) {
        (*headers)[offset] = header;
    }

    static inline bool isALargeList(list_header_t header) { return header & 0x80000000; };

    // For small lists.
    static inline uint32_t getSmallListLen(list_header_t header) { return header & 0x7ff; };
    static inline uint32_t getSmallListCSROffset(list_header_t header) {
        return header >> 11 & 0xfffff;
    };
    static inline uint32_t getSmallListHeader(uint32_t csrOffset, uint32_t numElementsInList) {
        return ((csrOffset & 0xfffff) << 11) | (numElementsInList & 0x7ff);
    }

    // For large lists.
    static inline uint32_t getLargeListIdx(list_header_t header) { return header & 0x7fffffff; };
    static inline uint32_t getLargeListHeader(uint32_t listIdx) {
        return 0x80000000 | (listIdx & 0x7fffffff);
    }

    void saveToDisk();

private:
    shared_ptr<spdlog::logger> logger;
    unique_ptr<InMemDiskArray<list_header_t>> headers;
    unique_ptr<FileHandle> headersFileHandle;
};

} // namespace storage
} // namespace graphflow

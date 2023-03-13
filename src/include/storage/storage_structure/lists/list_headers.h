#pragma once

#include <memory>

#include "common/types/types_include.h"
#include "storage/storage_structure/disk_array.h"

namespace spdlog {
class logger;
}

namespace kuzu {
namespace storage {

/**
 * ListHeaders holds the headers of all lists in a single Lists data structure, which implements a
 * chunked CSR, where each chunk stores ListsMetadataConstants::LISTS_CHUNK_SIZE many lists.
 *
 * A header of a list is a unsigned integer values. Each value describes the following
 * information about the list: 1) type: small or large, 2) location of the list in pages.
 *
 * If the list is small, the layout of header is:
 *      1. MSB (31st bit) is 0
 *      2. 30-11 bits denote the "logical" CSR offset in the chunk.
 *      3. 10-0 bits denote the number of elements in the list.
 * Using Lists::numElementsPerPage (which is stored in the base class StorageStructure), this offset
 * can be turned into a logical pageIdx (which we call idxInPageList), which can later be turned
 * into a physical page index (which we refer to as listPageIdx) in the .lists file. This is done as
 * follows: Consider a nodeOffset v that is in chunk c. We obtain the the beginning of the
 * "pageList" of the chunk by going to idxOfPageListBeginInPageLists =
 * chunkToPageListHeadIdxMap(c). PageLists are the list of physical page idx's in the .lists file
 * that store the contents of the lists in a chunk. Each chunk has a page list. Similarly each large
 * list has a pageList (see below for large lists)Now suppose the CSR offset of v is such
 * that it should be in the "2"nd page of the chunk. So the idxInPageList for v's list is 2. Then we
 * can go to pageLists[idxOfPageListBeginInPageLists+idxInPageList], which gives us the physical
 * pageIdx that contains v's list in the .lists file. We cannot always directly go to
 * pageLists[idxOfPageListBeginInPageLists+idxInPageList] because the page list of the chunk is
 * logically divided into "page groups" ListsMetadataConstants::PAGE_LIST_GROUP_SIZE with a pointer
 * to the next page group. That is, suppose PAGE_LIST_GROUP_SIZE = 3 and we need to find the
 * physical pageIdx of idxInPageList=5'th page. Then we need to follow one "pointer" at
 * idxOfNextPageGroupBeginInPageLists=pageLists[idxOfPageListBeginInPageLists+3] (+0, +1, and +2
 * store the physical page idx's for the first 3 pages and +3 stores the pointer to the beginning of
 * the next page group). Then we go to pageLists[idxOfNextPageGroupBeginInPageLists + (5-3=2)].
 *
 * If a list is a large one (during initial data ingest, the criterion for being large is
 * whether or not a list fits in a single page), the layout of the header is:
 *      1. MSB (31st bit) is 1
 *      2. 30-0  bits denote the largeListIdx in the largeListIdxToPageListHeadIdxMap where: (i)
 *      the location of the beginning of the first pageGroup for the large list is located; and (ii)
 *      the length of the list is located.
 * Each large list gets a largeListIdx, starting from 0, 1,.... Then through a similar process to
 * how we convert a idxInPageList to a physical page Idx, we can do the following to find the
 * physical page idxs of the pages of a large list. Suppose the largeListIdx = 7. Then
 * largeListIdxToPageListHeadIdxMap[2*7=14] gives the idxOfPageListBeginInPageLists for this large
 * list. Then pageLists[idxOfPageListBeginInPageLists+0],
 * pageLists[idxOfPageListBeginInPageLists+1], etc. will give the physical page idx's of the first,
 * second, etc. pages of the large list.
 */
class BaseListHeaders {

public:
    static constexpr uint64_t LIST_HEADERS_HEADER_PAGE_IDX = 0;

    explicit BaseListHeaders();

    static inline bool isALargeList(common::list_header_t header) { return header & 0x80000000; };

    // For small lists.
    static inline uint32_t getSmallListLen(common::list_header_t header) { return header & 0x7ff; };
    static inline uint32_t getSmallListCSROffset(common::list_header_t header) {
        return header >> 11 & 0xfffff;
    };
    static inline std::pair<uint32_t, uint32_t> getSmallListLenAndCSROffset(
        common::list_header_t header) {
        return std::make_pair(getSmallListLen(header), getSmallListCSROffset(header));
    }

    // Constructs a small list header for a list that starts at offset csrOffset and has
    // numElementsInList many elements.
    // Note: if the csrOffset is 0 and the list is empty the header is 0.
    static inline common::list_header_t getSmallListHeader(
        uint32_t csrOffset, uint32_t numElementsInList) {
        return ((csrOffset & 0xfffff) << 11) | (numElementsInList & 0x7ff);
    }

    // For large lists.
    static inline uint32_t getLargeListIdx(common::list_header_t header) {
        return header & 0x7fffffff;
    };
    static inline common::list_header_t getLargeListHeader(uint32_t listIdx) {
        return 0x80000000 | (listIdx & 0x7fffffff);
    }

protected:
    std::shared_ptr<spdlog::logger> logger;
};

class ListHeadersBuilder : public BaseListHeaders {
public:
    explicit ListHeadersBuilder(const std::string& baseListFName, uint64_t numElements);

    inline common::list_header_t getHeader(common::offset_t offset) {
        return (*headersBuilder)[offset];
    };

    inline void setHeader(common::offset_t offset, common::list_header_t header) {
        (*headersBuilder)[offset] = header;
    }
    void saveToDisk();

private:
    std::unique_ptr<FileHandle> fileHandle;
    std::unique_ptr<InMemDiskArrayBuilder<common::list_header_t>> headersBuilder;
};

class ListHeaders : public BaseListHeaders {

public:
    explicit ListHeaders(const StorageStructureIDAndFName& storageStructureIDAndFNameForBaseList,
        BufferManager* bufferManager, WAL* wal);

    inline common::list_header_t getHeader(common::offset_t offset) const {
        return (*headersDiskArray)[offset];
    };

    inline void checkpointInMemoryIfNecessary() const {
        headersDiskArray->checkpointInMemoryIfNecessary();
    }

    inline void rollbackInMemoryIfNecessary() const {
        headersDiskArray->rollbackInMemoryIfNecessary();
    }

public:
    std::unique_ptr<InMemDiskArray<common::list_header_t>> headersDiskArray;

private:
    std::unique_ptr<BMFileHandle> fileHandle;
    StorageStructureIDAndFName storageStructureIDAndFName;
};
} // namespace storage
} // namespace kuzu

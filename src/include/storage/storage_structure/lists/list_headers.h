#pragma once

#include <memory>

#include "common/types/types_include.h"
#include "storage/storage_structure/disk_array.h"

namespace kuzu {
namespace storage {

using csr_offset_t = uint32_t;

/**
 * ListHeaders holds the headers of all lists in a single Lists data structure, which implements a
 * chunked CSR, where each chunk stores ListsMetadataConstants::LISTS_CHUNK_SIZE many lists.
 *
 * A header of a list is a unsigned integer values. Each value records the start CSR offset of the
 * list inside the chunk, and the last value of each chunk records the length of the list within
 * the chunk.
 *
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
 */

static constexpr uint64_t LIST_HEADERS_HEADER_PAGE_IDX = 0;

class ListHeadersBuilder {
public:
    explicit ListHeadersBuilder(const std::string& baseListFName, uint64_t numElements);

    inline csr_offset_t getCSROffset(common::offset_t offset) {
        return offset % common::ListsMetadataConstants::LISTS_CHUNK_SIZE == 0 ?
                   0 :
                   (*headersBuilder)[offset - 1];
    };
    inline csr_offset_t getListSize(common::offset_t offset) const {
        return offset % common::ListsMetadataConstants::LISTS_CHUNK_SIZE == 0 ?
                   (*headersBuilder)[offset] :
                   (*headersBuilder)[offset] - (*headersBuilder)[offset - 1];
    };
    inline uint64_t getNumValues() { return headersBuilder->getNumElements(); }

    inline void setCSROffset(common::offset_t offset, csr_offset_t csrOffset) {
        (*headersBuilder)[offset - 1] = csrOffset;
    }
    inline void saveToDisk() { headersBuilder->saveToDisk(); }

private:
    std::unique_ptr<FileHandle> fileHandle;
    std::unique_ptr<InMemDiskArrayBuilder<csr_offset_t>> headersBuilder;
};

class ListHeaders {
public:
    explicit ListHeaders(const StorageStructureIDAndFName& storageStructureIDAndFNameForBaseList,
        BufferManager* bufferManager, WAL* wal);

    inline csr_offset_t getCSROffset(common::offset_t offset) const {
        return offset % common::ListsMetadataConstants::LISTS_CHUNK_SIZE == 0 ?
                   0 :
                   (*headersDiskArray)[offset - 1];
    };
    inline csr_offset_t getCSROffset(
        const common::offset_t offset, transaction::TransactionType transactionType) const {
        return offset % common::ListsMetadataConstants::LISTS_CHUNK_SIZE == 0 ?
                   0 :
                   headersDiskArray->get(offset - 1, transactionType);
    }
    inline csr_offset_t getListSize(common::offset_t offset) const {
        return offset % common::ListsMetadataConstants::LISTS_CHUNK_SIZE == 0 ?
                   (*headersDiskArray)[offset] :
                   (*headersDiskArray)[offset] - (*headersDiskArray)[offset - 1];
    };
    inline uint64_t getNumElements(transaction::TransactionType transactionType) const {
        return headersDiskArray->getNumElements(transactionType);
    }

    inline void update(common::offset_t offset, csr_offset_t csrOffset) {
        headersDiskArray->update(offset - 1, csrOffset);
    }

    inline void checkpointInMemoryIfNecessary() const {
        headersDiskArray->checkpointInMemoryIfNecessary();
    }
    inline void rollbackInMemoryIfNecessary() const {
        headersDiskArray->rollbackInMemoryIfNecessary();
    }
    inline void pushBack(csr_offset_t csrOffset) { headersDiskArray->pushBack(csrOffset); }

private:
    // The CSR offset inside the chunk for each relList. The size of relList[i] is calculated as
    // header[i] - header[i-1].
    std::unique_ptr<InMemDiskArray<csr_offset_t>> headersDiskArray;
    std::unique_ptr<BMFileHandle> fileHandle;
    StorageStructureIDAndFName storageStructureIDAndFName;
};

} // namespace storage
} // namespace kuzu

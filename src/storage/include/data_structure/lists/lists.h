#pragma once

#include <utility>

#include "src/common/include/literal.h"
#include "src/common/include/value.h"
#include "src/storage/include/data_structure/data_structure.h"
#include "src/storage/include/data_structure/lists/large_list_handle.h"
#include "src/storage/include/data_structure/lists/list_headers.h"
#include "src/storage/include/data_structure/lists/lists_metadata.h"
#include "src/storage/include/data_structure/lists/utils.h"
#include "src/storage/include/data_structure/string_overflow_pages.h"

namespace graphflow {
namespace storage {

struct ListInfo {
    bool isLargeList{false};
    uint64_t listLen{-1u};
    std::function<uint32_t(uint32_t)> mapper;
    PageCursor cursor;
};

/**
 * A lists data structure holds a list of homogeneous values for each offset in it. Lists are used
 * for storing Adjacency List, Rel Property Lists and unstructured Node Property Lists.
 *
 * The offsets in the Lists are partitioned into fixed size. Hence, each offset, and its list,
 * belongs to a chunk. If the offset's list is small (less than the PAGE_SIZE) it is stored together
 * along with other lists in that chunk as in a CSR. However, large lists are stored out of their
 * regular chunks and span multiple pages. The nature, size and logical location of the list is
 * given by a 32-bit header value (explained in {@class ListHeaders}). Given the logical location of
 * a list, {@class ListsMetadata} contains information that maps logical location of the list to the
 * actual physical location in the Lists file on disk.
 * */
class Lists : public DataStructure {

public:
    Lists(const string& fName, const DataType& dataType, const size_t& elementSize,
        shared_ptr<ListHeaders> headers, BufferManager& bufferManager, bool isInMemory)
        : DataStructure{fName, dataType, elementSize, bufferManager, isInMemory}, metadata{fName},
          headers(move(headers)){};

    void readValues(node_offset_t nodeOffset, const shared_ptr<ValueVector>& valueVector,
        const unique_ptr<LargeListHandle>& largeListHandle, BufferManagerMetrics& metrics);

    inline uint64_t getNumElementsInList(node_offset_t nodeOffset) {
        return getListInfo(nodeOffset).listLen;
    }

    ListInfo getListInfo(node_offset_t nodeOffset);

protected:
    virtual void readFromLargeList(const shared_ptr<ValueVector>& valueVector,
        const unique_ptr<LargeListHandle>& largeListHandle, ListInfo& info,
        BufferManagerMetrics& metrics);

    virtual void readSmallList(
        const shared_ptr<ValueVector>& valueVector, ListInfo& info, BufferManagerMetrics& metrics);

public:
    static constexpr char LISTS_SUFFIX[] = ".lists";

    // LIST_CHUNK_SIZE should strictly be a power of 2.
    constexpr static uint16_t LISTS_CHUNK_SIZE_LOG_2 = 9;
    constexpr static uint16_t LISTS_CHUNK_SIZE = 1 << LISTS_CHUNK_SIZE_LOG_2;

protected:
    ListsMetadata metadata;
    shared_ptr<ListHeaders> headers;
};

class StringPropertyLists : public Lists {

public:
    StringPropertyLists(const string& fName, shared_ptr<ListHeaders> headers,
        BufferManager& bufferManager, bool isInMemory)
        : Lists{fName, STRING, sizeof(gf_string_t), headers, bufferManager, isInMemory},
          stringOverflowPages{fName, bufferManager, isInMemory} {};

private:
    void readFromLargeList(const shared_ptr<ValueVector>& valueVector,
        const unique_ptr<LargeListHandle>& largeListHandle, ListInfo& info,
        BufferManagerMetrics& metrics) override;

    void readSmallList(const shared_ptr<ValueVector>& valueVector, ListInfo& info,
        BufferManagerMetrics& metrics) override;

private:
    StringOverflowPages stringOverflowPages;
};

class AdjLists : public Lists {

public:
    AdjLists(const string& fName, BufferManager& bufferManager,
        NodeIDCompressionScheme nodeIDCompressionScheme, bool isInMemory)
        : Lists{fName, NODE, nodeIDCompressionScheme.getNumTotalBytes(),
              make_shared<ListHeaders>(fName), bufferManager, isInMemory},
          nodeIDCompressionScheme{nodeIDCompressionScheme} {};

    shared_ptr<ListHeaders> getHeaders() { return headers; };

    //    // Currently, used only in Loader tests.
    unique_ptr<vector<nodeID_t>> readAdjacencyListOfNode(
        node_offset_t nodeOffset, BufferManagerMetrics& metrics);

private:
    void readFromLargeList(const shared_ptr<ValueVector>& valueVector,
        const unique_ptr<LargeListHandle>& largeListHandle, ListInfo& info,
        BufferManagerMetrics& metrics) override;

    void readSmallList(const shared_ptr<ValueVector>& valueVector, ListInfo& info,
        BufferManagerMetrics& metrics) override;

private:
    NodeIDCompressionScheme nodeIDCompressionScheme;
};

class ListsFactory {

public:
    static unique_ptr<Lists> getLists(const string& fName, const DataType& dataType,
        shared_ptr<ListHeaders> adjListsHeaders, BufferManager& bufferManager, bool isInMemory) {
        switch (dataType) {
        case INT64:
        case DOUBLE:
        case BOOL:
        case DATE:
        case TIMESTAMP:
            return make_unique<Lists>(fName, dataType, TypeUtils::getDataTypeSize(dataType),
                adjListsHeaders, bufferManager, isInMemory);
        case STRING:
            return make_unique<StringPropertyLists>(
                fName, adjListsHeaders, bufferManager, isInMemory);
        default:
            throw invalid_argument("Invalid type for property list creation.");
        }
    }
};

} // namespace storage
} // namespace graphflow

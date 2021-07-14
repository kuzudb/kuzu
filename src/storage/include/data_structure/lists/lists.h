#pragma once

#include <utility>

#include "src/common/include/value.h"
#include "src/common/include/vector/node_vector.h"
#include "src/storage/include/data_structure/data_structure.h"
#include "src/storage/include/data_structure/data_structure_handle.h"
#include "src/storage/include/data_structure/lists/list_headers.h"
#include "src/storage/include/data_structure/lists/lists_metadata.h"
#include "src/storage/include/data_structure/string_overflow_pages.h"

namespace graphflow {
namespace storage {

// BaseLists is the top-level structure that holds a set of lists {of adjacent edges or rel
// properties}.
class BaseLists : public DataStructure {

public:
    void readValues(node_offset_t nodeOffset, const shared_ptr<ValueVector>& valueVector,
        uint64_t& listLen, const unique_ptr<ListHandle>& listHandle, uint32_t maxElementsToRead,
        BufferManagerMetrics& metrics);

    uint64_t getNumElementsInList(node_offset_t nodeOffset);

protected:
    BaseLists(const string& fName, const DataType& dataType, const size_t& elementSize,
        shared_ptr<ListHeaders> headers, BufferManager& bufferManager)
        : DataStructure{fName, dataType, elementSize, bufferManager}, metadata{fName},
          headers(move(headers)){};

    virtual void readFromLargeList(const shared_ptr<ValueVector>& valueVector, uint64_t& listLen,
        const unique_ptr<ListHandle>& listHandle, uint32_t header, uint32_t maxElementsToRead,
        BufferManagerMetrics& metrics);

    virtual void readSmallList(node_offset_t nodeOffset, const shared_ptr<ValueVector>& valueVector,
        uint64_t& listLen, const unique_ptr<ListHandle>& listHandle, uint32_t header,
        BufferManagerMetrics& metrics);

public:
    // LIST_CHUNK_SIZE should strictly be a power of 2.
    constexpr static uint16_t LISTS_CHUNK_SIZE = 512;
    constexpr static uint16_t LISTS_CHUNK_SIZE_LOG_2 = 9;

protected:
    ListsMetadata metadata;
    shared_ptr<ListHeaders> headers;
};

// Lists<D> is the implementation of BaseLists for Lists of a specific datatype D.
template<DataType D>
class Lists : public BaseLists {

public:
    Lists(const string& fName, shared_ptr<ListHeaders> headers, BufferManager& bufferManager)
        : BaseLists{fName, D, TypeUtils::getDataTypeSize(D), headers, bufferManager} {};
};

template<>
class Lists<STRING> : public BaseLists {

public:
    Lists(const string& fName, shared_ptr<ListHeaders> headers, BufferManager& bufferManager)
        : BaseLists{fName, STRING, sizeof(gf_string_t), headers, bufferManager},
          stringOverflowPages{fName, bufferManager} {};

private:
    void readFromLargeList(const shared_ptr<ValueVector>& valueVector, uint64_t& listLen,
        const unique_ptr<ListHandle>& listHandle, uint32_t header, uint32_t maxElementsToRead,
        BufferManagerMetrics& metrics) override;

    void readSmallList(node_offset_t nodeOffset, const shared_ptr<ValueVector>& valueVector,
        uint64_t& listLen, const unique_ptr<ListHandle>& listHandle, uint32_t header,
        BufferManagerMetrics& metrics) override;

private:
    StringOverflowPages stringOverflowPages;
};

// Lists<NODE> is the specialization of Lists<D> for lists of nodeIDs.
template<>
class Lists<NODE> : public BaseLists {

public:
    Lists(const string& fName, BufferManager& bufferManager,
        NodeIDCompressionScheme nodeIDCompressionScheme)
        : BaseLists{fName, NODE, nodeIDCompressionScheme.getNumTotalBytes(),
              make_shared<ListHeaders>(fName), bufferManager},
          nodeIDCompressionScheme{nodeIDCompressionScheme} {};

    NodeIDCompressionScheme& getNodeIDCompressionScheme() { return nodeIDCompressionScheme; }

    shared_ptr<ListHeaders> getHeaders() { return headers; };

private:
    void readFromLargeList(const shared_ptr<ValueVector>& valueVector, uint64_t& listLen,
        const unique_ptr<ListHandle>& listHandle, uint32_t header, uint32_t maxElementsToRead,
        BufferManagerMetrics& metrics) override;

    void readSmallList(node_offset_t nodeOffset, const shared_ptr<ValueVector>& valueVector,
        uint64_t& listLen, const unique_ptr<ListHandle>& listHandle, uint32_t header,
        BufferManagerMetrics& metrics) override;

private:
    NodeIDCompressionScheme nodeIDCompressionScheme;
};

// Lists<UNSTRUCTURED> is the specialization of Lists<D> for Node's unstructured PropertyLists.
// Though this shares the identical representation as BaseLists, it is more aligned to Columns in
// terms of access. In particular, readValues(...) of Lists<UNSTRUCTURED> is given a NodeVector as
// input, similar to readValues() in Columns. For each node in NodeVector, unstructured property
// list of that node is read and the required property along with its dataType is copied to a
// specialized UNSTRUCTURED-typed ValueVector.
template<>
class Lists<UNSTRUCTURED> : public BaseLists {

public:
    Lists(const string& fName, BufferManager& bufferManager)
        : BaseLists{fName, UNSTRUCTURED, 1, make_shared<ListHeaders>(fName), bufferManager},
          stringOverflowPages{fName, bufferManager} {};

    // readValues is overloaded. Lists<UNSTRUCTURED> is not supposed to use the one defined in
    // BaseLists.
    void readValues(const shared_ptr<NodeIDVector>& nodeIDVector, uint32_t propertyKeyIdxToRead,
        const shared_ptr<ValueVector>& valueVector, const unique_ptr<PageHandle>& pageHandle,
        BufferManagerMetrics& metrics);

private:
    void readUnstrPropertyListOfNode(node_offset_t nodeOffset, uint32_t propertyKeyIdxToRead,
        const shared_ptr<ValueVector>& valueVector, uint64_t pos,
        const unique_ptr<PageHandle>& pageHandle, uint32_t header, BufferManagerMetrics& metrics);

    void readUnstrPropertyKeyIdxAndDatatype(uint8_t* propertyKeyDataTypeCache,
        uint64_t& physicalPageIdx, const uint32_t*& propertyKeyIdxPtr,
        DataType& propertyKeyDataType, const unique_ptr<PageHandle>& pageHandle,
        PageCursor& pageCursor, uint64_t& listLen, LogicalToPhysicalPageIdxMapper& mapper,
        BufferManagerMetrics& metrics);

    void readOrSkipUnstrPropertyValue(uint64_t& physicalPageIdx, DataType& propertyDataType,
        const unique_ptr<PageHandle>& pageHandle, PageCursor& pageCursor, uint64_t& listLen,
        LogicalToPhysicalPageIdxMapper& mapper, const shared_ptr<ValueVector>& valueVector,
        uint64_t pos, bool toRead, BufferManagerMetrics& metrics);

public:
    static constexpr uint8_t UNSTR_PROP_IDX_LEN = 4;
    static constexpr uint8_t UNSTR_PROP_DATATYPE_LEN = 1;
    static constexpr uint8_t UNSTR_PROP_HEADER_LEN = UNSTR_PROP_IDX_LEN + UNSTR_PROP_DATATYPE_LEN;

    StringOverflowPages stringOverflowPages;
};

typedef Lists<INT64> RelPropertyListsInt64;
typedef Lists<DOUBLE> RelPropertyListsDouble;
typedef Lists<BOOL> RelPropertyListsBool;
typedef Lists<STRING> RelPropertyListsString;
typedef Lists<NODE> AdjLists;
typedef Lists<UNSTRUCTURED> UnstructuredPropertyLists;
typedef Lists<DATE> RelPropertyListsDate;

} // namespace storage
} // namespace graphflow

#pragma once

#include "src/common/include/vector/node_vector.h"
#include "src/storage/include/data_structure/data_structure.h"
#include "src/storage/include/data_structure/data_structure_handle.h"
#include "src/storage/include/data_structure/lists/list_headers.h"
#include "src/storage/include/data_structure/lists/lists_metadata.h"

namespace graphflow {
namespace storage {

// BaseLists is the top-level structure that holds a set of lists {of adjacent edges or rel
// properties}.
class BaseLists : public DataStructure {

public:
    void readValues(node_offset_t nodeOffset, const shared_ptr<ValueVector>& valueVector,
        uint64_t& listLen, const unique_ptr<DataStructureHandle>& handle,
        uint32_t maxElementsToRead, BufferManagerMetrics& metrics);

    uint64_t getNumElementsInList(node_offset_t nodeOffset);

protected:
    BaseLists(const string& fname, const DataType& dataType, const size_t& elementSize,
        shared_ptr<ListHeaders> headers, BufferManager& bufferManager)
        : DataStructure{fname, dataType, elementSize, bufferManager}, metadata{fname},
          headers(headers){};

    void readFromLargeList(const shared_ptr<ValueVector>& valueVector, uint64_t& listLen,
        const unique_ptr<DataStructureHandle>& handle, uint32_t header, uint32_t maxElementsToRead,
        BufferManagerMetrics& metrics);

    void readSmallList(node_offset_t nodeOffset, const shared_ptr<ValueVector>& valueVector,
        uint64_t& listLen, const unique_ptr<DataStructureHandle>& handle, uint32_t header,
        BufferManagerMetrics& metrics);

public:
    constexpr static uint16_t LISTS_CHUNK_SIZE = 512;

protected:
    ListsMetadata metadata;
    shared_ptr<ListHeaders> headers;
};

// Lists<D> is the implementation of BaseLists for Lists of a specific datatype D.
template<DataType D>
class Lists : public BaseLists {

public:
    Lists(const string& fname, shared_ptr<ListHeaders> headers, BufferManager& bufferManager)
        : BaseLists{fname, D, TypeUtils::getDataTypeSize(D), headers, bufferManager} {};
};

// Lists<NODE> is the specialization of Lists<D> for lists of nodeIDs.
template<>
class Lists<NODE> : public BaseLists {

public:
    Lists(const string& fname, BufferManager& bufferManager,
        NodeIDCompressionScheme nodeIDCompressionScheme)
        : BaseLists{fname, NODE, nodeIDCompressionScheme.getNumTotalBytes(),
              make_shared<ListHeaders>(fname), bufferManager},
          nodeIDCompressionScheme{nodeIDCompressionScheme} {};

    NodeIDCompressionScheme& getNodeIDCompressionScheme() { return nodeIDCompressionScheme; }

    shared_ptr<ListHeaders> getHeaders() { return headers; };

private:
    NodeIDCompressionScheme nodeIDCompressionScheme;
};

// Lists<UNSTRUCTURED> is the specialization of Lists<D> for Node's unstructured PropertyLists.
// Though this shares the identical representation as BaseLists, it is more aligned to Columns in
// terms of access. In particular, readValues(...) of Lists<UNSTRUCTURED> is given a NodeVector as
// input, similar to readValues() in Columns. For each node in NodeVector, unstructured property
// list of that node is read and the reqired property alongwith its dataType is copied to a
// specialized UNSTRUCTURED-typed ValueVector.
template<>
class Lists<UNSTRUCTURED> : public BaseLists {

public:
    Lists(const string& fname, BufferManager& bufferManager)
        : BaseLists{fname, UNSTRUCTURED, 1, make_shared<ListHeaders>(fname), bufferManager},
          overflowPagesFileHandle{fname + OVERFLOW_FILE_SUFFIX, O_RDWR} {};

    // readValues is overloaded. Lists<UNSTRUCTURED> is not supposed to use the one defined in
    // BaseLists.
    void readValues(const shared_ptr<NodeIDVector>& nodeIDVector, uint32_t propertyKeyIdxToRead,
        const shared_ptr<ValueVector>& valueVector, const unique_ptr<DataStructureHandle>& handle,
        BufferManagerMetrics& metrics);

private:
    void readUnstrPropertyListOfNode(node_offset_t nodeOffset, uint32_t propertyKeyIdxToRead,
        const shared_ptr<ValueVector>& valueVector, uint64_t pos,
        const unique_ptr<DataStructureHandle>& handle, uint32_t header,
        BufferManagerMetrics& metrics);

    void readUnstrPropertyKeyIdxAndDatatype(uint8_t* propertyKeyDataTypeCache,
        uint64_t& physicalPageIdx, const uint32_t*& propertyKeyIdxPtr,
        DataType& propertyKeyDataType, const unique_ptr<DataStructureHandle>& handle,
        PageCursor& pageCursor, uint64_t& listLen, LogicalToPhysicalPageIdxMapper& mapper,
        BufferManagerMetrics& metrics);

    void readOrSkipUnstrPropertyValue(uint64_t& physicalPageIdx, DataType& propertyDataType,
        const unique_ptr<DataStructureHandle>& handle, PageCursor& pageCursor, uint64_t& listLen,
        LogicalToPhysicalPageIdxMapper& mapper, const shared_ptr<ValueVector>& valueVector,
        uint64_t pos, bool toRead, BufferManagerMetrics& metrics);

    void readStringsFromOverflowPages(ValueVector& valueVector, BufferManagerMetrics& metrics);

public:
    static constexpr uint8_t UNSTR_PROP_IDX_LEN = 4;
    static constexpr uint8_t UNSTR_PROP_DATATYPE_LEN = 1;
    static constexpr uint8_t UNSTR_PROP_HEADER_LEN = UNSTR_PROP_IDX_LEN + UNSTR_PROP_DATATYPE_LEN;

    FileHandle overflowPagesFileHandle;
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

#pragma once

#include "src/storage/include/data_structure/lists/lists.h"

namespace graphflow {
namespace storage {

struct UnstructuredPropertyKeyDataType {
    uint32_t keyIdx;
    DataType dataType;
};
// UnstructuredPropertyLists is the specialization of Lists for Node's unstructured property lists.
// Though this shares the identical representation as Lists, it is more aligned to Columns in
// terms of access. In particular, readValues(...) of UnstructuredPropertyLists> is given a
// NodeVector as input, similar to readValues() in Columns. For each node in NodeVector,
// unstructured property list of that node is read and the required property along with its dataType
// is copied to a specialized UNSTRUCTURED-typed ValueVector.
class UnstructuredPropertyLists : public Lists {

public:
    UnstructuredPropertyLists(const string& fName, BufferManager& bufferManager, bool isInMemory)
        : Lists{fName, UNSTRUCTURED, 1, make_shared<ListHeaders>(fName), bufferManager,
              false /*hasNULLBytes*/, isInMemory},
          stringOverflowPages{fName, bufferManager, isInMemory} {};

    void readUnstructuredProperties(const shared_ptr<ValueVector>& nodeIDVector,
        uint32_t propertyKeyIdxToRead, const shared_ptr<ValueVector>& valueVector,
        BufferManagerMetrics& metrics);

    // Currently, used only in Loader tests.
    unique_ptr<map<uint32_t, Literal>> readUnstructuredPropertiesOfNode(
        node_offset_t nodeOffset, BufferManagerMetrics& metrics);

private:
    void readUnstructuredPropertyForSingleNodeIDPosition(uint32_t pos,
        const shared_ptr<ValueVector>& nodeIDVector, uint32_t propertyKeyIdxToRead,
        const shared_ptr<ValueVector>& valueVector, BufferManagerMetrics& metrics);

    void readUnstrPropertyKeyIdxAndDatatype(uint8_t* propertyKeyDataType, PageByteCursor& cursor,
        uint64_t& listLen, const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper,
        BufferManagerMetrics& metrics);

    void readOrSkipUnstrPropertyValue(DataType& propertyDataType, PageByteCursor& cursor,
        uint64_t& listLen, const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper,
        Value* value, bool toRead, BufferManagerMetrics& metrics);

public:
    static constexpr uint8_t UNSTR_PROP_IDX_LEN = 4;
    static constexpr uint8_t UNSTR_PROP_DATATYPE_LEN = 1;
    static constexpr uint8_t UNSTR_PROP_HEADER_LEN = UNSTR_PROP_IDX_LEN + UNSTR_PROP_DATATYPE_LEN;

    StringOverflowPages stringOverflowPages;
};

} // namespace storage
} // namespace graphflow

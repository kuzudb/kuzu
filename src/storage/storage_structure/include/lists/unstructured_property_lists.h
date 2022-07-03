#pragma once

#include "src/storage/storage_structure/include/lists/lists.h"

namespace graphflow {
namespace storage {

struct UnstructuredPropertyKeyDataType {
    uint32_t keyIdx;
    DataTypeID dataTypeID;
};

// UnstructuredPropertyLists is the specialization of Lists for Node's unstructured property lists.
// Though this shares the identical representation as Lists, it is more aligned to Columns in
// terms of access. In particular, readValues(...) of UnstructuredPropertyLists> is given a
// NodeVector as input, similar to readValues() in Columns. For each node in NodeVector,
// unstructured property list of that node is read and the required property along with its
// dataTypeID is copied to a specialized UNSTRUCTURED-typed ValueVector.
class UnstructuredPropertyLists : public Lists {

public:
    UnstructuredPropertyLists(const StorageStructureIDAndFName& storageStructureIDAndFName,
        BufferManager& bufferManager, bool isInMemory)
        : Lists{storageStructureIDAndFName, DataType(UNSTRUCTURED), 1,
              make_shared<ListHeaders>(storageStructureIDAndFName), bufferManager,
              false /*hasNULLBytes*/, isInMemory},
          stringOverflowPages{storageStructureIDAndFName, bufferManager, isInMemory,
              nullptr /* no wal for now */} {};

    void readProperties(ValueVector* nodeIDVector,
        const unordered_map<uint32_t, ValueVector*>& propertyKeyToResultVectorMap);

    // Currently, used only in Loader tests.
    unique_ptr<map<uint32_t, Literal>> readUnstructuredPropertiesOfNode(node_offset_t nodeOffset);

private:
    void readPropertiesForPosition(ValueVector* nodeIDVector, uint32_t pos,
        const unordered_map<uint32_t, ValueVector*>& propertyKeyToResultVectorMap);

    void readPropertyKeyAndDatatype(uint8_t* propertyKeyDataType, PageCursor& cursor,
        const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper);

    void readPropertyValue(Value* propertyValue, uint64_t dataTypeSize, PageCursor& cursor,
        const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper);

    void skipPropertyValue(uint64_t dataTypeSize, PageCursor& cursor);

    void readFromAPage(uint8_t* value, uint64_t bytesToRead, PageCursor& cursor,
        const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper);

public:
    static constexpr uint8_t UNSTR_PROP_IDX_LEN = 4;
    static constexpr uint8_t UNSTR_PROP_DATATYPE_LEN = 1;
    static constexpr uint8_t UNSTR_PROP_HEADER_LEN = UNSTR_PROP_IDX_LEN + UNSTR_PROP_DATATYPE_LEN;

    OverflowFile stringOverflowPages;
};

} // namespace storage
} // namespace graphflow

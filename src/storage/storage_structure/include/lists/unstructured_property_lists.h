#pragma once

#include "updated_unstructured_property_lists.h"

#include "src/storage/storage_structure/include/lists/lists.h"

namespace graphflow {
namespace storage {

using lock_t = unique_lock<mutex>;

// UnstructuredPropertyLists is the specialization of Lists for Node's unstructured property lists.
// Though this shares the identical representation as Lists, it is more aligned to Columns in
// terms of access. In particular, readValues(...) of UnstructuredPropertyLists> is given a
// NodeVector as input, similar to readValues() in Columns. For each node in NodeVector,
// unstructured property list of that node is read and the required property along with its
// dataTypeID is copied to a specialized UNSTRUCTURED-typed ValueVector.
class UnstructuredPropertyLists : public Lists {

public:
    UnstructuredPropertyLists(const StorageStructureIDAndFName& storageStructureIDAndFName,
        BufferManager& bufferManager, bool isInMemory, WAL* wal)
        : Lists{storageStructureIDAndFName, DataType(UNSTRUCTURED), 1,
              make_shared<ListHeaders>(storageStructureIDAndFName), bufferManager,
              false /*hasNULLBytes*/, isInMemory},
          stringOverflowPages{storageStructureIDAndFName, bufferManager, isInMemory, wal},
          updatedLists{stringOverflowPages} {};

    void readProperties(Transaction* transaction, ValueVector* nodeIDVector,
        const unordered_map<uint32_t, ValueVector*>& propertyKeyToResultVectorMap);

    void setPropertyListEmpty(node_offset_t nodeOffset);
    void setProperty(node_offset_t nodeOffset, uint32_t propertyKey, Value* value);
    void removeProperty(node_offset_t nodeOffset, uint32_t propertyKey);

    // Currently, used only in Loader tests.
    unique_ptr<map<uint32_t, Literal>> readUnstructuredPropertiesOfNode(node_offset_t nodeOffset);

private:
    void fillUnstrPropListFromPrimaryStore(ListInfo& listInfo, uint8_t* dataToFill);
    void readPropertiesForPosition(Transaction* transaction, ValueVector* nodeIDVector,
        uint32_t pos, const unordered_map<uint32_t, ValueVector*>& propertyKeyToResultVectorMap);

    void readPropertyKeyAndDatatype(uint8_t* propertyKeyDataType, PageByteCursor& cursor,
        const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper);

    void readPropertyValue(Value* propertyValue, uint64_t dataTypeSize, PageByteCursor& cursor,
        const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper);

    void readFromAPage(uint8_t* value, uint64_t bytesToRead, PageByteCursor& cursor,
        const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper);

public:
    // TODO(Semih/Guodong): Currently we serialize all access to localUnstructuredPropertyLists
    // and should optimize parallel updates.
    mutex mtx;
    OverflowFile stringOverflowPages;
    UpdatedUnstructuredPropertyLists updatedLists;
};

} // namespace storage
} // namespace graphflow

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
              make_shared<ListHeaders>(storageStructureIDAndFName, &bufferManager, wal),
              bufferManager, false /*hasNULLBytes*/, isInMemory, wal},
          stringOverflowPages{storageStructureIDAndFName, bufferManager, isInMemory, wal},
          localUpdatedLists{stringOverflowPages} {};

    void readProperties(Transaction* transaction, ValueVector* nodeIDVector,
        const unordered_map<uint32_t, ValueVector*>& propertyKeyToResultVectorMap);

    void setPropertyListEmpty(node_offset_t nodeOffset);
    void setProperty(node_offset_t nodeOffset, uint32_t propertyKey, Value* value);
    void removeProperty(node_offset_t nodeOffset, uint32_t propertyKey);

    // Currently, used only in Loader tests.
    unique_ptr<map<uint32_t, Literal>> readUnstructuredPropertiesOfNode(node_offset_t nodeOffset);

    // Prepares all the db file changes necessary to update the "persistent" unstructured property
    // lists with the UpdatedUnstructuredPropertyLists localUpdatedLists, which stores the updates
    // by the write trx locally.
    // TODO(Semih): This should call update and push back on the InMemoryDiskArray and create the
    //  WAL and updates versions of the actual pages of the lists.
    void prepareCommit();

    // Checkpoints the in memory version of the lists.
    // TODO(Semih): This should simply call checkpointInMemory on all InMemDiskArrays that it has
    // and clear the localUpdatedLists.
    void checkpointInMemoryIfNecessary() override;

    // Rollbacks in memory any updates that have been performed.
    // TODO(Semih): This should simply call rollbackInMemory on all InMemDiskArrays that it has
    // and clear the localUpdatedLists.
    void rollbackInMemoryIfNecessary() override;

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
    // TODO(Semih/Guodong): Currently we serialize all access to localUpdatedLists
    // and should optimize parallel updates.
    mutex mtx;
    OverflowFile stringOverflowPages;
    UpdatedUnstructuredPropertyLists localUpdatedLists;
};

} // namespace storage
} // namespace graphflow

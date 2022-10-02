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
              bufferManager, false /*hasNULLBytes*/, isInMemory, wal,
              nullptr /* listUpdateStore */},
          overflowFile{storageStructureIDAndFName, bufferManager, isInMemory, wal},
          localUpdatedLists{overflowFile} {};

    void readProperties(Transaction* transaction, ValueVector* nodeIDVector,
        const unordered_map<uint32_t, ValueVector*>& propertyKeyToResultVectorMap);

    void setPropertyListEmpty(node_offset_t nodeOffset);
    void setOrRemoveProperty(
        node_offset_t nodeOffset, uint32_t propertyKey, bool isSetting, Value* value = nullptr);
    inline void setProperty(node_offset_t nodeOffset, uint32_t propertyKey, Value* value) {
        setOrRemoveProperty(nodeOffset, propertyKey, true /* isSetting */, value);
    }
    inline void removeProperty(node_offset_t nodeOffset, uint32_t propertyKey) {
        setOrRemoveProperty(nodeOffset, propertyKey, false /* isSetting */);
    }

    // Currently, used only in CopyCSV tests.
    unique_ptr<map<uint32_t, Literal>> readUnstructuredPropertiesOfNode(node_offset_t nodeOffset);

    // Prepares all the db file changes necessary to update the "persistent" unstructured property
    // lists with the UpdatedUnstructuredPropertyLists localUpdatedLists, which stores the updates
    // by the write trx locally.
    void prepareCommitOrRollbackIfNecessary(bool isCommit);

    // Checkpoints the in memory version of the lists.
    void checkpointInMemoryIfNecessary() override;

    // Rollbacks in memory any updates that have been performed.
    void rollbackInMemoryIfNecessary() override;

private:
    void readPropertiesForPosition(Transaction* transaction, ValueVector* nodeIDVector,
        uint32_t pos, const unordered_map<uint32_t, ValueVector*>& propertyKeyToResultVectorMap);

    void readPropertyKeyAndDatatype(uint8_t* propertyKeyDataType, PageByteCursor& cursor,
        const std::function<uint32_t(uint32_t)>& idxInPageListToListPageIdxMapper);

    void readPropertyValue(Value* propertyValue, uint64_t dataTypeSize, PageByteCursor& cursor,
        const std::function<uint32_t(uint32_t)>& idxInPageListToListPageIdxMapper);

    void readFromAPage(uint8_t* value, uint64_t bytesToRead, PageByteCursor& cursor,
        const std::function<uint32_t(uint32_t)>& idxInPageListToListPageIdxMapper);

public:
    // TODO(Semih/Guodong): Currently we serialize all access to localUpdatedLists
    // and should optimize parallel updates.
    mutex mtx;
    OverflowFile overflowFile;
    UpdatedUnstructuredPropertyLists localUpdatedLists;
};

} // namespace storage
} // namespace graphflow

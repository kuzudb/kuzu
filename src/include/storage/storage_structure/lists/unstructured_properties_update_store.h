#pragma once

#include "storage/storage_structure/lists/lists.h"
#include "storage/storage_structure/lists/unstructured_property_lists_utils.h"

namespace kuzu {
namespace storage {

using UpdatedChunk = map<node_offset_t, unique_ptr<UnstrPropListWrapper>>;

/**
 * UnstructuredPropertiesUpdateStore is not thread-safe and assumes that the
 * UnstructuredPropertyLists, which is the sole user of UnstructuredPropertiesUpdateStore acquires
 * necessary locks to ensure UnstructuredPropertiesUpdateStore is accessed in a thread-safe manner.
 */
class UnstructuredPropertiesUpdateStore {

public:
    UnstructuredPropertiesUpdateStore(DiskOverflowFile& stringDiskOverflowFile)
        : stringDiskOverflowFile{stringDiskOverflowFile} {};

    inline bool empty() { return updatedChunks.empty(); }
    bool hasUpdatedList(node_offset_t nodeOffset);
    UnstrPropListIterator getUpdatedListIterator(node_offset_t nodeOffset);
    void setEmptyUpdatedPropertiesList(node_offset_t nodeOffset);
    void setPropertyList(
        node_offset_t nodeOffset, unique_ptr<UnstrPropListWrapper> unstrPropListWrapper);
    void setProperty(node_offset_t nodeOffset, uint32_t propertyKey, Value* value);
    // Warning: This function assumes that an updatedList for the nodeOffsetForPropKeys already
    // exists
    void removeProperty(node_offset_t nodeOffset, uint32_t propertyKey);
    inline void clear() { updatedChunks.clear(); }

private:
    uint64_t getChunkIdxAndInsertUpdatedChunkIfNecessaryWithoutLock(node_offset_t nodeOffset);
    void insertUpdatedListWrapper(uint64_t chunkIdx, node_offset_t nodeOffset,
        unique_ptr<UnstrPropListWrapper> unstrPropListWrapper);
    void setValue(uint8_t* updatedListData, uint64_t offsetInUpdatedList, Value* value,
        uint32_t dataTypeSize);

public:
    DiskOverflowFile& stringDiskOverflowFile;
    map<uint64_t, unique_ptr<UpdatedChunk>> updatedChunks;
};

} // namespace storage
} // namespace kuzu

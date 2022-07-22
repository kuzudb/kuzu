#pragma once

#include "src/storage/storage_structure/include/lists/lists.h"
#include "src/storage/storage_structure/include/lists/unstructured_property_lists_utils.h"

namespace graphflow {
namespace storage {

using UpdatedChunk = map<node_offset_t, unique_ptr<UnstrPropListWrapper>>;

/**
 * UpdatedUnstructuredPropertyLists is not thread-safe and assumes that the
 * UnstructuredPropertyLists, which is the sole user of UpdatedUnstructuredPropertyLists acquires
 * necessary locks to ensure UpdatedUnstructuredPropertyLists is accessed in a thread-safe manner.
 */
class UpdatedUnstructuredPropertyLists {

public:
    UpdatedUnstructuredPropertyLists(OverflowFile& stringOverflowPages)
        : stringOverflowPages{stringOverflowPages} {};

    bool empty();
    bool hasUpdatedList(node_offset_t nodeOffset);
    UnstrPropListIterator getUpdatedListIterator(node_offset_t nodeOffset);
    void setEmptyUpdatedPropertiesList(node_offset_t nodeOffset);
    void setPropertyList(
        node_offset_t nodeOffset, unique_ptr<UnstrPropListWrapper> unstrPropListWrapper);
    void setProperty(node_offset_t nodeOffset, uint32_t propertyKey, Value* value);
    // Warning: This function assumes that an updatedList for the nodeOffsetForPropKeys already
    // exists
    void removeProperty(node_offset_t nodeOffset, uint32_t propertyKey);

private:
    uint64_t getChunkIdxAndInsertUpdatedChunkIfNecessaryWithoutLock(node_offset_t nodeOffset);
    void insertUpdatedListWrapper(uint64_t chunkIdx, node_offset_t nodeOffset,
        unique_ptr<UnstrPropListWrapper> unstrPropListWrapper);
    void setValue(uint8_t* updatedListData, uint64_t offsetInUpdatedList, Value* value,
        uint32_t dataTypeSize);

public:
    OverflowFile& stringOverflowPages;
    map<uint64_t, unique_ptr<UpdatedChunk>> updatedChunks;
};

} // namespace storage
} // namespace graphflow

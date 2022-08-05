#include "src/storage/storage_structure/include/lists/unstructured_property_lists.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

bool UpdatedUnstructuredPropertyLists::hasUpdatedList(node_offset_t nodeOffset) {
    uint64_t chunkIdx = StorageUtils::getListChunkIdx(nodeOffset);
    return updatedChunks.contains(chunkIdx) &&
           updatedChunks.find(chunkIdx)->second->contains(nodeOffset);
}

void UpdatedUnstructuredPropertyLists::setEmptyUpdatedPropertiesList(node_offset_t nodeOffset) {
    uint64_t chunkIdx = getChunkIdxAndInsertUpdatedChunkIfNecessaryWithoutLock(nodeOffset);
    auto updatedChunk = updatedChunks.find(chunkIdx)->second.get();
    if (!updatedChunk->contains(nodeOffset)) {
        unique_ptr<uint8_t[]> emptyList = make_unique<uint8_t[]>(0);
        insertUpdatedListWrapper(
            chunkIdx, nodeOffset, make_unique<UnstrPropListWrapper>(move(emptyList), 0, 0));
    } else {
        updatedChunk->find(nodeOffset)->second->clear();
    }
}

// Assumes the UpdatedChunk for nodeOffsetForPropKeys already exists.
// chunkIdx is passed because the current callers have already computed it not because it is
// necessary.
void UpdatedUnstructuredPropertyLists::insertUpdatedListWrapper(uint64_t chunkIdx,
    node_offset_t nodeOffset, unique_ptr<UnstrPropListWrapper> unstrPropListWrapper) {
    auto updatedChunk = updatedChunks.find(chunkIdx)->second.get();
    updatedChunk->insert({nodeOffset, move(unstrPropListWrapper)});
}

void UpdatedUnstructuredPropertyLists::setPropertyList(
    node_offset_t nodeOffset, unique_ptr<UnstrPropListWrapper> unstrPropListWrapper) {
    uint64_t chunkIdx = getChunkIdxAndInsertUpdatedChunkIfNecessaryWithoutLock(nodeOffset);
    insertUpdatedListWrapper(chunkIdx, nodeOffset, move(unstrPropListWrapper));
}

// This function assumes that the caller has before copied the initial original property list to
// the local store.
void UpdatedUnstructuredPropertyLists::setProperty(
    node_offset_t nodeOffset, uint32_t propertyKey, Value* value) {
    uint64_t chunkIdx = getChunkIdxAndInsertUpdatedChunkIfNecessaryWithoutLock(nodeOffset);
    auto updatedList = updatedChunks.find(chunkIdx)->second->find(nodeOffset)->second.get();
    bool found = UnstrPropListUtils::findKeyPropertyAndPerformOp(
        updatedList, propertyKey, [this, &updatedList, &value](UnstrPropListIterator& itr) -> void {
            setValue(
                updatedList->data.get(), itr.getCurOff(), value, itr.getDataTypeSizeOfCurrProp());
        });
    // If the property did not exist, we need to append to the list
    if (!found) {
        uint64_t dataTypeSize = Types::getDataTypeSize(value->dataType);
        uint64_t totalSize = StorageConfig::UNSTR_PROP_HEADER_LEN + dataTypeSize;

        updatedList->increaseCapacityIfNeeded(updatedList->size + totalSize);
        uint64_t offsetToWriteTo = updatedList->size;
        memcpy(updatedList->data.get() + offsetToWriteTo, reinterpret_cast<uint8_t*>(&propertyKey),
            StorageConfig::UNSTR_PROP_KEY_IDX_LEN);
        offsetToWriteTo += StorageConfig::UNSTR_PROP_KEY_IDX_LEN;
        memcpy(updatedList->data.get() + offsetToWriteTo,
            reinterpret_cast<uint8_t*>(&value->dataType.typeID),
            StorageConfig::UNSTR_PROP_DATATYPE_LEN);
        offsetToWriteTo += StorageConfig::UNSTR_PROP_DATATYPE_LEN;
        setValue(updatedList->data.get(), offsetToWriteTo, value, dataTypeSize);
        updatedList->incrementSize(totalSize);
    }
}

// dataTypeSize is passed because the current callers all have already computed it not because it is
// necessary. It can be computed from value->dataType if needed.
void UpdatedUnstructuredPropertyLists::setValue(
    uint8_t* updatedListData, uint64_t offsetInUpdatedList, Value* value, uint32_t dataTypeSize) {
    memcpy(updatedListData + offsetInUpdatedList, reinterpret_cast<uint8_t*>(&value->val),
        dataTypeSize);
    if (STRING == value->dataType.typeID) {
        if (!gf_string_t::isShortString(value->val.strVal.len)) {
            // If the string we write is a long string, its overflowPtr is currently
            // pointing to the overflow buffer of vectorToWriteFrom. We need to move it
            // to storage.
            gf_string_t* stringToWriteTo = (gf_string_t*)(updatedListData + offsetInUpdatedList);
            gf_string_t* stringToWriteFrom = (gf_string_t*)&value->val;
            stringOverflowPages.writeStringOverflowAndUpdateOverflowPtr(
                *stringToWriteFrom, *stringToWriteTo);
        }
    }
}

UnstrPropListIterator UpdatedUnstructuredPropertyLists::getUpdatedListIterator(
    node_offset_t nodeOffset) {
    uint64_t chunkIdx = StorageUtils::getListChunkIdx(nodeOffset);
    return UnstrPropListIterator(
        updatedChunks.find(chunkIdx)->second->find(nodeOffset)->second.get());
}

void UpdatedUnstructuredPropertyLists::removeProperty(
    node_offset_t nodeOffset, uint32_t propertyKey) {
    uint64_t chunkIdx = StorageUtils::getListChunkIdx(nodeOffset);
    auto updatedList = updatedChunks.find(chunkIdx)->second->find(nodeOffset)->second.get();
    UnstrPropListUtils::findKeyPropertyAndPerformOp(
        updatedList, propertyKey, [&updatedList](UnstrPropListIterator& itr) -> void {
            updatedList->removePropertyAtOffset(
                itr.getOffsetAtBeginningOfCurrProp(), itr.getDataTypeSizeOfCurrProp());
        });
}

uint64_t UpdatedUnstructuredPropertyLists::getChunkIdxAndInsertUpdatedChunkIfNecessaryWithoutLock(
    node_offset_t nodeOffset) {
    uint64_t chunkIdx = StorageUtils::getListChunkIdx(nodeOffset);
    if (!updatedChunks.contains(chunkIdx)) {
        updatedChunks.insert({chunkIdx, make_unique<UpdatedChunk>()});
    }
    return chunkIdx;
}

} // namespace storage
} // namespace graphflow

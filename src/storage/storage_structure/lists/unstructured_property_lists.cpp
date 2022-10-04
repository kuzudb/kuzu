#include "src/storage/storage_structure/include/lists/unstructured_property_lists.h"

#include "src/storage/storage_structure/include/lists/lists_update_iterator.h"
#include "src/storage/storage_structure/include/lists/unstructured_property_lists_utils.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

void UnstructuredPropertyLists::readProperties(Transaction* transaction, ValueVector* nodeIDVector,
    const unordered_map<uint32_t, ValueVector*>& propertyKeyToResultVectorMap) {
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->getPositionOfCurrIdx();
        readPropertiesForPosition(transaction, nodeIDVector, pos, propertyKeyToResultVectorMap);
    } else {
        for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            readPropertiesForPosition(transaction, nodeIDVector, pos, propertyKeyToResultVectorMap);
        }
    }
}

void UnstructuredPropertyLists::readPropertiesForPosition(Transaction* transaction,
    ValueVector* nodeIDVector, uint32_t pos,
    const unordered_map<uint32_t, ValueVector*>& propertyKeyToResultVectorMap) {
    if (nodeIDVector->isNull(pos)) {
        for (auto& [key, vector] : propertyKeyToResultVectorMap) {
            vector->setNull(pos, true);
        }
        return;
    }
    unordered_set<uint32_t> propertyKeysFound;
    auto nodeOffset = nodeIDVector->readNodeOffset(pos);
    // This is declared outside to ensure that in case the if branch is executed, the allocated
    // memory does not go out of space and we can keep a valid pointer in the pair above. In case
    // the else branch executes, data is never used.
    unique_ptr<UnstrPropListWrapper> primaryStoreListWrapper;
    UnstrPropListIterator itr;
    if (transaction->isReadOnly() || !localUpdatedLists.hasUpdatedList(nodeOffset)) {
        auto header = headers->getHeader(nodeOffset);
        CursorAndMapper cursorAndMapper;
        cursorAndMapper.reset(metadata, numElementsPerPage, header, nodeOffset);
        uint64_t numElementsInLIst = getNumElementsInPersistentStore(nodeOffset);
        InMemList inMemList{numElementsInLIst, elementSize, false /* requireNullMask */};
        fillListsFromPersistentStore(cursorAndMapper, numElementsInLIst, inMemList);
        primaryStoreListWrapper = make_unique<UnstrPropListWrapper>(
            move(inMemList.listData), numElementsInLIst, numElementsInLIst /* capacity */);
        itr = UnstrPropListIterator(primaryStoreListWrapper.get());
    } else {
        itr = localUpdatedLists.getUpdatedListIterator(nodeOffset);
    }

    while (itr.hasNext()) {
        auto propertyKeyDataType = itr.readNextPropKeyValue();
        if (propertyKeyToResultVectorMap.contains(propertyKeyDataType.keyIdx)) {
            propertyKeysFound.insert(propertyKeyDataType.keyIdx);
            auto vector = propertyKeyToResultVectorMap.at(propertyKeyDataType.keyIdx);
            vector->setNull(pos, false);
            auto value = &((Value*)vector->values)[pos];
            itr.copyValueOfCurrentProp(reinterpret_cast<uint8_t*>(&value->val));
            value->dataType.typeID = propertyKeyDataType.dataTypeID;
            if (propertyKeyDataType.dataTypeID == STRING) {
                overflowFile.readStringToVector(
                    transaction, value->val.strVal, vector->getOverflowBuffer());
            }
        }
        // We skipValue regardless of whether we found a property and called
        // itr.copyValueOfCurrentProp, because itr.copyValueOfCurrentProp does not move the
        // curOff of the iterator.
        itr.skipValue();
        if (propertyKeysFound.size() ==
            propertyKeyToResultVectorMap.size()) { // all properties are found.
            break;
        }
    }
    for (auto& [key, vector] : propertyKeyToResultVectorMap) {
        if (!propertyKeysFound.contains(key)) {
            vector->setNull(pos, true);
        }
    }
}

unique_ptr<map<uint32_t, Literal>> UnstructuredPropertyLists::readUnstructuredPropertiesOfNode(
    node_offset_t nodeOffset) {
    CursorAndMapper cursorAndMapper;
    cursorAndMapper.reset(metadata, numElementsPerPage, headers->getHeader(nodeOffset), nodeOffset);
    auto numElementsInList = getNumElementsInPersistentStore(nodeOffset);
    auto retVal = make_unique<map<uint32_t /*unstructuredProperty pageIdx*/, Literal>>();
    PageByteCursor byteCursor{cursorAndMapper.cursor.pageIdx, cursorAndMapper.cursor.elemPosInPage};
    auto propertyKeyDataType = UnstructuredPropertyKeyDataType{UINT32_MAX, ANY};
    auto numBytesRead = 0u;
    while (numBytesRead < numElementsInList) {
        readPropertyKeyAndDatatype(
            (uint8_t*)(&propertyKeyDataType), byteCursor, cursorAndMapper.mapper);
        numBytesRead += StorageConfig::UNSTR_PROP_HEADER_LEN;
        auto dataTypeSize = Types::getDataTypeSize(propertyKeyDataType.dataTypeID);
        Value unstrPropertyValue{DataType(propertyKeyDataType.dataTypeID)};
        readPropertyValue(&unstrPropertyValue,
            Types::getDataTypeSize(propertyKeyDataType.dataTypeID), byteCursor,
            cursorAndMapper.mapper);
        numBytesRead += dataTypeSize;
        Literal propertyValueAsLiteral;
        if (STRING == propertyKeyDataType.dataTypeID) {
            propertyValueAsLiteral =
                Literal(overflowFile.readString(unstrPropertyValue.val.strVal));
        } else {
            propertyValueAsLiteral = Literal(
                (uint8_t*)&unstrPropertyValue.val, DataType(propertyKeyDataType.dataTypeID));
        }
        retVal->insert(pair<uint32_t, Literal>(propertyKeyDataType.keyIdx, propertyValueAsLiteral));
    }
    return retVal;
}

void UnstructuredPropertyLists::readPropertyKeyAndDatatype(uint8_t* propertyKeyDataType,
    PageByteCursor& cursor, const function<uint32_t(uint32_t)>& idxInPageListToListPageIdxMapper) {
    auto totalNumBytesRead = 0u;
    auto bytesInCurrentPage = DEFAULT_PAGE_SIZE - cursor.offsetInPage;
    auto bytesToReadInCurrentPage =
        min((uint64_t)StorageConfig::UNSTR_PROP_HEADER_LEN, bytesInCurrentPage);
    readFromAPage(
        propertyKeyDataType, bytesToReadInCurrentPage, cursor, idxInPageListToListPageIdxMapper);
    totalNumBytesRead += bytesToReadInCurrentPage;
    if (StorageConfig::UNSTR_PROP_HEADER_LEN > totalNumBytesRead) { // move to next page
        cursor.pageIdx++;
        cursor.offsetInPage = 0;
        auto bytesToReadInNextPage = StorageConfig::UNSTR_PROP_HEADER_LEN - totalNumBytesRead;
        // IMPORTANT NOTE: Pranjal used to use bytesInCurrentPage instead of totalNumBytesRead
        // in the following function. Xiyang think this is a bug and modify it.
        readFromAPage(propertyKeyDataType + totalNumBytesRead, bytesToReadInNextPage, cursor,
            idxInPageListToListPageIdxMapper);
    }
}

void UnstructuredPropertyLists::readPropertyValue(Value* propertyValue, uint64_t dataTypeSize,
    PageByteCursor& cursor, const function<uint32_t(uint32_t)>& idxInPageListToListPageIdxMapper) {
    auto totalNumBytesRead = 0u;
    auto bytesInCurrentPage = DEFAULT_PAGE_SIZE - cursor.offsetInPage;
    auto bytesToReadInCurrentPage = min(dataTypeSize, bytesInCurrentPage);
    readFromAPage(((uint8_t*)&propertyValue->val), bytesToReadInCurrentPage, cursor,
        idxInPageListToListPageIdxMapper);
    totalNumBytesRead += bytesToReadInCurrentPage;
    if (dataTypeSize > totalNumBytesRead) { // move to next page
        cursor.pageIdx++;
        cursor.offsetInPage = 0;
        auto bytesToReadInNextPage = dataTypeSize - totalNumBytesRead;
        readFromAPage(((uint8_t*)&propertyValue->val) + totalNumBytesRead, bytesToReadInNextPage,
            cursor, idxInPageListToListPageIdxMapper);
    }
}

void UnstructuredPropertyLists::readFromAPage(uint8_t* value, uint64_t bytesToRead,
    PageByteCursor& cursor,
    const std::function<uint32_t(uint32_t)>& idxInPageListToListPageIdxMapper) {
    uint64_t physicalPageIdx = idxInPageListToListPageIdxMapper(cursor.pageIdx);
    auto frame = bufferManager.pin(fileHandle, physicalPageIdx);
    memcpy(value, frame + cursor.offsetInPage, bytesToRead);
    bufferManager.unpin(fileHandle, physicalPageIdx);
    cursor.offsetInPage += bytesToRead;
}

void UnstructuredPropertyLists::setPropertyListEmpty(node_offset_t nodeOffset) {
    lock_t lck{mtx};
    localUpdatedLists.setEmptyUpdatedPropertiesList(nodeOffset);
}

void UnstructuredPropertyLists::setOrRemoveProperty(
    node_offset_t nodeOffset, uint32_t propertyKey, bool isSetting, Value* value) {
    lock_t lck{mtx};
    if (!localUpdatedLists.hasUpdatedList(nodeOffset)) {
        CursorAndMapper cursorAndMapper;
        cursorAndMapper.reset(
            metadata, numElementsPerPage, headers->getHeader(nodeOffset), nodeOffset);
        auto numElementsInList = getNumElementsInPersistentStore(nodeOffset);
        uint64_t updatedListCapacity = max(numElementsInList,
            (uint64_t)(numElementsInList * StorageConfig::ARRAY_RESIZING_FACTOR));
        InMemList inMemList{updatedListCapacity, elementSize, false /* requireNullMask */};
        fillListsFromPersistentStore(cursorAndMapper, numElementsInList, inMemList);
        if (isSetting) {
            localUpdatedLists.setPropertyList(
                nodeOffset, make_unique<UnstrPropListWrapper>(
                                move(inMemList.listData), numElementsInList, updatedListCapacity));
        } else if (!localUpdatedLists.hasUpdatedList(nodeOffset)) {
            unique_ptr<UnstrPropListWrapper> unstrListWrapper = make_unique<UnstrPropListWrapper>(
                move(inMemList.listData), numElementsInList, updatedListCapacity);
            bool found = UnstrPropListUtils::findKeyPropertyAndPerformOp(
                unstrListWrapper.get(), propertyKey, [](UnstrPropListIterator& itr) -> void {});
            if (found) {
                localUpdatedLists.setPropertyList(nodeOffset, move(unstrListWrapper));
                localUpdatedLists.removeProperty(nodeOffset, propertyKey);
            }
        }
    } else if (!isSetting) {
        localUpdatedLists.removeProperty(nodeOffset, propertyKey);
    }
    if (isSetting) {
        localUpdatedLists.setProperty(nodeOffset, propertyKey, value);
    }
}

void UnstructuredPropertyLists::prepareCommitOrRollbackIfNecessary(bool isCommit) {
    if (localUpdatedLists.updatedChunks.empty()) {
        return;
    }
    // Note: We need to add this list to WAL's set of updatedUnstructuredPropertyLists here instead
    // of for example during WALReplayer when modifying pages for the following reason: Note that
    // until this function is called, no updates to the files of UnstructuredPropertyLists has
    // been made. That is, so far there are no log records in WAL to indicate a change to this
    // UnstructuredPropertyLists. Therefore suppose a transaction makes changes, which results in
    // changes to this UnstructuredPropertyLists but then rolls back. Then since there are no log
    // records, we cannot rely on the log for the WALReplayer to know that we need to rollback this
    // UnstructuredPropertyLists in memory. Therefore, we need to manually add this
    // UnstructuredPropertyLists to the set of UnstructuredPropertyLists to rollback when
    // the Database class calls storageManager->prepareListsToCommitOrRollbackIfNecessary, which
    // blindly calls each UnstructuredPropertyList to check if they have something to commit or
    // rollback.
    wal->addToUpdatedUnstructuredPropertyLists(
        storageStructureIDAndFName.storageStructureID.listFileID);
    UnstructuredPropertyListsUpdateIterator updateItr(this);
    if (isCommit) {
        // Note: In C++ iterating through maps happens in non-descending order of the keys. This
        // property is critical when using UnstructuredPropertyListsUpdateIterator, which requires
        // the user to make calls to writeListToListPages in ascending order of nodeOffsets.
        for (auto updatedChunkItr = localUpdatedLists.updatedChunks.begin();
             updatedChunkItr != localUpdatedLists.updatedChunks.end(); ++updatedChunkItr) {
            for (auto updatedNodeOffsetItr = updatedChunkItr->second->begin();
                 updatedNodeOffsetItr != updatedChunkItr->second->end(); updatedNodeOffsetItr++) {
                InMemList inMemList{
                    updatedNodeOffsetItr->second->size, elementSize, false /* requireNullMask */};
                memcpy(inMemList.getListData(), updatedNodeOffsetItr->second->data.get(),
                    updatedNodeOffsetItr->second->size * elementSize);
                updateItr.updateList(updatedNodeOffsetItr->first, inMemList);
            }
        }
    }
    updateItr.doneUpdating();
}

void UnstructuredPropertyLists::checkpointInMemoryIfNecessary() {
    if (localUpdatedLists.updatedChunks.empty()) {
        return;
    }
    Lists::checkpointInMemoryIfNecessary();
    localUpdatedLists.clear();
}

void UnstructuredPropertyLists::rollbackInMemoryIfNecessary() {
    if (localUpdatedLists.updatedChunks.empty()) {
        return;
    }
    Lists::rollbackInMemoryIfNecessary();
    localUpdatedLists.clear();
}

} // namespace storage
} // namespace graphflow

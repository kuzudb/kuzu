#include "src/storage/storage_structure/include/lists/unstructured_property_lists.h"

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
    if (transaction->isReadOnly() || !updatedLists.hasUpdatedList(nodeOffset)) {
        auto info = getListInfo(nodeOffset);
        auto primaryStoreData = make_unique<uint8_t[]>(info.listLen);
        fillUnstrPropListFromPrimaryStore(info, primaryStoreData.get());
        primaryStoreListWrapper =
            make_unique<UnstrPropListWrapper>(move(primaryStoreData), info.listLen, info.listLen);
        itr = UnstrPropListIterator(primaryStoreListWrapper.get());
    } else {
        itr = updatedLists.getUpdatedListIterator(nodeOffset);
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
                stringOverflowPages.readStringToVector(
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

void UnstructuredPropertyLists::fillUnstrPropListFromPrimaryStore(
    ListInfo& info, uint8_t* dataToFill) {
    PageElementCursor cursor{info.cursor.pageIdx, info.cursor.posInPage};
    uint64_t numBytesRead = 0;
    while (numBytesRead < info.listLen) {
        auto bytesToReadInCurrentPage =
            min(info.listLen - numBytesRead, DEFAULT_PAGE_SIZE - cursor.posInPage);
        auto physicalPageIdx = info.mapper(cursor.pageIdx);
        auto frame = bufferManager.pin(fileHandle, physicalPageIdx);
        std::copy(frame + cursor.posInPage, frame + cursor.posInPage + bytesToReadInCurrentPage,
            dataToFill + numBytesRead);
        bufferManager.unpin(fileHandle, physicalPageIdx);
        numBytesRead += bytesToReadInCurrentPage;
        cursor.nextPage();
    }
}

unique_ptr<map<uint32_t, Literal>> UnstructuredPropertyLists::readUnstructuredPropertiesOfNode(
    node_offset_t nodeOffset) {
    auto info = getListInfo(nodeOffset);
    auto retVal = make_unique<map<uint32_t /*unstructuredProperty pageIdx*/, Literal>>();
    PageByteCursor byteCursor{info.cursor.pageIdx, info.cursor.posInPage};
    auto propertyKeyDataType = UnstructuredPropertyKeyDataType{UINT32_MAX, ANY};
    auto numBytesRead = 0u;
    while (numBytesRead < info.listLen) {
        readPropertyKeyAndDatatype((uint8_t*)(&propertyKeyDataType), byteCursor, info.mapper);
        numBytesRead += StorageConfig::UNSTR_PROP_HEADER_LEN;
        auto dataTypeSize = Types::getDataTypeSize(propertyKeyDataType.dataTypeID);
        Value unstrPropertyValue{DataType(propertyKeyDataType.dataTypeID)};
        readPropertyValue(&unstrPropertyValue,
            Types::getDataTypeSize(propertyKeyDataType.dataTypeID), byteCursor, info.mapper);
        numBytesRead += dataTypeSize;
        Literal propertyValueAsLiteral;
        if (STRING == propertyKeyDataType.dataTypeID) {
            propertyValueAsLiteral =
                Literal(stringOverflowPages.readString(unstrPropertyValue.val.strVal));
        } else {
            propertyValueAsLiteral = Literal(
                (uint8_t*)&unstrPropertyValue.val, DataType(propertyKeyDataType.dataTypeID));
        }
        retVal->insert(pair<uint32_t, Literal>(propertyKeyDataType.keyIdx, propertyValueAsLiteral));
    }
    return retVal;
}

void UnstructuredPropertyLists::readPropertyKeyAndDatatype(uint8_t* propertyKeyDataType,
    PageByteCursor& cursor, const function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper) {
    auto totalNumBytesRead = 0u;
    auto bytesInCurrentPage = DEFAULT_PAGE_SIZE - cursor.offsetInPage;
    auto bytesToReadInCurrentPage =
        min((uint64_t)StorageConfig::UNSTR_PROP_HEADER_LEN, bytesInCurrentPage);
    readFromAPage(
        propertyKeyDataType, bytesToReadInCurrentPage, cursor, logicalToPhysicalPageMapper);
    totalNumBytesRead += bytesToReadInCurrentPage;
    if (StorageConfig::UNSTR_PROP_HEADER_LEN > totalNumBytesRead) { // move to next page
        cursor.pageIdx++;
        cursor.offsetInPage = 0;
        auto bytesToReadInNextPage = StorageConfig::UNSTR_PROP_HEADER_LEN - totalNumBytesRead;
        // IMPORTANT NOTE: Pranjal used to use bytesInCurrentPage instead of totalNumBytesRead
        // in the following function. Xiyang think this is a bug and modify it.
        readFromAPage(propertyKeyDataType + totalNumBytesRead, bytesToReadInNextPage, cursor,
            logicalToPhysicalPageMapper);
    }
}

void UnstructuredPropertyLists::readPropertyValue(Value* propertyValue, uint64_t dataTypeSize,
    PageByteCursor& cursor, const function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper) {
    auto totalNumBytesRead = 0u;
    auto bytesInCurrentPage = DEFAULT_PAGE_SIZE - cursor.offsetInPage;
    auto bytesToReadInCurrentPage = min(dataTypeSize, bytesInCurrentPage);
    readFromAPage(((uint8_t*)&propertyValue->val), bytesToReadInCurrentPage, cursor,
        logicalToPhysicalPageMapper);
    totalNumBytesRead += bytesToReadInCurrentPage;
    if (dataTypeSize > totalNumBytesRead) { // move to next page
        cursor.pageIdx++;
        cursor.offsetInPage = 0;
        auto bytesToReadInNextPage = dataTypeSize - totalNumBytesRead;
        readFromAPage(((uint8_t*)&propertyValue->val) + totalNumBytesRead, bytesToReadInNextPage,
            cursor, logicalToPhysicalPageMapper);
    }
}

void UnstructuredPropertyLists::readFromAPage(uint8_t* value, uint64_t bytesToRead,
    PageByteCursor& cursor, const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper) {
    uint64_t physicalPageIdx = logicalToPhysicalPageMapper(cursor.pageIdx);
    auto frame = bufferManager.pin(fileHandle, physicalPageIdx);
    memcpy(value, frame + cursor.offsetInPage, bytesToRead);
    bufferManager.unpin(fileHandle, physicalPageIdx);
    cursor.offsetInPage += bytesToRead;
}

void UnstructuredPropertyLists::setPropertyListEmpty(node_offset_t nodeOffset) {
    lock_t lck{mtx};
    updatedLists.setEmptyUpdatedPropertiesList(nodeOffset);
}

void UnstructuredPropertyLists::setProperty(
    node_offset_t nodeOffset, uint32_t propertyKey, Value* value) {
    lock_t lck{mtx};
    if (!updatedLists.hasUpdatedList(nodeOffset)) {
        auto info = getListInfo(nodeOffset);
        uint64_t updatedListCapacity =
            max(info.listLen, (uint64_t)(info.listLen * StorageConfig::ARRAY_RESIZING_FACTOR));
        unique_ptr<uint8_t[]> existingUstrPropLists = make_unique<uint8_t[]>(updatedListCapacity);
        fillUnstrPropListFromPrimaryStore(info, existingUstrPropLists.get());
        updatedLists.setPropertyList(
            nodeOffset, make_unique<UnstrPropListWrapper>(
                            move(existingUstrPropLists), info.listLen, updatedListCapacity));
    }
    updatedLists.setProperty(nodeOffset, propertyKey, value);
}

void UnstructuredPropertyLists::removeProperty(node_offset_t nodeOffset, uint32_t propertyKey) {
    lock_t lck{mtx};
    if (!updatedLists.hasUpdatedList(nodeOffset)) {
        auto info = getListInfo(nodeOffset);
        uint64_t updatedListCapacity =
            max(info.listLen, (uint64_t)(info.listLen * StorageConfig::ARRAY_RESIZING_FACTOR));
        unique_ptr<uint8_t[]> originalPropLists = make_unique<uint8_t[]>(updatedListCapacity);
        fillUnstrPropListFromPrimaryStore(info, originalPropLists.get());
        unique_ptr<UnstrPropListWrapper> unstrListWrapper = make_unique<UnstrPropListWrapper>(
            move(originalPropLists), info.listLen, updatedListCapacity);
        bool found = UnstrPropListUtils::findKeyPropertyAndPerformOp(
            unstrListWrapper.get(), propertyKey, [](UnstrPropListIterator& itr) -> void {});
        if (found) {
            updatedLists.setPropertyList(nodeOffset, move(unstrListWrapper));
            updatedLists.removeProperty(nodeOffset, propertyKey);
        }
    } else {
        updatedLists.removeProperty(nodeOffset, propertyKey);
    }
}

} // namespace storage
} // namespace graphflow

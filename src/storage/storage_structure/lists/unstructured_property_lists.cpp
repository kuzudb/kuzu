#include "src/storage/storage_structure/include/lists/unstructured_property_lists.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

void UnstructuredPropertyLists::readProperties(ValueVector* nodeIDVector,
    const unordered_map<uint32_t, ValueVector*>& propertyKeyToResultVectorMap) {
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->getPositionOfCurrIdx();
        readPropertiesForPosition(nodeIDVector, pos, propertyKeyToResultVectorMap);
    } else {
        for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            readPropertiesForPosition(nodeIDVector, pos, propertyKeyToResultVectorMap);
        }
    }
}

void UnstructuredPropertyLists::readPropertiesForPosition(ValueVector* nodeIDVector, uint32_t pos,
    const unordered_map<uint32_t, ValueVector*>& propertyKeyToResultVectorMap) {
    if (nodeIDVector->isNull(pos)) {
        for (auto& [key, vector] : propertyKeyToResultVectorMap) {
            vector->setNull(pos, true);
        }
        return;
    }
    unordered_set<uint32_t> propertyKeysFound;
    auto info = getListInfo(nodeIDVector->readNodeOffset(pos));
    PageByteCursor cursor{info.cursor.pageIdx, info.cursor.posInPage};
    auto propertyKeyDataType = UnstructuredPropertyKeyDataType{UINT32_MAX, ANY};
    auto numBytesRead = 0u;
    while (numBytesRead < info.listLen) {
        readPropertyKeyAndDatatype((uint8_t*)&propertyKeyDataType, cursor, info.mapper);
        numBytesRead += UNSTR_PROP_HEADER_LEN;
        auto dataTypeSize = Types::getDataTypeSize(propertyKeyDataType.dataTypeID);
        if (propertyKeyToResultVectorMap.contains(propertyKeyDataType.keyIdx)) {
            propertyKeysFound.insert(propertyKeyDataType.keyIdx);
            auto vector = propertyKeyToResultVectorMap.at(propertyKeyDataType.keyIdx);
            vector->setNull(pos, false);
            auto value = &((Value*)vector->values)[pos];
            readPropertyValue(value, dataTypeSize, cursor, info.mapper);
            value->dataType.typeID = propertyKeyDataType.dataTypeID;
            if (propertyKeyDataType.dataTypeID == STRING) {
                stringOverflowPages.readStringToVector(
                    value->val.strVal, vector->getOverflowBuffer());
            }
        } else {
            skipPropertyValue(dataTypeSize, cursor);
        }
        numBytesRead += dataTypeSize;
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
    auto info = getListInfo(nodeOffset);
    auto retVal = make_unique<map<uint32_t /*unstructuredProperty pageIdx*/, Literal>>();
    PageByteCursor byteCursor{info.cursor.pageIdx, info.cursor.posInPage};
    auto propertyKeyDataType = UnstructuredPropertyKeyDataType{UINT32_MAX, ANY};
    auto numBytesRead = 0u;
    while (numBytesRead < info.listLen) {
        readPropertyKeyAndDatatype((uint8_t*)(&propertyKeyDataType), byteCursor, info.mapper);
        numBytesRead += UNSTR_PROP_HEADER_LEN;
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
    auto bytesToReadInCurrentPage = min((uint64_t)UNSTR_PROP_HEADER_LEN, bytesInCurrentPage);
    readFromAPage(
        propertyKeyDataType, bytesToReadInCurrentPage, cursor, logicalToPhysicalPageMapper);
    totalNumBytesRead += bytesToReadInCurrentPage;
    if (UNSTR_PROP_HEADER_LEN > totalNumBytesRead) { // move to next page
        cursor.pageIdx++;
        cursor.offsetInPage = 0;
        auto bytesToReadInNextPage = UNSTR_PROP_HEADER_LEN - totalNumBytesRead;
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
        // See line 107
        readFromAPage(((uint8_t*)&propertyValue->val) + totalNumBytesRead, bytesToReadInNextPage,
            cursor, logicalToPhysicalPageMapper);
    }
}

void UnstructuredPropertyLists::skipPropertyValue(uint64_t dataTypeSize, PageByteCursor& cursor) {
    auto bytesToReadInCurrentPage = min(dataTypeSize, DEFAULT_PAGE_SIZE - cursor.offsetInPage);
    cursor.offsetInPage += bytesToReadInCurrentPage;
    if (dataTypeSize > bytesToReadInCurrentPage) {
        cursor.pageIdx++;
        cursor.offsetInPage = dataTypeSize - bytesToReadInCurrentPage;
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

} // namespace storage
} // namespace graphflow

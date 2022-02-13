#include "src/storage/include/data_structure/lists/unstructured_property_lists.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

void UnstructuredPropertyLists::readProperties(ValueVector* nodeIDVector,
    const unordered_map<uint32_t, ValueVector*>& propertyKeyToResultVectorMap,
    BufferManagerMetrics& metrics) {
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->getPositionOfCurrIdx();
        readPropertiesForPosition(nodeIDVector, pos, propertyKeyToResultVectorMap, metrics);
    } else {
        for (auto i = 0u; i < nodeIDVector->state->selectedSize; ++i) {
            auto pos = nodeIDVector->state->selectedPositions[i];
            readPropertiesForPosition(nodeIDVector, pos, propertyKeyToResultVectorMap, metrics);
        }
    }
}

void UnstructuredPropertyLists::readPropertiesForPosition(ValueVector* nodeIDVector, uint32_t pos,
    const unordered_map<uint32_t, ValueVector*>& propertyKeyToResultVectorMap,
    BufferManagerMetrics& metrics) {
    if (nodeIDVector->isNull(pos)) {
        for (auto& [key, vector] : propertyKeyToResultVectorMap) {
            vector->setNull(pos, true);
        }
        return;
    }
    unordered_set<uint32_t> propertyKeysFound;
    auto info = getListInfo(nodeIDVector->readNodeOffset(pos));
    PageByteCursor cursor{info.cursor.idx, info.cursor.pos};
    auto propertyKeyDataType = UnstructuredPropertyKeyDataType{UINT32_MAX, INVALID};
    auto numBytesRead = 0u;
    while (numBytesRead < info.listLen) {
        readPropertyKeyAndDatatype((uint8_t*)&propertyKeyDataType, cursor, info.mapper, metrics);
        numBytesRead += UNSTR_PROP_HEADER_LEN;
        auto dataTypeSize = TypeUtils::getDataTypeSize(propertyKeyDataType.dataType);
        if (propertyKeyToResultVectorMap.contains(propertyKeyDataType.keyIdx)) {
            propertyKeysFound.insert(propertyKeyDataType.keyIdx);
            auto vector = propertyKeyToResultVectorMap.at(propertyKeyDataType.keyIdx);
            vector->setNull(pos, false);
            auto value = &((Value*)vector->values)[pos];
            readPropertyValue(value, dataTypeSize, cursor, info.mapper, metrics);
            value->dataType = propertyKeyDataType.dataType;
            if (propertyKeyDataType.dataType == STRING) {
                stringOverflowPages.readStringToVector(
                    value->val.strVal, *vector->stringBuffer, metrics);
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
    node_offset_t nodeOffset, BufferManagerMetrics& metrics) {
    auto info = getListInfo(nodeOffset);
    auto retVal = make_unique<map<uint32_t /*unstructuredProperty idx*/, Literal>>();
    PageByteCursor byteCursor{info.cursor.idx, info.cursor.pos};
    auto propertyKeyDataType = UnstructuredPropertyKeyDataType{UINT32_MAX, INVALID};
    auto numBytesRead = 0u;
    while (numBytesRead < info.listLen) {
        readPropertyKeyAndDatatype(
            (uint8_t*)(&propertyKeyDataType), byteCursor, info.mapper, metrics);
        numBytesRead += UNSTR_PROP_HEADER_LEN;
        auto dataTypeSize = TypeUtils::getDataTypeSize(propertyKeyDataType.dataType);
        Value unstrPropertyValue{propertyKeyDataType.dataType};
        readPropertyValue(&unstrPropertyValue,
            TypeUtils::getDataTypeSize(propertyKeyDataType.dataType), byteCursor, info.mapper,
            metrics);
        numBytesRead += dataTypeSize;
        Literal propertyValueAsLiteral;
        propertyValueAsLiteral.dataType = unstrPropertyValue.dataType;
        if (STRING == propertyKeyDataType.dataType) {
            propertyValueAsLiteral.strVal =
                stringOverflowPages.readString(unstrPropertyValue.val.strVal, metrics);
        } else {
            memcpy(&propertyValueAsLiteral.val, &unstrPropertyValue.val,
                TypeUtils::getDataTypeSize(propertyKeyDataType.dataType));
        }
        retVal->insert(pair<uint32_t, Literal>(propertyKeyDataType.keyIdx, propertyValueAsLiteral));
    }
    return retVal;
}

void UnstructuredPropertyLists::readPropertyKeyAndDatatype(uint8_t* propertyKeyDataType,
    PageByteCursor& cursor, const function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper,
    BufferManagerMetrics& metrics) {
    auto totalNumBytesRead = 0u;
    auto bytesInCurrentPage = PAGE_SIZE - cursor.offset;
    auto bytesToReadInCurrentPage = min((uint64_t)UNSTR_PROP_HEADER_LEN, bytesInCurrentPage);
    readFromAPage(propertyKeyDataType, bytesToReadInCurrentPage, cursor,
        logicalToPhysicalPageMapper, metrics);
    totalNumBytesRead += bytesToReadInCurrentPage;
    if (UNSTR_PROP_HEADER_LEN > totalNumBytesRead) { // move to next page
        cursor.idx++;
        cursor.offset = 0;
        auto bytesToReadInNextPage = UNSTR_PROP_HEADER_LEN - totalNumBytesRead;
        // IMPORTANT NOTE: Pranjal used to use bytesInCurrentPage instead of totalNumBytesRead
        // in the following function. Xiyang think this is a bug and modify it.
        readFromAPage(propertyKeyDataType + totalNumBytesRead, bytesToReadInNextPage, cursor,
            logicalToPhysicalPageMapper, metrics);
    }
}

void UnstructuredPropertyLists::readPropertyValue(Value* propertyValue, uint64_t dataTypeSize,
    PageByteCursor& cursor, const function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper,
    BufferManagerMetrics& metrics) {
    auto totalNumBytesRead = 0u;
    auto bytesInCurrentPage = PAGE_SIZE - cursor.offset;
    auto bytesToReadInCurrentPage = min(dataTypeSize, bytesInCurrentPage);
    readFromAPage(((uint8_t*)&propertyValue->val), bytesToReadInCurrentPage, cursor,
        logicalToPhysicalPageMapper, metrics);
    totalNumBytesRead += bytesToReadInCurrentPage;
    if (dataTypeSize > totalNumBytesRead) { // move to next page
        cursor.idx++;
        cursor.offset = 0;
        auto bytesToReadInNextPage = dataTypeSize - totalNumBytesRead;
        // See line 107
        readFromAPage(((uint8_t*)&propertyValue->val) + totalNumBytesRead, bytesToReadInNextPage,
            cursor, logicalToPhysicalPageMapper, metrics);
    }
}

void UnstructuredPropertyLists::skipPropertyValue(uint64_t dataTypeSize, PageByteCursor& cursor) {
    auto bytesToReadInCurrentPage = min(dataTypeSize, PAGE_SIZE - cursor.offset);
    cursor.offset += bytesToReadInCurrentPage;
    if (dataTypeSize > bytesToReadInCurrentPage) {
        cursor.idx++;
        cursor.offset = dataTypeSize - bytesToReadInCurrentPage;
    }
}

void UnstructuredPropertyLists::readFromAPage(uint8_t* value, uint64_t bytesToRead,
    PageByteCursor& cursor, const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper,
    BufferManagerMetrics& metrics) {
    uint64_t physicalPageIdx = logicalToPhysicalPageMapper(cursor.idx);
    auto frame = bufferManager.pin(fileHandle, physicalPageIdx, metrics);
    memcpy(value, frame + cursor.offset, bytesToRead);
    bufferManager.unpin(fileHandle, physicalPageIdx);
    cursor.offset += bytesToRead;
}

} // namespace storage
} // namespace graphflow

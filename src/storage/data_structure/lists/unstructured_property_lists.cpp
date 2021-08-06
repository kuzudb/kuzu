#include "src/storage/include/data_structure/lists/unstructured_property_lists.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

void UnstructuredPropertyLists::readUnstructuredProperties(
    const shared_ptr<NodeIDVector>& nodeIDVector, uint32_t propertyKeyIdxToRead,
    const shared_ptr<ValueVector>& valueVector, BufferManagerMetrics& metrics) {
    node_offset_t nodeOffset;
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->getPositionOfCurrIdx();
        nodeOffset = nodeIDVector->readNodeOffset(pos);
        auto info = getListInfo(nodeOffset);
        readUnstrPropertyFromAList(propertyKeyIdxToRead, valueVector, pos, info, metrics);
    } else {
        for (auto i = 0ul; i < nodeIDVector->state->selectedSize; i++) {
            auto pos = nodeIDVector->state->selectedPositions[i];
            nodeOffset = nodeIDVector->readNodeOffset(pos);
            auto info = getListInfo(nodeOffset);
            readUnstrPropertyFromAList(propertyKeyIdxToRead, valueVector, pos, info, metrics);
        }
    }
}

unique_ptr<map<uint32_t, Literal>> UnstructuredPropertyLists::readUnstructuredPropertiesOfNode(
    node_offset_t nodeOffset, BufferManagerMetrics& metrics) {
    auto info = getListInfo(nodeOffset);
    auto retVal = make_unique<map<uint32_t /*unstructuredProperty idx*/, Literal>>();
    while (info.listLen) {
        UnstructuredPropertyKeyDataType propertyKeyDataType;
        readUnstrPropertyKeyIdxAndDatatype(reinterpret_cast<uint8_t*>(&propertyKeyDataType),
            info.cursor, info.listLen, info.mapper, metrics);
        Value unstrPropertyValue{propertyKeyDataType.dataType};
        readOrSkipUnstrPropertyValue(propertyKeyDataType.dataType, info.cursor, info.listLen,
            info.mapper, &unstrPropertyValue, true /*to read*/, metrics);
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

void UnstructuredPropertyLists::readUnstrPropertyFromAList(uint32_t propertyKeyIdxToRead,
    const shared_ptr<ValueVector>& valueVector, uint64_t pos, ListInfo& info,
    BufferManagerMetrics& metrics) {
    while (info.listLen) {
        UnstructuredPropertyKeyDataType propertyKeyDataType;
        readUnstrPropertyKeyIdxAndDatatype(reinterpret_cast<uint8_t*>(&propertyKeyDataType),
            info.cursor, info.listLen, info.mapper, metrics);
        Value* value = &((Value*)valueVector->values)[pos];
        readOrSkipUnstrPropertyValue(propertyKeyDataType.dataType, info.cursor, info.listLen,
            info.mapper, value,
            propertyKeyIdxToRead ==
                propertyKeyDataType.keyIdx /* read if we found the key. skip otherwise. */,
            metrics);
        if (propertyKeyIdxToRead == propertyKeyDataType.keyIdx) {
            valueVector->setNull(pos, false);
            value->dataType = propertyKeyDataType.dataType;
            if (STRING == propertyKeyDataType.dataType) {
                stringOverflowPages.readStringToVector(*valueVector, pos, metrics);
            }
            // found the property, exiting.
            return;
        }
    }
    // We did not find the key. We set the value to null.
    valueVector->setNull(pos, true);
}

void UnstructuredPropertyLists::readUnstrPropertyKeyIdxAndDatatype(uint8_t* propertyKeyDataType,
    PageCursor& pageCursor, uint64_t& listLen,
    const function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper,
    BufferManagerMetrics& metrics) {
    uint32_t bytesLeftToRead = UNSTR_PROP_HEADER_LEN;
    uint64_t physicalPageIdx = logicalToPhysicalPageMapper(pageCursor.idx);
    auto frame = bufferManager.pin(fileHandle, physicalPageIdx, metrics);
    uint32_t bytesInCurrentFrame = PAGE_SIZE - pageCursor.offset;
    auto bytesToRead = min(bytesLeftToRead, (uint32_t)bytesInCurrentFrame);
    memcpy(propertyKeyDataType, frame + pageCursor.offset, bytesToRead);
    bufferManager.unpin(fileHandle, physicalPageIdx);
    bytesLeftToRead -= bytesToRead;
    if (bytesLeftToRead > 0) {
        physicalPageIdx = logicalToPhysicalPageMapper(++pageCursor.idx);
        frame = bufferManager.pin(fileHandle, physicalPageIdx, metrics);
        memcpy(propertyKeyDataType + bytesInCurrentFrame, frame, bytesLeftToRead);
        pageCursor.offset = bytesLeftToRead;
        bufferManager.unpin(fileHandle, physicalPageIdx);
    } else {
        pageCursor.offset += UNSTR_PROP_HEADER_LEN;
    }
    listLen -= UNSTR_PROP_HEADER_LEN;
}

void UnstructuredPropertyLists::readOrSkipUnstrPropertyValue(DataType& propertyDataType,
    PageCursor& pageCursor, uint64_t& listLen,
    const function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper, Value* value, bool toRead,
    BufferManagerMetrics& metrics) {
    uint32_t dataTypeSize = TypeUtils::getDataTypeSize(propertyDataType);
    uint32_t bytesLeftToRead = dataTypeSize;
    uint64_t physicalPageIdx = logicalToPhysicalPageMapper(pageCursor.idx);
    auto frame = bufferManager.pin(fileHandle, physicalPageIdx, metrics);
    uint32_t bytesInCurrentFrame = PAGE_SIZE - pageCursor.offset;
    auto bytesToRead = min(bytesLeftToRead, (uint32_t)bytesInCurrentFrame);
    if (toRead) {
        memcpy((uint8_t*)&value->val, frame + pageCursor.offset, bytesToRead);
    }
    bufferManager.unpin(fileHandle, physicalPageIdx);
    bytesLeftToRead -= bytesToRead;
    if (bytesLeftToRead > 0) {
        physicalPageIdx = logicalToPhysicalPageMapper(++pageCursor.idx);
        frame = bufferManager.pin(fileHandle, physicalPageIdx, metrics);
        if (toRead) {
            memcpy(((uint8_t*)&value->val) + bytesInCurrentFrame, frame, bytesLeftToRead);
        }
        pageCursor.offset = bytesLeftToRead;
        bufferManager.unpin(fileHandle, physicalPageIdx);
    } else {
        pageCursor.offset += dataTypeSize;
    }
    listLen -= dataTypeSize;
}

} // namespace storage
} // namespace graphflow

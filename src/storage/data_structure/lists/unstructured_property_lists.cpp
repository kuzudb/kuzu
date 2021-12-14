#include "src/storage/include/data_structure/lists/unstructured_property_lists.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

void UnstructuredPropertyLists::readUnstructuredProperties(
    const shared_ptr<ValueVector>& nodeIDVector, uint32_t propertyKeyIdxToRead,
    const shared_ptr<ValueVector>& valueVector, BufferManagerMetrics& metrics) {
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->getPositionOfCurrIdx();
        readUnstructuredPropertyForSingleNodeIDPosition(
            pos, nodeIDVector, propertyKeyIdxToRead, valueVector, metrics);
    } else {
        for (auto i = 0ul; i < nodeIDVector->state->selectedSize; i++) {
            auto pos = nodeIDVector->state->selectedPositions[i];
            readUnstructuredPropertyForSingleNodeIDPosition(
                pos, nodeIDVector, propertyKeyIdxToRead, valueVector, metrics);
        }
    }
}

void UnstructuredPropertyLists::readUnstructuredPropertyForSingleNodeIDPosition(uint32_t pos,
    const shared_ptr<ValueVector>& nodeIDVector, uint32_t propertyKeyIdxToRead,
    const shared_ptr<ValueVector>& valueVector, BufferManagerMetrics& metrics) {
    if (nodeIDVector->isNull(pos)) {
        valueVector->setNull(pos, true);
        return;
    }
    node_offset_t nodeOffset;
    nodeOffset = nodeIDVector->readNodeOffset(pos);
    auto info = getListInfo(nodeOffset);
    PageByteCursor byteCursor{info.cursor.idx, info.cursor.pos};
    while (info.listLen) {
        UnstructuredPropertyKeyDataType propertyKeyDataType;
        readUnstrPropertyKeyIdxAndDatatype(reinterpret_cast<uint8_t*>(&propertyKeyDataType),
            byteCursor, info.listLen, info.mapper, metrics);
        Value* value = &((Value*)valueVector->values)[pos];
        readOrSkipUnstrPropertyValue(propertyKeyDataType.dataType, byteCursor, info.listLen,
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

unique_ptr<map<uint32_t, Literal>> UnstructuredPropertyLists::readUnstructuredPropertiesOfNode(
    node_offset_t nodeOffset, BufferManagerMetrics& metrics) {
    auto info = getListInfo(nodeOffset);
    auto retVal = make_unique<map<uint32_t /*unstructuredProperty idx*/, Literal>>();
    PageByteCursor byteCursor{info.cursor.idx, info.cursor.pos};
    while (info.listLen) {
        UnstructuredPropertyKeyDataType propertyKeyDataType;
        readUnstrPropertyKeyIdxAndDatatype(reinterpret_cast<uint8_t*>(&propertyKeyDataType),
            byteCursor, info.listLen, info.mapper, metrics);
        Value unstrPropertyValue{propertyKeyDataType.dataType};
        readOrSkipUnstrPropertyValue(propertyKeyDataType.dataType, byteCursor, info.listLen,
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

void UnstructuredPropertyLists::readUnstrPropertyKeyIdxAndDatatype(uint8_t* propertyKeyDataType,
    PageByteCursor& cursor, uint64_t& listLen,
    const function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper,
    BufferManagerMetrics& metrics) {
    uint32_t bytesLeftToRead = UNSTR_PROP_HEADER_LEN;
    uint64_t physicalPageIdx = logicalToPhysicalPageMapper(cursor.idx);
    auto frame = bufferManager.pin(fileHandle, physicalPageIdx, metrics);
    uint32_t bytesInCurrentFrame = PAGE_SIZE - cursor.offset;
    auto bytesToRead = min(bytesLeftToRead, (uint32_t)bytesInCurrentFrame);
    memcpy(propertyKeyDataType, frame + cursor.offset, bytesToRead);
    bufferManager.unpin(fileHandle, physicalPageIdx);
    bytesLeftToRead -= bytesToRead;
    if (bytesLeftToRead > 0) {
        physicalPageIdx = logicalToPhysicalPageMapper(++cursor.idx);
        frame = bufferManager.pin(fileHandle, physicalPageIdx, metrics);
        memcpy(propertyKeyDataType + bytesInCurrentFrame, frame, bytesLeftToRead);
        cursor.offset = bytesLeftToRead;
        bufferManager.unpin(fileHandle, physicalPageIdx);
    } else {
        cursor.offset += UNSTR_PROP_HEADER_LEN;
    }
    listLen -= UNSTR_PROP_HEADER_LEN;
}

void UnstructuredPropertyLists::readOrSkipUnstrPropertyValue(DataType& propertyDataType,
    PageByteCursor& cursor, uint64_t& listLen,
    const function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper, Value* value, bool toRead,
    BufferManagerMetrics& metrics) {
    uint32_t dataTypeSize = TypeUtils::getDataTypeSize(propertyDataType);
    uint32_t bytesLeftToRead = dataTypeSize;
    uint64_t physicalPageIdx = logicalToPhysicalPageMapper(cursor.idx);
    auto frame = bufferManager.pin(fileHandle, physicalPageIdx, metrics);
    uint32_t bytesInCurrentFrame = PAGE_SIZE - cursor.offset;
    auto bytesToRead = min(bytesLeftToRead, (uint32_t)bytesInCurrentFrame);
    if (toRead) {
        memcpy((uint8_t*)&value->val, frame + cursor.offset, bytesToRead);
    }
    bufferManager.unpin(fileHandle, physicalPageIdx);
    bytesLeftToRead -= bytesToRead;
    if (bytesLeftToRead > 0) {
        physicalPageIdx = logicalToPhysicalPageMapper(++cursor.idx);
        frame = bufferManager.pin(fileHandle, physicalPageIdx, metrics);
        if (toRead) {
            memcpy(((uint8_t*)&value->val) + bytesInCurrentFrame, frame, bytesLeftToRead);
        }
        cursor.offset = bytesLeftToRead;
        bufferManager.unpin(fileHandle, physicalPageIdx);
    } else {
        cursor.offset += dataTypeSize;
    }
    listLen -= dataTypeSize;
}

} // namespace storage
} // namespace graphflow

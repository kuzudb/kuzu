#include "src/storage/include/structures/lists.h"

#include "src/common/include/value.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

void BaseLists::readValues(const nodeID_t& nodeID, const shared_ptr<ValueVector>& valueVector,
    uint64_t& listLen, const unique_ptr<ColumnOrListsHandle>& handle, uint32_t maxElementsToRead) {
    auto header = headers->getHeader(nodeID.offset);
    if (handle->hasMoreToRead() || ListHeaders::isALargeList(header)) {
        readFromLargeList(nodeID, valueVector, listLen, handle, header, maxElementsToRead);
    } else {
        readSmallList(nodeID, valueVector, listLen, handle, header);
    }
}

void BaseLists::readSmallList(const nodeID_t& nodeID, const shared_ptr<ValueVector>& valueVector,
    uint64_t& listLen, const unique_ptr<ColumnOrListsHandle>& handle, uint32_t header) {
    if (handle->getIsAdjListsHandle()) {
        listLen = ListHeaders::getSmallListLen(header);
    }
    auto pageCursor = getPageCursorForOffset(ListHeaders::getSmallListCSROffset(header));
    auto sizeLeftToCopy = listLen * elementSize;
    uint32_t chunkIdx = nodeID.offset / LISTS_CHUNK_SIZE;
    if (pageCursor.offset + sizeLeftToCopy > PAGE_SIZE) {
        readBySequentialCopy(valueVector, handle, sizeLeftToCopy, pageCursor,
            metadata.getPageMapperForChunkIdx(chunkIdx));
    } else {
        // map logical pageIdx to physical pageIdx
        pageCursor.idx = metadata.getPageIdxFromChunkPagesMap(chunkIdx, pageCursor.idx);
        readBySettingFrame(valueVector, handle, pageCursor);
    }
}

void BaseLists::readFromLargeList(const nodeID_t& nodeID,
    const shared_ptr<ValueVector>& valueVector, uint64_t& listLen,
    const unique_ptr<ColumnOrListsHandle>& handle, uint32_t header, uint32_t maxElementsToRead) {
    auto largeListIdx = ListHeaders::getLargeListIdx(header);
    auto listSyncState = handle->getListSyncState();
    uint32_t sizeLeftToCopy;
    PageCursor pageCursor;
    if (handle->getIsAdjListsHandle()) {
        // Case of reading from AdjList
        auto numElements = metadata.getNumElementsInLargeLists(largeListIdx);
        uint64_t csrOffset;
        if (!handle->hasMoreToRead()) {
            // initialize listSyncState to start tracking a new list.
            listSyncState->init(numElements);
            csrOffset = 0;
        } else {
            csrOffset = listSyncState->getEndIdx();
        }
        pageCursor = getPageCursorForOffset(csrOffset);
        // length of the list to be read, in case of adjLists, is limited to the maximum number of
        // elements that can be read by setting the frame and not doing any copy.
        listLen = min((uint32_t)(numElements - csrOffset),
            min(numElementsPerPage - (uint32_t)(csrOffset % numElementsPerPage),
                maxElementsToRead));
        sizeLeftToCopy = listLen * elementSize;
        listSyncState->set(csrOffset, listLen);
    } else {
        // Case of reading from Property Lists
        pageCursor = getPageCursorForOffset(listSyncState->getStartIdx());
        sizeLeftToCopy = elementSize * listLen;
    }
    if (pageCursor.offset + sizeLeftToCopy > PAGE_SIZE) {
        readBySequentialCopy(valueVector, handle, sizeLeftToCopy, pageCursor,
            metadata.getPageMapperForLargeListIdx(largeListIdx));
    } else {
        // map logical pageIdx to physical pageIdx
        pageCursor.idx = metadata.getPageIdxFromLargeListPagesMap(largeListIdx, pageCursor.idx);
        readBySettingFrame(valueVector, handle, pageCursor);
    }
}

void Lists<UNSTRUCTURED>::readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
    uint32_t propertyKeyIdxToRead, const shared_ptr<ValueVector>& valueVector,
    const unique_ptr<ColumnOrListsHandle>& handle) {
    valueVector->reset();
    nodeID_t nodeID;
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->getCurrSelectedValuesPos();
        nodeIDVector->readNodeOffset(pos, nodeID);
        readUnstrPropertyListOfNode(nodeID, propertyKeyIdxToRead, valueVector, pos, handle,
            headers->getHeader(nodeID.offset));
    } else {
        for (auto i = 0ul; i < valueVector->state->numSelectedValues; i++) {
            nodeIDVector->readNodeOffset(valueVector->state->selectedValuesPos[i], nodeID);
            readUnstrPropertyListOfNode(nodeID, propertyKeyIdxToRead, valueVector,
                valueVector->state->selectedValuesPos[i], handle,
                headers->getHeader(nodeID.offset));
        }
    }
    reclaim(handle);
}

void Lists<UNSTRUCTURED>::readUnstrPropertyListOfNode(const nodeID_t& nodeID,
    uint32_t propertyKeyIdxToRead, const shared_ptr<ValueVector>& valueVector, uint64_t pos,
    const unique_ptr<ColumnOrListsHandle>& handle, uint32_t header) {
    PageCursor pageCursor;
    uint64_t listLen;
    unique_ptr<LogicalToPhysicalPageIdxMapper> mapper;
    if (ListHeaders::isALargeList(header)) {
        auto largeListIdx = ListHeaders::getLargeListIdx(header);
        listLen = metadata.getNumElementsInLargeLists(largeListIdx);
        pageCursor = getPageCursorForOffset(0);
        mapper = move(metadata.getPageMapperForLargeListIdx(largeListIdx));
    } else {
        pageCursor = getPageCursorForOffset(ListHeaders::getSmallListCSROffset(header));
        listLen = ListHeaders::getSmallListLen(header);
        uint32_t chunkIdx = nodeID.offset / LISTS_CHUNK_SIZE;
        mapper = move(metadata.getPageMapperForChunkIdx(chunkIdx));
    }
    auto propertyKeyDataTypeCache = make_unique<uint8_t[]>(5);
    while (listLen) {
        auto physicalPageIdx = mapper->getPageIdx(pageCursor.idx);
        if (handle->getPageIdx() != physicalPageIdx) {
            reclaim(handle);
            bufferManager.pin(fileHandle, physicalPageIdx);
        }
        const uint32_t* propertyKeyIdxPtr;
        DataType propertyDataType;
        readUnstrPropertyKeyIdxAndDatatype(propertyKeyDataTypeCache.get(), physicalPageIdx,
            propertyKeyIdxPtr, propertyDataType, handle, pageCursor, listLen, *mapper);
        if (propertyKeyIdxToRead == *propertyKeyIdxPtr) {
            readOrSkipUnstrPropertyValue(physicalPageIdx, propertyDataType, handle, pageCursor,
                listLen, *mapper, valueVector, pos, true /*to read*/);
            valueVector->nullMask[pos] = false;
            // found the property, exiting.
            return;
        }
        // property not found, skipping the current property value.
        readOrSkipUnstrPropertyValue(physicalPageIdx, propertyDataType, handle, pageCursor, listLen,
            *mapper, valueVector, pos, false /*to read*/);
    }
    valueVector->nullMask[pos] = true;
}

bool Lists<UNSTRUCTURED>::readUnstrPropertyKeyIdxAndDatatype(uint8_t* propertyKeyDataTypeCache,
    uint64_t& physicalPageIdx, const uint32_t*& propertyKeyIdxPtr, DataType& propertyDataType,
    const unique_ptr<ColumnOrListsHandle>& handle, PageCursor& pageCursor, uint32_t listLen,
    LogicalToPhysicalPageIdxMapper& mapper) {
    auto frame = bufferManager.get(fileHandle, physicalPageIdx);
    const uint8_t* readFrom;
    if (pageCursor.offset + PROPERTY_IDX_LEN + PROPERTY_DATATYPE_LEN < PAGE_SIZE) {
        readFrom = frame + pageCursor.offset;
        pageCursor.offset += PROPERTY_IDX_LEN + PROPERTY_DATATYPE_LEN;
    } else {
        auto bytesInCurrentFrame = PAGE_SIZE - pageCursor.offset;
        memcpy(propertyKeyDataTypeCache, frame + pageCursor.offset, bytesInCurrentFrame);
        reclaim(handle);
        physicalPageIdx = mapper.getPageIdx(++pageCursor.idx);
        frame = bufferManager.pin(fileHandle, physicalPageIdx);
        handle->setPageIdx(physicalPageIdx);
        memcpy(propertyKeyDataTypeCache + bytesInCurrentFrame, frame,
            PROPERTY_IDX_LEN + PROPERTY_DATATYPE_LEN - bytesInCurrentFrame);
        pageCursor.offset = PROPERTY_IDX_LEN + PROPERTY_DATATYPE_LEN - bytesInCurrentFrame;
        readFrom = propertyKeyDataTypeCache;
    }
    propertyKeyIdxPtr = reinterpret_cast<const uint32_t*>(readFrom);
    propertyDataType = DataType(*reinterpret_cast<const uint8_t*>(readFrom + PROPERTY_IDX_LEN));
    listLen -= PROPERTY_IDX_LEN + PROPERTY_DATATYPE_LEN;
}

void Lists<UNSTRUCTURED>::readOrSkipUnstrPropertyValue(uint64_t& physicalPageIdx,
    DataType& propertyDataType, const unique_ptr<ColumnOrListsHandle>& handle,
    PageCursor& pageCursor, uint32_t listLen, LogicalToPhysicalPageIdxMapper& mapper,
    const shared_ptr<ValueVector>& valueVector, uint64_t pos, bool toRead) {
    auto frame = bufferManager.get(fileHandle, physicalPageIdx);
    auto dataTypeSize = getDataTypeSize(propertyDataType);
    auto values = (Value*)valueVector->values;
    if (pageCursor.offset + dataTypeSize < PAGE_SIZE) {
        if (toRead) {
            memcpy(&values[pos].primitive, frame + pageCursor.offset, dataTypeSize);
        }
        pageCursor.offset += dataTypeSize;

    } else {
        auto bytesInCurrentFrame = PAGE_SIZE - pageCursor.offset;
        if (toRead) {
            memcpy(&values[pos].primitive, frame + pageCursor.offset, bytesInCurrentFrame);
        }
        reclaim(handle);
        physicalPageIdx = mapper.getPageIdx(++pageCursor.idx);
        frame = bufferManager.pin(fileHandle, physicalPageIdx);
        handle->setPageIdx(physicalPageIdx);
        if (toRead) {
            memcpy(&values[pos].primitive + bytesInCurrentFrame, frame,
                dataTypeSize - bytesInCurrentFrame);
        }
        pageCursor.offset = dataTypeSize - bytesInCurrentFrame;
    }
    listLen -= dataTypeSize;
    values[pos].dataType = propertyDataType;
    readStringsFromOverflowPages(valueVector);
}

void Lists<UNSTRUCTURED>::readStringsFromOverflowPages(const shared_ptr<ValueVector>& valueVector) {
    auto values = valueVector->values;
    uint64_t overflowPtr = valueVector->state->size * sizeof(Value);
    size_t sizeNeededForOverflowStrings = valueVector->state->size * sizeof(Value);
    for (auto i = 0u; i < valueVector->state->numSelectedValues; i++) {
        auto pos = valueVector->state->selectedValuesPos[i];
        if (!valueVector->nullMask[pos] && ((Value*)values)[pos].dataType == STRING) {
            if (((Value*)values)[pos].primitive.gf_stringVal.len > 4) {
                sizeNeededForOverflowStrings += ((Value*)values)[pos].primitive.gf_stringVal.len;
            }
        }
    }
    values = valueVector->reserve(sizeNeededForOverflowStrings);
    PageCursor cursor;
    for (auto i = 0u; i < valueVector->state->numSelectedValues; i++) {
        auto pos = valueVector->state->selectedValuesPos[i];
        if (!valueVector->nullMask[pos] && ((Value*)values)[pos].dataType == STRING) {
            if (((Value*)values)[pos].primitive.gf_stringVal.len > 12) {
                ((Value*)values)[pos].primitive.gf_stringVal.getOverflowPtrInfo(
                    cursor.idx, cursor.offset);
                auto frame = bufferManager.pin(overflowPagesFileHandle, cursor.idx);
                memcpy(
                    values + overflowPtr, frame + cursor.offset, ((gf_string_t*)values)[pos].len);
                ((Value*)values)[pos].primitive.gf_stringVal.overflowPtr =
                    reinterpret_cast<uintptr_t>(values + overflowPtr);
                overflowPtr += ((Value*)values)[pos].primitive.gf_stringVal.len;
                bufferManager.unpin(overflowPagesFileHandle, cursor.idx);
            }
        }
    }
}

} // namespace storage
} // namespace graphflow

#include "src/storage/include/data_structure/lists/lists.h"

#include "src/common/include/value.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

uint64_t BaseLists::getNumElementsInList(node_offset_t nodeOffset) {
    auto header = headers->getHeader(nodeOffset);
    auto numElements =
        ListHeaders::isALargeList(header) ?
            metadata.getNumElementsInLargeLists(ListHeaders::getLargeListIdx(header)) :
            ListHeaders::getSmallListLen(header);
    return numElements;
}

void BaseLists::readValues(node_offset_t nodeOffset, const shared_ptr<ValueVector>& valueVector,
    uint64_t& listLen, const unique_ptr<DataStructureHandle>& handle, uint32_t maxElementsToRead,
    BufferManagerMetrics& metrics) {
    auto header = headers->getHeader(nodeOffset);
    if (handle->hasMoreToRead() || ListHeaders::isALargeList(header)) {
        readFromLargeList(valueVector, listLen, handle, header, maxElementsToRead, metrics);
    } else {
        readSmallList(nodeOffset, valueVector, listLen, handle, header, metrics);
    }
}

void BaseLists::readSmallList(node_offset_t nodeOffset, const shared_ptr<ValueVector>& valueVector,
    uint64_t& listLen, const unique_ptr<DataStructureHandle>& handle, uint32_t header,
    BufferManagerMetrics& metrics) {
    if (handle->getIsAdjListsHandle()) {
        listLen = ListHeaders::getSmallListLen(header);
    }
    auto pageCursor = getPageCursorForOffset(ListHeaders::getSmallListCSROffset(header));
    auto sizeLeftToCopy = listLen * elementSize;
    uint32_t chunkIdx = nodeOffset / LISTS_CHUNK_SIZE;
    if (pageCursor.offset + sizeLeftToCopy > PAGE_SIZE) {
        readBySequentialCopy(valueVector, handle, sizeLeftToCopy, pageCursor,
            metadata.getPageMapperForChunkIdx(chunkIdx), metrics);
    } else {
        // map logical pageIdx to physical pageIdx
        pageCursor.idx = metadata.getPageIdxFromAChunkPageList(chunkIdx, pageCursor.idx);
        readBySettingFrame(valueVector, handle, pageCursor, metrics);
    }
}

void BaseLists::readFromLargeList(const shared_ptr<ValueVector>& valueVector, uint64_t& listLen,
    const unique_ptr<DataStructureHandle>& handle, uint32_t header, uint32_t maxElementsToRead,
    BufferManagerMetrics& metrics) {
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
            metadata.getPageMapperForLargeListIdx(largeListIdx), metrics);
    } else {
        // map logical pageIdx to physical pageIdx
        pageCursor.idx = metadata.getPageIdxFromALargeListPageList(largeListIdx, pageCursor.idx);
        readBySettingFrame(valueVector, handle, pageCursor, metrics);
    }
}

void Lists<UNSTRUCTURED>::readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
    uint32_t propertyKeyIdxToRead, const shared_ptr<ValueVector>& valueVector,
    const unique_ptr<DataStructureHandle>& handle, BufferManagerMetrics& metrics) {
    valueVector->reset();
    node_offset_t nodeOffset;
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->getCurrSelectedValuesPos();
        nodeOffset = nodeIDVector->readNodeOffset(pos);
        readUnstrPropertyListOfNode(nodeOffset, propertyKeyIdxToRead, valueVector, pos, handle,
            headers->getHeader(nodeOffset), metrics);
    } else {
        for (auto i = 0ul; i < valueVector->state->size; i++) {
            auto pos = valueVector->state->selectedValuesPos[i];
            nodeOffset = nodeIDVector->readNodeOffset(pos);
            readUnstrPropertyListOfNode(nodeOffset, propertyKeyIdxToRead, valueVector, pos, handle,
                headers->getHeader(nodeOffset), metrics);
        }
    }
    reclaim(handle);
}

void Lists<UNSTRUCTURED>::readUnstrPropertyListOfNode(node_offset_t nodeOffset,
    uint32_t propertyKeyIdxToRead, const shared_ptr<ValueVector>& valueVector, uint64_t pos,
    const unique_ptr<DataStructureHandle>& handle, uint32_t header, BufferManagerMetrics& metrics) {
    PageCursor pageCursor;
    uint64_t listLen;
    unique_ptr<LogicalToPhysicalPageIdxMapper> mapper;
    if (ListHeaders::isALargeList(header)) {
        auto largeListIdx = ListHeaders::getLargeListIdx(header);
        listLen = metadata.getNumElementsInLargeLists(largeListIdx);
        pageCursor = getPageCursorForOffset(0);
        mapper = metadata.getPageMapperForLargeListIdx(largeListIdx);
    } else {
        pageCursor = getPageCursorForOffset(ListHeaders::getSmallListCSROffset(header));
        listLen = ListHeaders::getSmallListLen(header);
        uint32_t chunkIdx = nodeOffset / LISTS_CHUNK_SIZE;
        mapper = metadata.getPageMapperForChunkIdx(chunkIdx);
    }
    auto propertyKeyDataTypeCache = make_unique<uint8_t[]>(UNSTR_PROP_HEADER_LEN);
    while (listLen) {
        auto physicalPageIdx = mapper->getPageIdx(pageCursor.idx);
        if (handle->getPageIdx() != physicalPageIdx) {
            reclaim(handle);
            bufferManager.pin(fileHandle, physicalPageIdx, metrics);
            handle->setPageIdx(physicalPageIdx);
        }
        const uint32_t* propertyKeyIdxPtr;
        DataType propertyDataType;
        readUnstrPropertyKeyIdxAndDatatype(propertyKeyDataTypeCache.get(), physicalPageIdx,
            propertyKeyIdxPtr, propertyDataType, handle, pageCursor, listLen, *mapper, metrics);
        if (propertyKeyIdxToRead == *propertyKeyIdxPtr) {
            readOrSkipUnstrPropertyValue(physicalPageIdx, propertyDataType, handle, pageCursor,
                listLen, *mapper, valueVector, pos, true /*to read*/, metrics);
            valueVector->nullMask[pos] = false;
            // found the property, exiting.
            return;
        }
        // property not found, skipping the current property value.
        readOrSkipUnstrPropertyValue(physicalPageIdx, propertyDataType, handle, pageCursor, listLen,
            *mapper, valueVector, pos, false /*to read*/, metrics);
    }
    valueVector->nullMask[pos] = true;
}

void Lists<UNSTRUCTURED>::readUnstrPropertyKeyIdxAndDatatype(uint8_t* propertyKeyDataTypeCache,
    uint64_t& physicalPageIdx, const uint32_t*& propertyKeyIdxPtr, DataType& propertyDataType,
    const unique_ptr<DataStructureHandle>& handle, PageCursor& pageCursor, uint64_t& listLen,
    LogicalToPhysicalPageIdxMapper& mapper, BufferManagerMetrics& metrics) {
    auto frame = bufferManager.get(fileHandle, physicalPageIdx, metrics);
    const uint8_t* readFrom;
    if ((uint64_t)(pageCursor.offset + UNSTR_PROP_HEADER_LEN) < PAGE_SIZE) {
        readFrom = frame + pageCursor.offset;
        pageCursor.offset += UNSTR_PROP_HEADER_LEN;
    } else {
        auto bytesInCurrentFrame = PAGE_SIZE - pageCursor.offset;
        memcpy(propertyKeyDataTypeCache, frame + pageCursor.offset, bytesInCurrentFrame);
        reclaim(handle);
        physicalPageIdx = mapper.getPageIdx(++pageCursor.idx);
        frame = bufferManager.pin(fileHandle, physicalPageIdx, metrics);
        handle->setPageIdx(physicalPageIdx);
        memcpy(propertyKeyDataTypeCache + bytesInCurrentFrame, frame,
            UNSTR_PROP_HEADER_LEN - bytesInCurrentFrame);
        pageCursor.offset = UNSTR_PROP_HEADER_LEN - bytesInCurrentFrame;
        readFrom = propertyKeyDataTypeCache;
    }
    propertyKeyIdxPtr = reinterpret_cast<const uint32_t*>(readFrom);
    propertyDataType = DataType(*reinterpret_cast<const uint8_t*>(readFrom + UNSTR_PROP_IDX_LEN));
    listLen -= UNSTR_PROP_HEADER_LEN;
}

void Lists<UNSTRUCTURED>::readOrSkipUnstrPropertyValue(uint64_t& physicalPageIdx,
    DataType& propertyDataType, const unique_ptr<DataStructureHandle>& handle,
    PageCursor& pageCursor, uint64_t& listLen, LogicalToPhysicalPageIdxMapper& mapper,
    const shared_ptr<ValueVector>& valueVector, uint64_t pos, bool toRead,
    BufferManagerMetrics& metrics) {
    auto frame = bufferManager.get(fileHandle, physicalPageIdx, metrics);
    auto dataTypeSize = TypeUtils::getDataTypeSize(propertyDataType);
    auto& value = ((Value*)valueVector->values)[pos];
    if (pageCursor.offset + dataTypeSize < PAGE_SIZE) {
        if (toRead) {
            memcpy(&value.val, frame + pageCursor.offset, dataTypeSize);
        }
        pageCursor.offset += dataTypeSize;
    } else {
        auto bytesInCurrentFrame = PAGE_SIZE - pageCursor.offset;
        if (toRead) {
            memcpy(&value.val, frame + pageCursor.offset, bytesInCurrentFrame);
        }
        reclaim(handle);
        physicalPageIdx = mapper.getPageIdx(++pageCursor.idx);
        frame = bufferManager.pin(fileHandle, physicalPageIdx, metrics);
        handle->setPageIdx(physicalPageIdx);
        if (toRead) {
            memcpy(&value.val + bytesInCurrentFrame, frame, dataTypeSize - bytesInCurrentFrame);
        }
        pageCursor.offset = dataTypeSize - bytesInCurrentFrame;
    }
    listLen -= dataTypeSize;
    value.dataType = propertyDataType;
    if (toRead && STRING == propertyDataType) {
        stringOverflowPages.readAStringFromOverflowPages(*valueVector, pos, metrics);
    }
}

} // namespace storage
} // namespace graphflow

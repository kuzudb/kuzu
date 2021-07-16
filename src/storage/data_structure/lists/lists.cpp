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
    uint64_t& listLen, const unique_ptr<ListsPageHandle>& listsPageHandle,
    uint32_t maxElementsToRead, BufferManagerMetrics& metrics) {
    auto header = headers->getHeader(nodeOffset);
    if (listsPageHandle->hasMoreToRead() || ListHeaders::isALargeList(header)) {
        readFromLargeList(
            valueVector, listLen, listsPageHandle, header, maxElementsToRead, metrics);
    } else {
        readSmallList(nodeOffset, valueVector, listLen, listsPageHandle, header, metrics);
    }
}

void BaseLists::readFromLargeList(const shared_ptr<ValueVector>& valueVector, uint64_t& listLen,
    const unique_ptr<ListsPageHandle>& listsPageHandle, uint32_t header, uint32_t maxElementsToRead,
    BufferManagerMetrics& metrics) {
    auto largeListIdx = ListHeaders::getLargeListIdx(header);
    auto pageCursor = getPageCursorForOffset(listsPageHandle->getListSyncState()->getStartIdx());
    auto sizeLeftToCopy = elementSize * listLen;
    if (pageCursor.offset + sizeLeftToCopy > PAGE_SIZE) {
        readBySequentialCopy(valueVector, *listsPageHandle, sizeLeftToCopy, pageCursor,
            metadata.getPageMapperForLargeListIdx(largeListIdx), metrics);
    } else {
        // map logical pageIdx to physical pageIdx
        pageCursor.idx = metadata.getPageMapperForLargeListIdx(largeListIdx)(pageCursor.idx);
        readBySettingFrame(valueVector, *listsPageHandle, pageCursor, metrics);
    }
}

void BaseLists::readSmallList(node_offset_t nodeOffset, const shared_ptr<ValueVector>& valueVector,
    uint64_t& listLen, const unique_ptr<ListsPageHandle>& listsPageHandle, uint32_t header,
    BufferManagerMetrics& metrics) {
    auto pageCursor = getPageCursorForOffset(ListHeaders::getSmallListCSROffset(header));
    auto sizeLeftToCopy = listLen * elementSize;
    auto chunkIdx = nodeOffset >> BaseLists::LISTS_CHUNK_SIZE_LOG_2;
    if (pageCursor.offset + sizeLeftToCopy > PAGE_SIZE) {
        readBySequentialCopy(valueVector, *listsPageHandle, sizeLeftToCopy, pageCursor,
            metadata.getPageMapperForChunkIdx(chunkIdx), metrics);
    } else {
        // map logical pageIdx to physical pageIdx
        pageCursor.idx = metadata.getPageMapperForChunkIdx(chunkIdx)(pageCursor.idx);
        readBySettingFrame(valueVector, *listsPageHandle, pageCursor, metrics);
    }
}

// For the case of reading a list of strings, we always read by sequential copy.
void Lists<STRING>::readFromLargeList(const shared_ptr<ValueVector>& valueVector, uint64_t& listLen,
    const unique_ptr<ListsPageHandle>& listsPageHandle, uint32_t header, uint32_t maxElementsToRead,
    BufferManagerMetrics& metrics) {
    auto largeListIdx = ListHeaders::getLargeListIdx(header);
    auto pageCursor = getPageCursorForOffset(listsPageHandle->getListSyncState()->getStartIdx());
    auto sizeLeftToCopy = elementSize * listLen;
    readBySequentialCopy(valueVector, *listsPageHandle, sizeLeftToCopy, pageCursor,
        metadata.getPageMapperForLargeListIdx(largeListIdx), metrics);
    stringOverflowPages.readStringsFromOverflowPages(*valueVector, metrics);
}

// For the case of reading a list of strings, we always read by sequential copy.
void Lists<STRING>::readSmallList(node_offset_t nodeOffset,
    const shared_ptr<ValueVector>& valueVector, uint64_t& listLen,
    const unique_ptr<ListsPageHandle>& listsPageHandle, uint32_t header,
    BufferManagerMetrics& metrics) {
    auto pageCursor = getPageCursorForOffset(ListHeaders::getSmallListCSROffset(header));
    auto sizeLeftToCopy = listLen * elementSize;
    auto chunkIdx = nodeOffset >> BaseLists::LISTS_CHUNK_SIZE_LOG_2;
    readBySequentialCopy(valueVector, *listsPageHandle, sizeLeftToCopy, pageCursor,
        metadata.getPageMapperForChunkIdx(chunkIdx), metrics);
    stringOverflowPages.readStringsFromOverflowPages(*valueVector, metrics);
}

// In case of adjLists, length of the list to be read is limited to the maximum number of
// elements that can be read by setting the frame and not doing any copy.
void Lists<NODE>::readFromLargeList(const shared_ptr<ValueVector>& valueVector, uint64_t& listLen,
    const unique_ptr<ListsPageHandle>& listsPageHandle, uint32_t header, uint32_t maxElementsToRead,
    BufferManagerMetrics& metrics) {
    auto largeListIdx = ListHeaders::getLargeListIdx(header);
    auto listSyncState = listsPageHandle->getListSyncState();
    auto numElements = metadata.getNumElementsInLargeLists(largeListIdx);
    uint64_t csrOffset;
    if (!listsPageHandle->hasMoreToRead()) {
        // initialize listSyncState to start tracking a new list.
        listSyncState->init(numElements);
        csrOffset = 0;
    } else {
        csrOffset = listSyncState->getEndIdx();
    }
    auto pageCursor = getPageCursorForOffset(csrOffset);
    listLen = min((uint32_t)(numElements - csrOffset),
        min(numElementsPerPage - (uint32_t)(csrOffset % numElementsPerPage), maxElementsToRead));
    listSyncState->set(csrOffset, listLen);
    // map logical pageIdx to physical pageIdx
    pageCursor.idx = metadata.getPageMapperForLargeListIdx(largeListIdx)(pageCursor.idx);
    readBySettingFrame(valueVector, *listsPageHandle, pageCursor, metrics);
}

// The case of reading a small adjList is similar to that of reading any list except that we set
// also the listLen variable.
void Lists<NODE>::readSmallList(node_offset_t nodeOffset,
    const shared_ptr<ValueVector>& valueVector, uint64_t& listLen,
    const unique_ptr<ListsPageHandle>& listsPageHandle, uint32_t header,
    BufferManagerMetrics& metrics) {
    listLen = ListHeaders::getSmallListLen(header);
    BaseLists::readSmallList(nodeOffset, valueVector, listLen, listsPageHandle, header, metrics);
}

void Lists<UNSTRUCTURED>::readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
    uint32_t propertyKeyIdxToRead, const shared_ptr<ValueVector>& valueVector,
    const unique_ptr<PageHandle>& pageHandle, BufferManagerMetrics& metrics) {
    valueVector->reset();
    node_offset_t nodeOffset;
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->getCurrSelectedValuesPos();
        nodeOffset = nodeIDVector->readNodeOffset(pos);
        readUnstrPropertyListOfNode(nodeOffset, propertyKeyIdxToRead, valueVector, pos, pageHandle,
            headers->getHeader(nodeOffset), metrics);
    } else {
        for (auto i = 0ul; i < valueVector->state->size; i++) {
            auto pos = valueVector->state->selectedValuesPos[i];
            nodeOffset = nodeIDVector->readNodeOffset(pos);
            readUnstrPropertyListOfNode(nodeOffset, propertyKeyIdxToRead, valueVector, pos,
                pageHandle, headers->getHeader(nodeOffset), metrics);
        }
    }
    reclaim(*pageHandle);
}

void Lists<UNSTRUCTURED>::readUnstrPropertyListOfNode(node_offset_t nodeOffset,
    uint32_t propertyKeyIdxToRead, const shared_ptr<ValueVector>& valueVector, uint64_t pos,
    const unique_ptr<PageHandle>& pageHandle, uint32_t header, BufferManagerMetrics& metrics) {
    PageCursor pageCursor;
    uint64_t listLen;
    function<uint32_t(uint32_t)> logicalToPhysicalPageMapper;
    if (ListHeaders::isALargeList(header)) {
        auto largeListIdx = ListHeaders::getLargeListIdx(header);
        listLen = metadata.getNumElementsInLargeLists(largeListIdx);
        pageCursor = getPageCursorForOffset(0);
        logicalToPhysicalPageMapper = metadata.getPageMapperForLargeListIdx(largeListIdx);
    } else {
        pageCursor = getPageCursorForOffset(ListHeaders::getSmallListCSROffset(header));
        listLen = ListHeaders::getSmallListLen(header);
        uint32_t chunkIdx = nodeOffset >> BaseLists::LISTS_CHUNK_SIZE_LOG_2;
        logicalToPhysicalPageMapper = metadata.getPageMapperForChunkIdx(chunkIdx);
    }
    // propertyKeyDataTypeCache holds the unstructured property's KeyIdx and dataType for the case
    // when the property header is splitted across 2 pages. When such a case exists,
    // readUnstrPropertyKeyIdxAndDatatype() copies the property header to the cache and
    // propertyKeyIdxPtr is made to point to the cache instead of pointing to the read buffer frame.
    uint8_t propertyKeyDataTypeCache[UNSTR_PROP_HEADER_LEN];
    while (listLen) {
        uint64_t physicalPageIdx = logicalToPhysicalPageMapper(pageCursor.idx);
        if (pageHandle->getPageIdx() != physicalPageIdx) {
            reclaim(*pageHandle);
            bufferManager.pin(fileHandle, physicalPageIdx, metrics);
            pageHandle->setPageIdx(physicalPageIdx);
        }
        const uint32_t* propertyKeyIdxPtr;
        DataType propertyDataType;
        readUnstrPropertyKeyIdxAndDatatype(propertyKeyDataTypeCache, physicalPageIdx,
            propertyKeyIdxPtr, propertyDataType, pageHandle, pageCursor, listLen,
            logicalToPhysicalPageMapper, metrics);
        if (propertyKeyIdxToRead == *propertyKeyIdxPtr) {
            readOrSkipUnstrPropertyValue(physicalPageIdx, propertyDataType, pageHandle, pageCursor,
                listLen, logicalToPhysicalPageMapper, valueVector, pos, true /*to read*/, metrics);
            valueVector->nullMask[pos] = false;
            // found the property, exiting.
            return;
        }
        // property not found, skipping the current property value.
        readOrSkipUnstrPropertyValue(physicalPageIdx, propertyDataType, pageHandle, pageCursor,
            listLen, logicalToPhysicalPageMapper, valueVector, pos, false /*to read*/, metrics);
    }
    valueVector->nullMask[pos] = true;
}

void Lists<UNSTRUCTURED>::readUnstrPropertyKeyIdxAndDatatype(uint8_t* propertyKeyDataTypeCache,
    uint64_t& physicalPageIdx, const uint32_t*& propertyKeyIdxPtr, DataType& propertyDataType,
    const unique_ptr<PageHandle>& pageHandle, PageCursor& pageCursor, uint64_t& listLen,
    const function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper,
    BufferManagerMetrics& metrics) {
    auto frame = bufferManager.get(fileHandle, physicalPageIdx, metrics);
    const uint8_t* readFrom;
    if ((uint64_t)(pageCursor.offset + UNSTR_PROP_HEADER_LEN) <= PAGE_SIZE) {
        readFrom = frame + pageCursor.offset;
        pageCursor.offset += UNSTR_PROP_HEADER_LEN;
    } else {
        auto bytesInCurrentFrame = PAGE_SIZE - pageCursor.offset;
        memcpy(propertyKeyDataTypeCache, frame + pageCursor.offset, bytesInCurrentFrame);
        reclaim(*pageHandle);
        physicalPageIdx = logicalToPhysicalPageMapper(++pageCursor.idx);
        frame = bufferManager.pin(fileHandle, physicalPageIdx, metrics);
        pageHandle->setPageIdx(physicalPageIdx);
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
    DataType& propertyDataType, const unique_ptr<PageHandle>& pageHandle, PageCursor& pageCursor,
    uint64_t& listLen, const function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper,
    const shared_ptr<ValueVector>& valueVector, uint64_t pos, bool toRead,
    BufferManagerMetrics& metrics) {
    auto frame = bufferManager.get(fileHandle, physicalPageIdx, metrics);
    auto dataTypeSize = TypeUtils::getDataTypeSize(propertyDataType);
    auto& value = ((Value*)valueVector->values)[pos];
    if (pageCursor.offset + dataTypeSize <= PAGE_SIZE) {
        if (toRead) {
            memcpy(&value.val, frame + pageCursor.offset, dataTypeSize);
        }
        pageCursor.offset += dataTypeSize;
    } else {
        auto bytesInCurrentFrame = PAGE_SIZE - pageCursor.offset;
        if (toRead) {
            memcpy(&value.val, frame + pageCursor.offset, bytesInCurrentFrame);
        }
        reclaim(*pageHandle);
        physicalPageIdx = logicalToPhysicalPageMapper(++pageCursor.idx);
        frame = bufferManager.pin(fileHandle, physicalPageIdx, metrics);
        pageHandle->setPageIdx(physicalPageIdx);
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

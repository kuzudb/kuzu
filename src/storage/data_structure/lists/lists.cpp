#include "src/storage/include/data_structure/lists/lists.h"

#include "src/common/include/value.h"

using namespace graphflow::common;

namespace graphflow {
namespace storage {

ListInfo BaseLists::getListInfo(node_offset_t nodeOffset) {
    ListInfo info;
    auto header = headers->getHeader(nodeOffset);
    if (ListHeaders::isALargeList(header)) {
        info.isLargeList = true;
        auto largeListIdx = ListHeaders::getLargeListIdx(header);
        info.cursor = getPageCursorForOffset(0);
        info.mapper = metadata.getPageMapperForLargeListIdx(largeListIdx);
        info.listLen = metadata.getNumElementsInLargeLists(largeListIdx);
    } else {
        info.isLargeList = false;
        auto chunkIdx = nodeOffset >> BaseLists::LISTS_CHUNK_SIZE_LOG_2;
        info.cursor = getPageCursorForOffset(ListHeaders::getSmallListCSROffset(header));
        info.mapper = metadata.getPageMapperForChunkIdx(chunkIdx);
        info.listLen = ListHeaders::getSmallListLen(header);
    }
    return info;
}

void BaseLists::readValues(node_offset_t nodeOffset, const shared_ptr<ValueVector>& valueVector,
    uint64_t& listLen, const unique_ptr<ListsPageHandle>& listsPageHandle,
    uint32_t maxElementsToRead, BufferManagerMetrics& metrics) {
    auto info = getListInfo(nodeOffset);
    if (listsPageHandle->hasMoreToRead() || info.isLargeList) {
        readFromLargeList(valueVector, listLen, listsPageHandle, info, maxElementsToRead, metrics);
    } else {
        readSmallList(nodeOffset, valueVector, listLen, listsPageHandle, info, metrics);
    }
}

void BaseLists::readFromLargeList(const shared_ptr<ValueVector>& valueVector, uint64_t& lenToRead,
    const unique_ptr<ListsPageHandle>& listsPageHandle, ListInfo& info, uint32_t maxElementsToRead,
    BufferManagerMetrics& metrics) {
    // assumes that the associated adjList has already updated the syncState.
    auto pageCursor = getPageCursorForOffset(listsPageHandle->getListSyncState()->getStartIdx());
    auto sizeLeftToCopy = elementSize * lenToRead;
    if (pageCursor.offset + sizeLeftToCopy > PAGE_SIZE) {
        readBySequentialCopy(
            valueVector, *listsPageHandle, sizeLeftToCopy, pageCursor, info.mapper, metrics);
    } else {
        // map logical pageIdx to physical pageIdx
        pageCursor.idx = info.mapper(pageCursor.idx);
        readBySettingFrame(valueVector, *listsPageHandle, pageCursor, metrics);
    }
}

void BaseLists::readSmallList(node_offset_t nodeOffset, const shared_ptr<ValueVector>& valueVector,
    uint64_t& lenToRead, const unique_ptr<ListsPageHandle>& listsPageHandle, ListInfo& info,
    BufferManagerMetrics& metrics) {
    auto sizeLeftToCopy = lenToRead * elementSize;
    if (info.cursor.offset + sizeLeftToCopy > PAGE_SIZE) {
        readBySequentialCopy(
            valueVector, *listsPageHandle, sizeLeftToCopy, info.cursor, info.mapper, metrics);
    } else {
        // map logical pageIdx to physical pageIdx
        info.cursor.idx = info.mapper(info.cursor.idx);
        readBySettingFrame(valueVector, *listsPageHandle, info.cursor, metrics);
    }
}

// For the case of reading a list of strings, we always read by sequential copy.
void Lists<STRING>::readFromLargeList(const shared_ptr<ValueVector>& valueVector,
    uint64_t& lenToRead, const unique_ptr<ListsPageHandle>& listsPageHandle, ListInfo& info,
    uint32_t maxElementsToRead, BufferManagerMetrics& metrics) {
    auto pageCursor = getPageCursorForOffset(listsPageHandle->getListSyncState()->getStartIdx());
    auto sizeLeftToCopy = elementSize * lenToRead;
    readBySequentialCopy(
        valueVector, *listsPageHandle, sizeLeftToCopy, pageCursor, info.mapper, metrics);
    stringOverflowPages.readStringsToVector(*valueVector, metrics);
}

// For the case of reading a list of strings, we always read by sequential copy.
void Lists<STRING>::readSmallList(node_offset_t nodeOffset,
    const shared_ptr<ValueVector>& valueVector, uint64_t& lenToRead,
    const unique_ptr<ListsPageHandle>& listsPageHandle, ListInfo& info,
    BufferManagerMetrics& metrics) {
    auto sizeLeftToCopy = lenToRead * elementSize;
    readBySequentialCopy(
        valueVector, *listsPageHandle, sizeLeftToCopy, info.cursor, info.mapper, metrics);
    stringOverflowPages.readStringsToVector(*valueVector, metrics);
}

// In case of adjLists, length of the list to be read is limited to the maximum number of
// elements that can be read by setting the frame and not doing any copy.
void Lists<NODE>::readFromLargeList(const shared_ptr<ValueVector>& valueVector, uint64_t& lenToRead,
    const unique_ptr<ListsPageHandle>& listsPageHandle, ListInfo& info, uint32_t maxElementsToRead,
    BufferManagerMetrics& metrics) {
    auto listSyncState = listsPageHandle->getListSyncState();
    uint64_t csrOffset;
    if (!listsPageHandle->hasMoreToRead()) {
        // initialize listSyncState to start tracking a new list.
        listSyncState->init(info.listLen);
        csrOffset = 0;
    } else {
        csrOffset = listSyncState->getEndIdx();
        info.cursor = getPageCursorForOffset(csrOffset);
    }
    lenToRead = min((uint32_t)(info.listLen - csrOffset),
        min(numElementsPerPage - (uint32_t)(csrOffset % numElementsPerPage), maxElementsToRead));
    listSyncState->set(csrOffset, lenToRead);
    // map logical pageIdx to physical pageIdx
    info.cursor.idx = info.mapper(info.cursor.idx);
    readBySettingFrame(valueVector, *listsPageHandle, info.cursor, metrics);
}

// The case of reading a small adjList is similar to that of reading any list except that we set
// also the lenToRead variable.
void Lists<NODE>::readSmallList(node_offset_t nodeOffset,
    const shared_ptr<ValueVector>& valueVector, uint64_t& lenToRead,
    const unique_ptr<ListsPageHandle>& listsPageHandle, ListInfo& info,
    BufferManagerMetrics& metrics) {
    lenToRead = info.listLen;
    BaseLists::readSmallList(nodeOffset, valueVector, lenToRead, listsPageHandle, info, metrics);
}

void Lists<UNSTRUCTURED>::readValues(const shared_ptr<NodeIDVector>& nodeIDVector,
    uint32_t propertyKeyIdxToRead, const shared_ptr<ValueVector>& valueVector,
    const unique_ptr<PageHandle>& pageHandle, BufferManagerMetrics& metrics) {
    valueVector->reset();
    node_offset_t nodeOffset;
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->getPositionOfCurrIdx();
        nodeOffset = nodeIDVector->readNodeOffset(pos);
        auto info = getListInfo(nodeOffset);
        readUnstrPropertyFromAList(
            propertyKeyIdxToRead, valueVector, pos, pageHandle, info, metrics);
    } else {
        for (auto i = 0ul; i < valueVector->state->size; i++) {
            auto pos = valueVector->state->getSelectedPositionAtIdx(i);
            nodeOffset = nodeIDVector->readNodeOffset(pos);
            auto info = getListInfo(nodeOffset);
            readUnstrPropertyFromAList(
                propertyKeyIdxToRead, valueVector, pos, pageHandle, info, metrics);
        }
    }
    reclaim(*pageHandle);
}

unique_ptr<map<uint32_t, Literal>> Lists<UNSTRUCTURED>::readList(node_offset_t nodeOffset,
    const unique_ptr<PageHandle>& pageHandle, BufferManagerMetrics& metrics) {
    auto info = getListInfo(nodeOffset);
    auto retVal = make_unique<map<uint32_t /*unstructuredProperty idx*/, Literal>>();
    uint8_t propertyKeyDataTypeCache[UNSTR_PROP_HEADER_LEN];
    while (info.listLen) {
        uint64_t physicalPageIdx = info.mapper(info.cursor.idx);
        if (pageHandle->getPageIdx() != physicalPageIdx) {
            reclaim(*pageHandle);
            bufferManager.pin(fileHandle, physicalPageIdx, metrics);
            pageHandle->setPageIdx(physicalPageIdx);
        }
        const uint32_t* propertyKeyIdxPtr;
        DataType propertyDataType;
        readUnstrPropertyKeyIdxAndDatatype(propertyKeyDataTypeCache, physicalPageIdx,
            propertyKeyIdxPtr, propertyDataType, pageHandle, info.cursor, info.listLen, info.mapper,
            metrics);
        Value unstrPropertyValue{propertyDataType};
        readOrSkipUnstrPropertyValue(physicalPageIdx, propertyDataType, pageHandle, info.cursor,
            info.listLen, info.mapper, &unstrPropertyValue, true /*to read*/, metrics);
        Literal propertyValueAsLiteral;
        propertyValueAsLiteral.dataType = unstrPropertyValue.dataType;
        if (STRING == propertyDataType) {
            propertyValueAsLiteral.strVal =
                stringOverflowPages.readString(unstrPropertyValue.val.strVal, metrics);
        } else {
            memcpy(&propertyValueAsLiteral.val, &unstrPropertyValue.val,
                TypeUtils::getDataTypeSize(propertyDataType));
        }
        retVal->insert(pair<uint32_t, Literal>(*propertyKeyIdxPtr, propertyValueAsLiteral));
    }
    return retVal;
}

void Lists<UNSTRUCTURED>::readUnstrPropertyFromAList(uint32_t propertyKeyIdxToRead,
    const shared_ptr<ValueVector>& valueVector, uint64_t pos,
    const unique_ptr<PageHandle>& pageHandle, ListInfo& info, BufferManagerMetrics& metrics) {
    // propertyKeyDataTypeCache holds the unstructured property's KeyIdx and dataType for the case
    // when the property header is split across 2 pages. When such a case exists,
    // readUnstrPropertyKeyIdxAndDatatype() copies the property header to the cache and
    // propertyKeyIdxPtr is made to point to the cache instead of pointing to the read buffer frame.
    uint8_t propertyKeyDataTypeCache[UNSTR_PROP_HEADER_LEN];
    while (info.listLen) {
        uint64_t physicalPageIdx = info.mapper(info.cursor.idx);
        if (pageHandle->getPageIdx() != physicalPageIdx) {
            reclaim(*pageHandle);
            bufferManager.pin(fileHandle, physicalPageIdx, metrics);
            pageHandle->setPageIdx(physicalPageIdx);
        }
        const uint32_t* propertyKeyIdxPtr;
        DataType propertyDataType;
        readUnstrPropertyKeyIdxAndDatatype(propertyKeyDataTypeCache, physicalPageIdx,
            propertyKeyIdxPtr, propertyDataType, pageHandle, info.cursor, info.listLen, info.mapper,
            metrics);
        Value* value = &((Value*)valueVector->values)[pos];
        if (propertyKeyIdxToRead == *propertyKeyIdxPtr) {
            readOrSkipUnstrPropertyValue(physicalPageIdx, propertyDataType, pageHandle, info.cursor,
                info.listLen, info.mapper, value, true /*to read*/, metrics);
            valueVector->nullMask[pos] = false;
            value->dataType = propertyDataType;
            if (STRING == propertyDataType) {
                stringOverflowPages.readStringToVector(*valueVector, pos, metrics);
            }
            // found the property, exiting.
            return;
        }
        // property not found, skipping the current property value.
        readOrSkipUnstrPropertyValue(physicalPageIdx, propertyDataType, pageHandle, info.cursor,
            info.listLen, info.mapper, value, false /*to read*/, metrics);
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
    Value* value, bool toRead, BufferManagerMetrics& metrics) {
    auto frame = bufferManager.get(fileHandle, physicalPageIdx, metrics);
    auto dataTypeSize = TypeUtils::getDataTypeSize(propertyDataType);
    if (pageCursor.offset + dataTypeSize <= PAGE_SIZE) {
        if (toRead) {
            memcpy((uint8_t*)&value->val, frame + pageCursor.offset, dataTypeSize);
        }
        pageCursor.offset += dataTypeSize;
    } else {
        auto bytesInCurrentFrame = PAGE_SIZE - pageCursor.offset;
        if (toRead) {
            memcpy((uint8_t*)&value->val, frame + pageCursor.offset, bytesInCurrentFrame);
        }
        reclaim(*pageHandle);
        physicalPageIdx = logicalToPhysicalPageMapper(++pageCursor.idx);
        frame = bufferManager.pin(fileHandle, physicalPageIdx, metrics);
        pageHandle->setPageIdx(physicalPageIdx);
        if (toRead) {
            memcpy(((uint8_t*)&value->val) + bytesInCurrentFrame, frame,
                dataTypeSize - bytesInCurrentFrame);
        }
        pageCursor.offset = dataTypeSize - bytesInCurrentFrame;
    }
    listLen -= dataTypeSize;
}

} // namespace storage
} // namespace graphflow

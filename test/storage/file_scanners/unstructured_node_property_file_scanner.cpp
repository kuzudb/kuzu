#include "test/storage/include/file_scanners/unstructured_node_property_file_scanner.h"

namespace graphflow {
namespace storage {

void UnstructuredNodePropertyFileScanner::readUnstrPropertyKeyIdxAndDatatype(
    uint64_t& physicalPageIdx, uint8_t* propertyKeyDataTypeCache,
    const uint32_t*& propertyKeyIdxPtr, DataType& propertyDataType, PageCursor& pageCursor,
    uint64_t& listLen, const function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper) {
    const uint8_t* readFrom;
    if ((uint64_t)(pageCursor.offset + Lists<UNSTRUCTURED>::UNSTR_PROP_HEADER_LEN) <= PAGE_SIZE) {
        readFrom = buffer.get() + (physicalPageIdx << PAGE_SIZE_LOG_2) + pageCursor.offset;
        pageCursor.offset += Lists<UNSTRUCTURED>::UNSTR_PROP_HEADER_LEN;
    } else {
        auto bytesInCurrentFrame = PAGE_SIZE - pageCursor.offset;
        memcpy(propertyKeyDataTypeCache,
            buffer.get() + (physicalPageIdx << PAGE_SIZE_LOG_2) + pageCursor.offset,
            bytesInCurrentFrame);
        physicalPageIdx = logicalToPhysicalPageMapper(++pageCursor.idx);
        memcpy(propertyKeyDataTypeCache + bytesInCurrentFrame,
            buffer.get() + (physicalPageIdx << PAGE_SIZE_LOG_2),
            Lists<UNSTRUCTURED>::UNSTR_PROP_HEADER_LEN - bytesInCurrentFrame);
        pageCursor.offset = Lists<UNSTRUCTURED>::UNSTR_PROP_HEADER_LEN - bytesInCurrentFrame;
        readFrom = propertyKeyDataTypeCache;
    }
    propertyKeyIdxPtr = reinterpret_cast<const uint32_t*>(readFrom);
    propertyDataType = DataType(
        *reinterpret_cast<const uint8_t*>(readFrom + Lists<UNSTRUCTURED>::UNSTR_PROP_IDX_LEN));
    listLen -= Lists<UNSTRUCTURED>::UNSTR_PROP_HEADER_LEN;
}

Literal UnstructuredNodePropertyFileScanner::readUnstrPropertyValue(uint64_t& physicalPageIdx,
    DataType& propertyDataType, PageCursor& pageCursor, uint64_t& listLen,
    const function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper) {
    auto dataTypeSize = TypeUtils::getDataTypeSize(propertyDataType);
    uint8_t tmpVal[sizeof(gf_string_t) > sizeof(Literal::Val) ? sizeof(gf_string_t) :
                                                                sizeof(Literal::Val)];
    if (pageCursor.offset + dataTypeSize <= PAGE_SIZE) {
        memcpy(tmpVal, buffer.get() + (physicalPageIdx << PAGE_SIZE_LOG_2) + pageCursor.offset,
            dataTypeSize);
        pageCursor.offset += dataTypeSize;
    } else {
        auto bytesInCurrentFrame = PAGE_SIZE - pageCursor.offset;
        memcpy(tmpVal, buffer.get() + (physicalPageIdx << PAGE_SIZE_LOG_2) + pageCursor.offset,
            bytesInCurrentFrame);
        physicalPageIdx = logicalToPhysicalPageMapper(++pageCursor.idx);
        memcpy(tmpVal + bytesInCurrentFrame, buffer.get() + (physicalPageIdx << PAGE_SIZE_LOG_2),
            dataTypeSize - bytesInCurrentFrame);
        pageCursor.offset = dataTypeSize - bytesInCurrentFrame;
    }
    listLen -= dataTypeSize;
    Literal literal;
    literal.dataType = propertyDataType;
    if (STRING == propertyDataType) {
        gf_string_t mainString;
        memcpy(&mainString, tmpVal, sizeof(gf_string_t));
        string str =
            StringOverflowFileFileScanner::readString(mainString, stringOverflowFileScanner);
        literal.strVal = str;
    } else {
        memcpy(&literal.val, tmpVal, dataTypeSize);
    }
    return literal;
}

map<string, Literal> UnstructuredNodePropertyFileScanner::readUnstructuredProperties(
    int nodeOffset) {
    auto header = listHeaders.getHeader(nodeOffset);
    PageCursor pageCursor;
    uint64_t listLen;
    function<uint32_t(uint32_t)> logicalToPhysicalPageMapper;
    map<string, Literal> retVal;
    if (ListHeaders::isALargeList(header)) {
        auto largeListIdx = ListHeaders::getLargeListIdx(header);
        listLen = listsMetadata.getNumElementsInLargeLists(largeListIdx);
        pageCursor = getPageCursorForOffset(0);
        logicalToPhysicalPageMapper = listsMetadata.getPageMapperForLargeListIdx(largeListIdx);
    } else {
        pageCursor = getPageCursorForOffset(ListHeaders::getSmallListCSROffset(header));
        listLen = ListHeaders::getSmallListLen(header);
        uint32_t chunkIdx = nodeOffset >> BaseLists::LISTS_CHUNK_SIZE_LOG_2;
        logicalToPhysicalPageMapper = listsMetadata.getPageMapperForChunkIdx(chunkIdx);
    }
    // propertyKeyDataTypeCache holds the unstructured property's KeyIdx and dataType for the case
    // when the property header is split across 2 pages. When such a case exists,
    // readUnstrPropertyKeyIdxAndDatatype() copies the property header to the cache and
    // propertyKeyIdxPtr is made to point to the cache instead of pointing to a location in the
    // buffer.
    uint8_t propertyKeyDataTypeCache[Lists<UNSTRUCTURED>::UNSTR_PROP_HEADER_LEN];
    while (listLen) {
        const uint32_t* propertyKeyIdxPtr;
        DataType propertyDataType;
        uint64_t physicalPageIdx = logicalToPhysicalPageMapper(pageCursor.idx);
        readUnstrPropertyKeyIdxAndDatatype(physicalPageIdx, propertyKeyDataTypeCache,
            propertyKeyIdxPtr, propertyDataType, pageCursor, listLen, logicalToPhysicalPageMapper);
        Literal literal = readUnstrPropertyValue(
            physicalPageIdx, propertyDataType, pageCursor, listLen, logicalToPhysicalPageMapper);
        retVal.insert(pair<string, Literal>(
            catalog.getNodePropertyAsString(nodeLabel, *propertyKeyIdxPtr), literal));
    }
    return retVal;
}

} // namespace storage
} // namespace graphflow

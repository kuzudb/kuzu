#include "src/loader/in_mem_storage_structure/include/in_mem_lists.h"

namespace graphflow {
namespace loader {

PageCursor InMemListsUtils::calcPageElementCursor(uint32_t header, uint64_t reversePos,
    uint8_t numBytesPerElement, node_offset_t nodeOffset, ListsMetadataBuilder& metadataBuilder,
    bool hasNULLBytes) {
    PageCursor cursor;
    auto numElementsInAPage = PageUtils::getNumElementsInAPage(numBytesPerElement, hasNULLBytes);
    if (ListHeaders::isALargeList(header)) {
        auto lAdjListIdx = ListHeaders::getLargeListIdx(header);
        auto pos = metadataBuilder.getNumElementsInLargeLists(lAdjListIdx) - reversePos;
        cursor = PageUtils::getPageElementCursorForPos(pos, numElementsInAPage);
        cursor.pageIdx = metadataBuilder.getPageMapperForLargeListIdx(lAdjListIdx)(cursor.pageIdx);
    } else {
        auto chunkId = nodeOffset >> StorageConfig::LISTS_CHUNK_SIZE_LOG_2;
        auto csrOffset = ListHeaders::getSmallListCSROffset(header);
        auto listLen = ListHeaders::getSmallListLen(header);
        auto pos = listLen - reversePos;
        cursor = PageUtils::getPageElementCursorForPos(csrOffset + pos, numElementsInAPage);
        cursor.pageIdx = metadataBuilder.getPageMapperForChunkIdx(chunkId)(
            (csrOffset + pos) / numElementsInAPage);
    }
    return cursor;
}

InMemLists::InMemLists(string fName, DataType dataType, uint64_t numBytesForElement)
    : fName{move(fName)}, dataType{move(dataType)}, numBytesForElement{numBytesForElement} {
    listsMetadataBuilder = make_unique<ListsMetadataBuilder>(this->fName);
    inMemFile = make_unique<InMemFile>(this->fName, numBytesForElement,
        this->dataType.typeID != NODE_ID && this->dataType.typeID != UNSTRUCTURED);
}

void InMemLists::saveToFile() {
    listsMetadataBuilder->saveToDisk();
    inMemFile->flush();
}

void InMemLists::setElement(uint32_t header, node_offset_t nodeOffset, uint64_t pos, uint8_t* val) {
    auto cursor = InMemListsUtils::calcPageElementCursor(header, pos, numBytesForElement,
        nodeOffset, *listsMetadataBuilder, true /* hasNULLBytes */);
    inMemFile->getPage(cursor.pageIdx)
        ->write(cursor.posInPage * numBytesForElement, cursor.posInPage, val, numBytesForElement);
}

void InMemAdjLists::setElement(
    uint32_t header, node_offset_t nodeOffset, uint64_t pos, uint8_t* val) {
    auto cursor = InMemListsUtils::calcPageElementCursor(header, pos, numBytesForElement,
        nodeOffset, *listsMetadataBuilder, false /* hasNULLBytes */);
    auto node = (nodeID_t*)val;
    inMemFile->getPage(cursor.pageIdx)
        ->write(node, cursor.posInPage * compressionScheme.getNumTotalBytes(), cursor.posInPage,
            compressionScheme);
}

void InMemAdjLists::saveToFile() {
    listHeadersBuilder->saveToDisk();
    InMemLists::saveToFile();
}

InMemListsWithOverflow::InMemListsWithOverflow(string fName, DataType dataType)
    : InMemLists{move(fName), move(dataType), Types::getDataTypeSize(dataType)} {
    assert(this->dataType.typeID == STRING || this->dataType.typeID == LIST ||
           this->dataType.typeID == UNSTRUCTURED);
    overflowInMemFile =
        make_unique<InMemOverflowFile>(StorageUtils::getOverflowPagesFName(this->fName));
}

void InMemListsWithOverflow::saveToFile() {
    InMemLists::saveToFile();
    overflowInMemFile->flush();
}

void InMemUnstructuredLists::setUnstructuredElement(PageCursor& cursor, uint32_t propertyKey,
    DataTypeID dataTypeID, const uint8_t* val, PageCursor* overflowCursor) {
    PageCursor localCursor{cursor};
    setComponentOfUnstrProperty(localCursor, 4, reinterpret_cast<uint8_t*>(&propertyKey));
    setComponentOfUnstrProperty(localCursor, 1, reinterpret_cast<uint8_t*>(&dataTypeID));
    switch (dataTypeID) {
    case INT64:
    case DOUBLE:
    case BOOL:
    case DATE:
    case TIMESTAMP:
    case INTERVAL: {
        setComponentOfUnstrProperty(localCursor, Types::getDataTypeSize(dataTypeID), val);
    } break;
    case STRING: {
        auto gfString = overflowInMemFile->copyString((const char*)val, *overflowCursor);
        val = (uint8_t*)(&gfString);
        setComponentOfUnstrProperty(localCursor, Types::getDataTypeSize(dataTypeID), val);
    } break;
    case LIST: {
        auto gfList = overflowInMemFile->copyList(*(Literal*)val, *overflowCursor);
        val = (uint8_t*)(&gfList);
        setComponentOfUnstrProperty(localCursor, Types::getDataTypeSize(dataTypeID), val);
    } break;
    default:
        throw LoaderException("Unsupported data type for unstructured list.");
    }
}

void InMemUnstructuredLists::setComponentOfUnstrProperty(
    PageCursor& localCursor, uint8_t len, const uint8_t* val) {
    if (DEFAULT_PAGE_SIZE - localCursor.posInPage >= len) {
        memcpy(inMemFile->getPage(localCursor.pageIdx)->data + localCursor.posInPage, val, len);
        localCursor.posInPage += len;
    } else {
        auto diff = DEFAULT_PAGE_SIZE - localCursor.posInPage;
        auto writeOffset = inMemFile->getPage(localCursor.pageIdx)->data + localCursor.posInPage;
        memcpy(writeOffset, val, diff);
        auto left = len - diff;
        localCursor.pageIdx++;
        localCursor.posInPage = 0;
        writeOffset = inMemFile->getPage(localCursor.pageIdx)->data + localCursor.posInPage;
        memcpy(writeOffset, val + diff, left);
        localCursor.posInPage = left;
    }
}

void InMemUnstructuredLists::saveToFile() {
    listHeadersBuilder->saveToDisk();
    InMemListsWithOverflow::saveToFile();
}

unique_ptr<InMemLists> InMemListsFactory::getInMemPropertyLists(
    const string& fName, const DataType& dataType, uint64_t numNodes) {
    switch (dataType.typeID) {
    case INT64:
    case DOUBLE:
    case BOOL:
    case DATE:
    case TIMESTAMP:
    case INTERVAL:
        return make_unique<InMemLists>(fName, dataType, Types::getDataTypeSize(dataType));
    case STRING:
        return make_unique<InMemStringLists>(fName);
    case LIST:
        return make_unique<InMemListLists>(fName, dataType);
    case UNSTRUCTURED:
        return make_unique<InMemUnstructuredLists>(fName, numNodes);
    default:
        throw LoaderException("Invalid type for property list creation.");
    }
}

} // namespace loader
} // namespace graphflow

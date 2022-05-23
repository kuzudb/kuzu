#include "src/loader/include/in_mem_structure/in_mem_lists.h"

namespace graphflow {
namespace loader {

PageElementCursor InMemListsUtils::calcPageElementCursor(uint32_t header, uint64_t reversePos,
    uint8_t numBytesPerElement, node_offset_t nodeOffset, ListsMetadata& metadata,
    bool hasNULLBytes) {
    PageElementCursor cursor;
    auto numElementsInAPage =
        hasNULLBytes ? PageUtils::getNumElementsInAPageWithNULLBytes(numBytesPerElement) :
                       PageUtils::getNumElementsInAPageWithoutNULLBytes(numBytesPerElement);
    if (ListHeaders::isALargeList(header)) {
        auto lAdjListIdx = ListHeaders::getLargeListIdx(header);
        auto pos = metadata.getNumElementsInLargeLists(lAdjListIdx) - reversePos;
        cursor = PageUtils::getPageElementCursorForOffset(pos, numElementsInAPage);
        cursor.pageIdx = metadata.getPageMapperForLargeListIdx(lAdjListIdx)(cursor.pageIdx);
    } else {
        auto chunkId = nodeOffset >> StorageConfig::LISTS_CHUNK_SIZE_LOG_2;
        auto csrOffset = ListHeaders::getSmallListCSROffset(header);
        auto listLen = ListHeaders::getSmallListLen(header);
        auto pos = listLen - reversePos;
        cursor = PageUtils::getPageElementCursorForOffset(csrOffset + pos, numElementsInAPage);
        cursor.pageIdx =
            metadata.getPageMapperForChunkIdx(chunkId)((csrOffset + pos) / numElementsInAPage);
    }
    return cursor;
}

InMemLists::InMemLists(string fName, DataType dataType, uint64_t numBytesForElement)
    : fName{move(fName)}, dataType{move(dataType)}, numBytesForElement{numBytesForElement} {
    listsMetadata = make_unique<ListsMetadata>();
}

void InMemLists::init() {
    pages = make_unique<InMemPages>(fName, numBytesForElement,
        this->dataType.typeID != NODE && this->dataType.typeID != UNSTRUCTURED,
        listsMetadata->getNumPages());
}

void InMemLists::saveToFile() {
    listsMetadata->saveToDisk(fName);
    pages->saveToFile();
}

void InMemLists::setElement(uint32_t header, node_offset_t nodeOffset, uint64_t pos, uint8_t* val) {
    auto cursor = InMemListsUtils::calcPageElementCursor(
        header, pos, numBytesForElement, nodeOffset, *listsMetadata, true /* hasNULLBytes */);
    pages->pages[cursor.pageIdx]->write(
        cursor.pos * numBytesForElement, cursor.pos, val, numBytesForElement);
}

void InMemAdjLists::setElement(
    uint32_t header, node_offset_t nodeOffset, uint64_t pos, uint8_t* val) {
    auto cursor = InMemListsUtils::calcPageElementCursor(
        header, pos, numBytesForElement, nodeOffset, *listsMetadata, false /* hasNULLBytes */);
    auto node = *(nodeID_t*)val;
    pages->pages[cursor.pageIdx]->write(cursor.pos * compressionScheme.getNumTotalBytes(),
        cursor.pos, (uint8_t*)&node.label, compressionScheme.getNumBytesForLabel(),
        (uint8_t*)&node.offset, compressionScheme.getNumBytesForOffset());
}

void InMemAdjLists::saveToFile() {
    listHeaders->saveToDisk(fName);
    InMemLists::saveToFile();
}

InMemListsWithOverflow::InMemListsWithOverflow(string fName, DataType dataType)
    : InMemLists{move(fName), move(dataType), Types::getDataTypeSize(dataType)} {
    assert(dataType.typeID == STRING || dataType.typeID == LIST || dataType.typeID == UNSTRUCTURED);
    overflowPages =
        make_unique<InMemOverflowPages>(StorageUtils::getOverflowPagesFName(this->fName));
}

void InMemListsWithOverflow::saveToFile() {
    InMemLists::saveToFile();
    overflowPages->saveToFile();
}

void InMemUnstructuredLists::setUnstructuredElement(PageByteCursor& cursor, uint32_t propertyKey,
    DataTypeID dataTypeID, const uint8_t* val, PageByteCursor* overflowCursor) {
    PageByteCursor localCursor{cursor};
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
        auto gfString = overflowPages->addString((const char*)val, *overflowCursor);
        val = (uint8_t*)(&gfString);
        setComponentOfUnstrProperty(localCursor, Types::getDataTypeSize(dataTypeID), val);
    } break;
    case LIST: {
        auto gfList = overflowPages->addList(*(Literal*)val, *overflowCursor);
        val = (uint8_t*)(&gfList);
        setComponentOfUnstrProperty(localCursor, Types::getDataTypeSize(dataTypeID), val);
    } break;
    default:
        throw LoaderException("Unsupported data type for unstructured list.");
    }
}

void InMemUnstructuredLists::setComponentOfUnstrProperty(
    PageByteCursor& localCursor, uint8_t len, const uint8_t* val) {
    if (DEFAULT_PAGE_SIZE - localCursor.offset >= len) {
        memcpy(pages->pages[localCursor.idx]->data + localCursor.offset, val, len);
        localCursor.offset += len;
    } else {
        auto diff = DEFAULT_PAGE_SIZE - localCursor.offset;
        auto writeOffset = pages->pages[localCursor.idx]->data + localCursor.offset;
        memcpy(writeOffset, val, diff);
        auto left = len - diff;
        localCursor.idx++;
        localCursor.offset = 0;
        writeOffset = pages->pages[localCursor.idx]->data + localCursor.offset;
        memcpy(writeOffset, val + diff, left);
        localCursor.offset = left;
    }
}

void InMemUnstructuredLists::saveToFile() {
    listHeaders->saveToDisk(fName);
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

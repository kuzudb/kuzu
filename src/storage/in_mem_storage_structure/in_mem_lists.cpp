#include "storage/in_mem_storage_structure/in_mem_lists.h"

#include "storage/storage_structure/lists/lists.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

PageElementCursor InMemListsUtils::calcPageElementCursor(uint32_t header, uint64_t reversePos,
    uint8_t numBytesPerElement, offset_t nodeOffset, ListsMetadataBuilder& metadataBuilder,
    bool hasNULLBytes) {
    PageElementCursor cursor;
    auto numElementsInAPage = PageUtils::getNumElementsInAPage(numBytesPerElement, hasNULLBytes);
    if (ListHeaders::isALargeList(header)) {
        auto lAdjListIdx = ListHeaders::getLargeListIdx(header);
        auto pos = metadataBuilder.getNumElementsInLargeLists(lAdjListIdx) - reversePos;
        cursor = PageUtils::getPageElementCursorForPos(pos, numElementsInAPage);
        cursor.pageIdx = metadataBuilder.getPageMapperForLargeListIdx(lAdjListIdx)(cursor.pageIdx);
    } else {
        auto chunkId = StorageUtils::getListChunkIdx(nodeOffset);
        auto [listLen, csrOffset] = ListHeaders::getSmallListLenAndCSROffset(header);
        auto pos = listLen - reversePos;
        cursor = PageUtils::getPageElementCursorForPos(csrOffset + pos, numElementsInAPage);
        cursor.pageIdx = metadataBuilder.getPageMapperForChunkIdx(chunkId)(
            (csrOffset + pos) / numElementsInAPage);
    }
    return cursor;
}

InMemLists::InMemLists(
    std::string fName, DataType dataType, uint64_t numBytesForElement, uint64_t numNodes)
    : fName{std::move(fName)}, dataType{std::move(dataType)}, numBytesForElement{
                                                                  numBytesForElement} {
    listsMetadataBuilder = make_unique<ListsMetadataBuilder>(this->fName);
    auto numChunks = StorageUtils::getListChunkIdx(numNodes);
    if (0 != (numNodes & (ListsMetadataConstants::LISTS_CHUNK_SIZE - 1))) {
        numChunks++;
    }
    listsMetadataBuilder->initChunkPageLists(numChunks);
    inMemFile = make_unique<InMemFile>(
        this->fName, numBytesForElement, this->dataType.typeID != INTERNAL_ID);
}

void InMemLists::fillWithDefaultVal(
    uint8_t* defaultVal, uint64_t numNodes, AdjLists* adjList, const DataType& dataType) {
    PageByteCursor pageByteCursor{};
    auto fillInMemListsFunc = getFillInMemListsFunc(dataType);
    for (auto i = 0; i < numNodes; i++) {
        auto header = adjList->getHeaders()->getHeader(i);
        auto numElementsInList = adjList->getNumElementsFromListHeader(i);
        for (auto j = 0u; j < numElementsInList; j++) {
            fillInMemListsFunc(
                this, defaultVal, pageByteCursor, i, header, numElementsInList - j, dataType);
        }
    }
}

void InMemLists::saveToFile() {
    listsMetadataBuilder->saveToDisk();
    inMemFile->flush();
}

void InMemLists::setElement(uint32_t header, offset_t nodeOffset, uint64_t pos, uint8_t* val) {
    auto cursor = InMemListsUtils::calcPageElementCursor(header, pos, numBytesForElement,
        nodeOffset, *listsMetadataBuilder, true /* hasNULLBytes */);
    inMemFile->getPage(cursor.pageIdx)
        ->write(cursor.elemPosInPage * numBytesForElement, cursor.elemPosInPage, val,
            numBytesForElement);
}

void InMemAdjLists::setElement(uint32_t header, offset_t nodeOffset, uint64_t pos, uint8_t* val) {
    auto cursor = InMemListsUtils::calcPageElementCursor(header, pos, numBytesForElement,
        nodeOffset, *listsMetadataBuilder, false /* hasNULLBytes */);
    auto node = (nodeID_t*)val;
    inMemFile->getPage(cursor.pageIdx)
        ->writeNodeID(node, cursor.elemPosInPage * numBytesForElement, cursor.elemPosInPage);
}

void InMemLists::initListsMetadataAndAllocatePages(
    uint64_t numNodes, ListHeaders* listHeaders, ListsMetadata* listsMetadata) {
    initLargeListPageLists(numNodes, listHeaders);
    offset_t nodeOffset = 0u;
    auto largeListIdx = 0u;
    auto numElementsPerPage =
        PageUtils::getNumElementsInAPage(numBytesForElement, true /* hasNull */);
    auto numChunks = StorageUtils::getNumChunks(numNodes);
    for (auto chunkIdx = 0u; chunkIdx < numChunks; chunkIdx++) {
        uint64_t numPages = 0u, offsetInPage = 0u;
        auto lastNodeOffsetInChunk =
            std::min(nodeOffset + ListsMetadataConstants::LISTS_CHUNK_SIZE, numNodes);
        while (nodeOffset < lastNodeOffsetInChunk) {
            auto header = listHeaders->getHeader(nodeOffset);
            auto numElementsInList = ListHeaders::isALargeList(header) ?
                                         listsMetadata->getNumElementsInLargeLists(
                                             ListHeaders::getLargeListIdx(header)) :
                                         ListHeaders::getSmallListLen(header);
            if (ListHeaders::isALargeList(header)) {
                allocatePagesForLargeList(numElementsInList, numElementsPerPage, largeListIdx);
            } else {
                calculatePagesForSmallList(
                    numPages, offsetInPage, numElementsInList, numElementsPerPage);
            }
            nodeOffset++;
        }
        if (offsetInPage != 0) {
            numPages++;
        }
        listsMetadataBuilder->populateChunkPageList(chunkIdx, numPages, inMemFile->getNumPages());
        inMemFile->addNewPages(numPages);
    }
}

void InMemLists::initLargeListPageLists(uint64_t numNodes, ListHeaders* listHeaders) {
    auto largeListIdx = 0u;
    for (offset_t nodeOffset = 0; nodeOffset < numNodes; nodeOffset++) {
        if (ListHeaders::isALargeList(listHeaders->getHeader(nodeOffset))) {
            largeListIdx++;
        }
    }
    listsMetadataBuilder->initLargeListPageLists(largeListIdx);
}

void InMemLists::allocatePagesForLargeList(
    uint64_t numElementsInList, uint64_t numElementsPerPage, uint32_t& largeListIdx) {
    auto numPagesForLargeList =
        numElementsInList / numElementsPerPage + (numElementsInList % numElementsPerPage ? 1 : 0);
    listsMetadataBuilder->populateLargeListPageList(
        largeListIdx, numPagesForLargeList, numElementsInList, inMemFile->getNumPages());
    inMemFile->addNewPages(numPagesForLargeList);
    largeListIdx++;
}

void InMemLists::calculatePagesForSmallList(uint64_t& numPages, uint64_t& offsetInPage,
    uint64_t numElementsInList, uint64_t numElementsPerPage) {
    while (numElementsInList + offsetInPage > numElementsPerPage) {
        numElementsInList -= (numElementsPerPage - offsetInPage);
        numPages++;
        offsetInPage = 0;
    }
    offsetInPage += numElementsInList;
}

void InMemLists::fillInMemListsWithStrValFunc(InMemLists* inMemLists, uint8_t* defaultVal,
    PageByteCursor& pageByteCursor, offset_t nodeOffset, list_header_t header, uint64_t posInList,
    const DataType& dataType) {
    auto strVal = *(ku_string_t*)defaultVal;
    inMemLists->getInMemOverflowFile()->copyStringOverflow(
        pageByteCursor, reinterpret_cast<uint8_t*>(strVal.overflowPtr), &strVal);
    inMemLists->setElement(header, nodeOffset, posInList, reinterpret_cast<uint8_t*>(&strVal));
}

void InMemLists::fillInMemListsWithListValFunc(InMemLists* inMemLists, uint8_t* defaultVal,
    PageByteCursor& pageByteCursor, offset_t nodeOffset, list_header_t header, uint64_t posInList,
    const DataType& dataType) {
    auto listVal = *reinterpret_cast<ku_list_t*>(defaultVal);
    inMemLists->getInMemOverflowFile()->copyListOverflowToFile(
        pageByteCursor, &listVal, dataType.childType.get());
    inMemLists->setElement(header, nodeOffset, posInList, reinterpret_cast<uint8_t*>(&listVal));
}

fill_in_mem_lists_function_t InMemLists::getFillInMemListsFunc(const DataType& dataType) {
    switch (dataType.typeID) {
    case INT64:
    case DOUBLE:
    case BOOL:
    case DATE:
    case TIMESTAMP:
    case INTERVAL:
    case FIXED_LIST: {
        return fillInMemListsWithNonOverflowValFunc;
    }
    case STRING: {
        return fillInMemListsWithStrValFunc;
    }
    case VAR_LIST: {
        return fillInMemListsWithListValFunc;
    }
    default: {
        assert(false);
    }
    }
}

void InMemAdjLists::saveToFile() {
    listHeadersBuilder->saveToDisk();
    InMemLists::saveToFile();
}

InMemListsWithOverflow::InMemListsWithOverflow(
    std::string fName, DataType dataType, uint64_t numNodes)
    : InMemLists{
          std::move(fName), std::move(dataType), Types::getDataTypeSize(dataType), numNodes} {
    assert(this->dataType.typeID == STRING || this->dataType.typeID == VAR_LIST);
    overflowInMemFile =
        make_unique<InMemOverflowFile>(StorageUtils::getOverflowFileName(this->fName));
}

void InMemListsWithOverflow::saveToFile() {
    InMemLists::saveToFile();
    overflowInMemFile->flush();
}

std::unique_ptr<InMemLists> InMemListsFactory::getInMemPropertyLists(
    const std::string& fName, const DataType& dataType, uint64_t numNodes) {
    switch (dataType.typeID) {
    case INT64:
    case INT32:
    case INT16:
    case DOUBLE:
    case FLOAT:
    case BOOL:
    case DATE:
    case TIMESTAMP:
    case INTERVAL:
    case FIXED_LIST:
        return make_unique<InMemLists>(fName, dataType, Types::getDataTypeSize(dataType), numNodes);
    case STRING:
        return make_unique<InMemStringLists>(fName, numNodes);
    case VAR_LIST:
        return make_unique<InMemListLists>(fName, dataType, numNodes);
    case INTERNAL_ID:
        return make_unique<InMemRelIDLists>(fName, numNodes);
    default:
        throw CopyException("Invalid type for property list creation.");
    }
}

} // namespace storage
} // namespace kuzu

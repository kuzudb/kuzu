#include "storage/in_mem_storage_structure/in_mem_lists.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

PageElementCursor InMemLists::calcPageElementCursor(
    uint64_t reversePos, uint8_t numBytesPerElement, offset_t nodeOffset, bool hasNULLBytes) {
    PageElementCursor cursor;
    auto numElementsInAPage = PageUtils::getNumElementsInAPage(numBytesPerElement, hasNULLBytes);
    auto listSize = listHeadersBuilder->getListSize(nodeOffset);
    auto csrOffset = listHeadersBuilder->getCSROffset(nodeOffset);
    auto pos = listSize - reversePos;
    cursor = PageUtils::getPageElementCursorForPos(csrOffset + pos, numElementsInAPage);
    cursor.pageIdx = listsMetadataBuilder->getPageMapperForChunkIdx(
        StorageUtils::getListChunkIdx(nodeOffset))((csrOffset + pos) / numElementsInAPage);
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
    uint8_t* defaultVal, uint64_t numNodes, ListHeaders* listHeaders) {
    PageByteCursor pageByteCursor{};
    auto fillInMemListsFunc = getFillInMemListsFunc(dataType);
    for (auto i = 0; i < numNodes; i++) {
        auto numElementsInList = listHeaders->getListSize(i);
        for (auto j = 0u; j < numElementsInList; j++) {
            fillInMemListsFunc(
                this, defaultVal, pageByteCursor, i, numElementsInList - j, dataType);
        }
    }
}

void InMemLists::saveToFile() {
    listsMetadataBuilder->saveToDisk();
    inMemFile->flush();
}

void InMemLists::setElement(offset_t nodeOffset, uint64_t pos, uint8_t* val) {
    auto cursor =
        calcPageElementCursor(pos, numBytesForElement, nodeOffset, true /* hasNULLBytes */);
    inMemFile->getPage(cursor.pageIdx)
        ->write(cursor.elemPosInPage * numBytesForElement, cursor.elemPosInPage, val,
            numBytesForElement);
}

void InMemAdjLists::setElement(offset_t nodeOffset, uint64_t pos, uint8_t* val) {
    auto cursor =
        calcPageElementCursor(pos, numBytesForElement, nodeOffset, false /* hasNULLBytes */);
    auto node = (nodeID_t*)val;
    inMemFile->getPage(cursor.pageIdx)
        ->writeNodeID(node, cursor.elemPosInPage * numBytesForElement, cursor.elemPosInPage);
}

void InMemLists::initListsMetadataAndAllocatePages(
    uint64_t numNodes, ListHeaders* listHeaders, ListsMetadata* listsMetadata) {
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
            auto numElementsInList = listHeaders->getListSize(nodeOffset);
            calculatePagesForList(numPages, offsetInPage, numElementsInList, numElementsPerPage);
            nodeOffset++;
        }
        if (offsetInPage != 0) {
            numPages++;
        }
        listsMetadataBuilder->populateChunkPageList(chunkIdx, numPages, inMemFile->getNumPages());
        inMemFile->addNewPages(numPages);
    }
}

void InMemLists::calculatePagesForList(uint64_t& numPages, uint64_t& offsetInPage,
    uint64_t numElementsInList, uint64_t numElementsPerPage) {
    while (numElementsInList + offsetInPage > numElementsPerPage) {
        numElementsInList -= (numElementsPerPage - offsetInPage);
        numPages++;
        offsetInPage = 0;
    }
    offsetInPage += numElementsInList;
}

void InMemLists::fillInMemListsWithStrValFunc(InMemLists* inMemLists, uint8_t* defaultVal,
    PageByteCursor& pageByteCursor, offset_t nodeOffset, uint64_t posInList,
    const DataType& dataType) {
    auto strVal = *(ku_string_t*)defaultVal;
    inMemLists->getInMemOverflowFile()->copyStringOverflow(
        pageByteCursor, reinterpret_cast<uint8_t*>(strVal.overflowPtr), &strVal);
    inMemLists->setElement(nodeOffset, posInList, reinterpret_cast<uint8_t*>(&strVal));
}

void InMemLists::fillInMemListsWithListValFunc(InMemLists* inMemLists, uint8_t* defaultVal,
    PageByteCursor& pageByteCursor, offset_t nodeOffset, uint64_t posInList,
    const DataType& dataType) {
    auto listVal = *reinterpret_cast<ku_list_t*>(defaultVal);
    inMemLists->getInMemOverflowFile()->copyListOverflowToFile(
        pageByteCursor, &listVal, dataType.getChildType());
    inMemLists->setElement(nodeOffset, posInList, reinterpret_cast<uint8_t*>(&listVal));
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

InMemListsWithOverflow::InMemListsWithOverflow(std::string fName, DataType dataType,
    uint64_t numNodes, std::shared_ptr<ListHeadersBuilder> listHeadersBuilder)
    : InMemLists{
          std::move(fName), std::move(dataType), Types::getDataTypeSize(dataType), numNodes} {
    assert(this->dataType.typeID == STRING || this->dataType.typeID == VAR_LIST);
    overflowInMemFile =
        make_unique<InMemOverflowFile>(StorageUtils::getOverflowFileName(this->fName));
    this->listHeadersBuilder = std::move(listHeadersBuilder);
}

void InMemListsWithOverflow::saveToFile() {
    InMemLists::saveToFile();
    overflowInMemFile->flush();
}

std::unique_ptr<InMemLists> InMemListsFactory::getInMemPropertyLists(const std::string& fName,
    const DataType& dataType, uint64_t numNodes,
    std::shared_ptr<ListHeadersBuilder> listHeadersBuilder) {
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
        return make_unique<InMemLists>(fName, dataType, Types::getDataTypeSize(dataType), numNodes,
            std::move(listHeadersBuilder));
    case STRING:
        return make_unique<InMemStringLists>(fName, numNodes, std::move(listHeadersBuilder));
    case VAR_LIST:
        return make_unique<InMemListLists>(
            fName, dataType, numNodes, std::move(listHeadersBuilder));
    case INTERNAL_ID:
        return make_unique<InMemRelIDLists>(fName, numNodes, std::move(listHeadersBuilder));
    default:
        throw CopyException("Invalid type for property list creation.");
    }
}

} // namespace storage
} // namespace kuzu

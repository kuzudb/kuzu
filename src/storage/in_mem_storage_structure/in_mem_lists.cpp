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
    std::string fName, LogicalType dataType, uint64_t numBytesForElement, uint64_t numNodes)
    : fName{std::move(fName)}, dataType{std::move(dataType)}, numBytesForElement{
                                                                  numBytesForElement} {
    listsMetadataBuilder = make_unique<ListsMetadataBuilder>(this->fName);
    auto numChunks = StorageUtils::getListChunkIdx(numNodes);
    if (0 != (numNodes & (ListsMetadataConstants::LISTS_CHUNK_SIZE - 1))) {
        numChunks++;
    }
    listsMetadataBuilder->initChunkPageLists(numChunks);
    inMemFile = make_unique<InMemFile>(this->fName, numBytesForElement,
        this->dataType.getLogicalTypeID() != LogicalTypeID::INTERNAL_ID);
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
    const LogicalType& dataType) {
    auto strVal = *(ku_string_t*)defaultVal;
    inMemLists->getInMemOverflowFile()->copyStringOverflow(
        pageByteCursor, reinterpret_cast<uint8_t*>(strVal.overflowPtr), &strVal);
    inMemLists->setElement(nodeOffset, posInList, reinterpret_cast<uint8_t*>(&strVal));
}

void InMemLists::fillInMemListsWithListValFunc(InMemLists* inMemLists, uint8_t* defaultVal,
    PageByteCursor& pageByteCursor, offset_t nodeOffset, uint64_t posInList,
    const LogicalType& dataType) {
    auto listVal = *reinterpret_cast<ku_list_t*>(defaultVal);
    inMemLists->getInMemOverflowFile()->copyListOverflowToFile(
        pageByteCursor, &listVal, VarListType::getChildType(&dataType));
    inMemLists->setElement(nodeOffset, posInList, reinterpret_cast<uint8_t*>(&listVal));
}

fill_in_mem_lists_function_t InMemLists::getFillInMemListsFunc(const LogicalType& dataType) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::INT64:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::BOOL:
    case LogicalTypeID::DATE:
    case LogicalTypeID::TIMESTAMP:
    case LogicalTypeID::INTERVAL:
    case LogicalTypeID::FIXED_LIST: {
        return fillInMemListsWithNonOverflowValFunc;
    }
    case LogicalTypeID::STRING: {
        return fillInMemListsWithStrValFunc;
    }
    case LogicalTypeID::VAR_LIST: {
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

InMemListsWithOverflow::InMemListsWithOverflow(std::string fName, LogicalType dataType,
    uint64_t numNodes, std::shared_ptr<ListHeadersBuilder> listHeadersBuilder)
    : InMemLists{std::move(fName), std::move(dataType),
          storage::StorageUtils::getDataTypeSize(dataType), numNodes} {
    assert(this->dataType.getLogicalTypeID() == LogicalTypeID::STRING ||
           this->dataType.getLogicalTypeID() == LogicalTypeID::VAR_LIST);
    overflowInMemFile =
        make_unique<InMemOverflowFile>(StorageUtils::getOverflowFileName(this->fName));
    this->listHeadersBuilder = std::move(listHeadersBuilder);
}

void InMemListsWithOverflow::saveToFile() {
    InMemLists::saveToFile();
    overflowInMemFile->flush();
}

std::unique_ptr<InMemLists> InMemListsFactory::getInMemPropertyLists(const std::string& fName,
    const LogicalType& dataType, uint64_t numNodes,
    std::shared_ptr<ListHeadersBuilder> listHeadersBuilder) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT16:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::BOOL:
    case LogicalTypeID::DATE:
    case LogicalTypeID::TIMESTAMP:
    case LogicalTypeID::INTERVAL:
    case LogicalTypeID::FIXED_LIST:
        return make_unique<InMemLists>(fName, dataType,
            storage::StorageUtils::getDataTypeSize(dataType), numNodes,
            std::move(listHeadersBuilder));
    case LogicalTypeID::STRING:
        return make_unique<InMemStringLists>(fName, numNodes, std::move(listHeadersBuilder));
    case LogicalTypeID::VAR_LIST:
        return make_unique<InMemListLists>(
            fName, dataType, numNodes, std::move(listHeadersBuilder));
    case LogicalTypeID::INTERNAL_ID:
        return make_unique<InMemRelIDLists>(fName, numNodes, std::move(listHeadersBuilder));
    default:
        throw CopyException("Invalid type for property list creation.");
    }
}

} // namespace storage
} // namespace kuzu

#include "storage/in_mem_storage_structure/in_mem_lists.h"

namespace kuzu {
namespace storage {

PageElementCursor InMemListsUtils::calcPageElementCursor(uint32_t header, uint64_t reversePos,
    uint8_t numBytesPerElement, node_offset_t nodeOffset, ListsMetadataBuilder& metadataBuilder,
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
    string fName, DataType dataType, uint64_t numBytesForElement, uint64_t numNodes)
    : fName{move(fName)}, dataType{move(dataType)}, numBytesForElement{numBytesForElement} {
    listsMetadataBuilder = make_unique<ListsMetadataBuilder>(this->fName);
    auto numChunks = StorageUtils::getListChunkIdx(numNodes);
    if (0 != (numNodes & (ListsMetadataConfig::LISTS_CHUNK_SIZE - 1))) {
        numChunks++;
    }
    listsMetadataBuilder->initChunkPageLists(numChunks);
    inMemFile =
        make_unique<InMemFile>(this->fName, numBytesForElement, this->dataType.typeID != NODE_ID);
}

void InMemLists::saveToFile() {
    listsMetadataBuilder->saveToDisk();
    inMemFile->flush();
}

void InMemLists::setElement(uint32_t header, node_offset_t nodeOffset, uint64_t pos, uint8_t* val) {
    auto cursor = InMemListsUtils::calcPageElementCursor(header, pos, numBytesForElement,
        nodeOffset, *listsMetadataBuilder, true /* hasNULLBytes */);
    inMemFile->getPage(cursor.pageIdx)
        ->write(cursor.elemPosInPage * numBytesForElement, cursor.elemPosInPage, val,
            numBytesForElement);
}

void InMemAdjLists::setElement(
    uint32_t header, node_offset_t nodeOffset, uint64_t pos, uint8_t* val) {
    auto cursor = InMemListsUtils::calcPageElementCursor(header, pos, numBytesForElement,
        nodeOffset, *listsMetadataBuilder, false /* hasNULLBytes */);
    auto node = (nodeID_t*)val;
    inMemFile->getPage(cursor.pageIdx)
        ->writeNodeID(node, cursor.elemPosInPage * numBytesForElement, cursor.elemPosInPage,
            nodeIDCompressionScheme);
}

void InMemAdjLists::saveToFile() {
    listHeadersBuilder->saveToDisk();
    InMemLists::saveToFile();
}

InMemListsWithOverflow::InMemListsWithOverflow(string fName, DataType dataType, uint64_t numNodes)
    : InMemLists{move(fName), move(dataType), Types::getDataTypeSize(dataType), numNodes} {
    assert(this->dataType.typeID == STRING || this->dataType.typeID == LIST);
    overflowInMemFile =
        make_unique<InMemOverflowFile>(StorageUtils::getOverflowFileName(this->fName));
}

void InMemListsWithOverflow::saveToFile() {
    InMemLists::saveToFile();
    overflowInMemFile->flush();
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
        return make_unique<InMemLists>(fName, dataType, Types::getDataTypeSize(dataType), numNodes);
    case STRING:
        return make_unique<InMemStringLists>(fName, numNodes);
    case LIST:
        return make_unique<InMemListLists>(fName, dataType, numNodes);
    default:
        throw CopyCSVException("Invalid type for property list creation.");
    }
}

} // namespace storage
} // namespace kuzu

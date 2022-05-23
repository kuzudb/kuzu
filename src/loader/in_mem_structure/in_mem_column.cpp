#include "src/loader/include/in_mem_structure/in_mem_column.h"

namespace graphflow {
namespace loader {

InMemColumn::InMemColumn(
    std::string fName, DataType dataType, uint64_t numBytesForElement, uint64_t numElements)
    : fName{move(fName)}, dataType{move(dataType)}, numBytesForElement{numBytesForElement} {
    assert(dataType.typeID != UNSTRUCTURED);
    numElementsInAPage = PageUtils::getNumElementsInAPageWithNULLBytes(numBytesForElement);
    auto numPages = numElements / numElementsInAPage;
    if (0 != numElements % numElementsInAPage) {
        numPages++;
    }
    pages =
        make_unique<InMemPages>(this->fName, numBytesForElement, true /* hasNULLBytes */, numPages);
};

void InMemColumn::saveToFile() {
    pages->saveToFile();
}

void InMemColumn::setElement(node_offset_t offset, const uint8_t* val) {
    auto cursor = getPageElementCursorForOffset(offset);
    pages->pages[cursor.pageIdx]->write(
        cursor.pos * numBytesForElement, cursor.pos, val, numBytesForElement);
}

InMemColumnWithOverflow::InMemColumnWithOverflow(
    string fName, DataType dataType, uint64_t numElements)
    : InMemColumn{move(fName), move(dataType), Types::getDataTypeSize(dataType), numElements} {
    assert(dataType.typeID == STRING || dataType.typeID == LIST);
    overflowPages =
        make_unique<InMemOverflowPages>(StorageUtils::getOverflowPagesFName(this->fName));
}

void InMemColumnWithOverflow::saveToFile() {
    overflowPages->saveToFile();
    InMemColumn::saveToFile();
}

void InMemAdjColumn::setElement(node_offset_t offset, const uint8_t* val) {
    auto node = *(nodeID_t*)val;
    auto cursor = getPageElementCursorForOffset(offset);
    pages->pages[cursor.pageIdx]->write(cursor.pos * numBytesForElement, cursor.pos,
        (uint8_t*)(&node.label), compressionScheme.getNumBytesForLabel(), (uint8_t*)(&node.offset),
        compressionScheme.getNumBytesForOffset());
}

unique_ptr<InMemColumn> InMemColumnFactory::getInMemPropertyColumn(
    const string& fName, const DataType& dataType, uint64_t numElements) {
    switch (dataType.typeID) {
    case INT64:
    case DOUBLE:
    case BOOL:
    case DATE:
    case TIMESTAMP:
    case INTERVAL:
        return make_unique<InMemColumn>(
            fName, dataType, Types::getDataTypeSize(dataType), numElements);
    case STRING:
        return make_unique<InMemStringColumn>(fName, numElements);
    case LIST:
        return make_unique<InMemListColumn>(fName, dataType, numElements);
    default:
        throw LoaderException("Invalid type for property column creation.");
    }
}

} // namespace loader
} // namespace graphflow

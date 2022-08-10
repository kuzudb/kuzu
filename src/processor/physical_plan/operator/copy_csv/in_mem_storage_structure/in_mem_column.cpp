#include "src/processor/include/physical_plan/operator/copy_csv/in_mem_storage_structure/in_mem_column.h"

namespace graphflow {
namespace processor {

InMemColumn::InMemColumn(
    std::string fName, DataType dataType, uint64_t numBytesForElement, uint64_t numElements)
    : fName{move(fName)}, dataType{move(dataType)}, numBytesForElement{numBytesForElement} {
    assert(this->dataType.typeID != UNSTRUCTURED);
    numElementsInAPage = PageUtils::getNumElementsInAPage(numBytesForElement, true /* hasNull */);
    auto numPages = ceil((double)numElements / (double)numElementsInAPage);
    inMemFile =
        make_unique<InMemFile>(this->fName, numBytesForElement, true /* hasNULLBytes */, numPages);
}

void InMemColumn::saveToFile() {
    inMemFile->flush();
}

void InMemColumn::setElement(node_offset_t offset, const uint8_t* val) {
    auto cursor = getPageElementCursorForOffset(offset);
    inMemFile->getPage(cursor.pageIdx)
        ->write(cursor.posInPage * numBytesForElement, cursor.posInPage, val, numBytesForElement);
}

InMemColumnWithOverflow::InMemColumnWithOverflow(
    string fName, DataType dataType, uint64_t numElements)
    : InMemColumn{move(fName), move(dataType), Types::getDataTypeSize(dataType), numElements} {
    assert(this->dataType.typeID == STRING || this->dataType.typeID == LIST);
    inMemOverflowFile =
        make_unique<InMemOverflowFile>(StorageUtils::getOverflowPagesFName(this->fName));
}

void InMemColumnWithOverflow::saveToFile() {
    inMemOverflowFile->flush();
    InMemColumn::saveToFile();
}

void InMemAdjColumn::setElement(node_offset_t offset, const uint8_t* val) {
    auto node = (nodeID_t*)val;
    auto cursor = getPageElementCursorForOffset(offset);
    inMemFile->getPage(cursor.pageIdx)
        ->write(node, cursor.posInPage * numBytesForElement, cursor.posInPage, compressionScheme);
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

} // namespace processor
} // namespace graphflow

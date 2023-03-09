#include "storage/in_mem_storage_structure/in_mem_column.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

InMemColumn::InMemColumn(
    std::string fName, DataType dataType, uint64_t numBytesForElement, uint64_t numElements)
    : fName{std::move(fName)}, dataType{std::move(dataType)}, numBytesForElement{
                                                                  numBytesForElement} {
    numElementsInAPage = PageUtils::getNumElementsInAPage(numBytesForElement, true /* hasNull */);
    auto numPages = ceil((double)numElements / (double)numElementsInAPage);
    inMemFile =
        make_unique<InMemFile>(this->fName, numBytesForElement, true /* hasNULLBytes */, numPages);
}

void InMemColumn::fillWithDefaultVal(
    uint8_t* defaultVal, uint64_t numNodes, const DataType& dataType_) {
    PageByteCursor pageByteCursor{};
    auto fillInMemColumnFunc = getFillInMemColumnFunc(dataType_);
    for (auto i = 0; i < numNodes; i++) {
        fillInMemColumnFunc(this, defaultVal, pageByteCursor, i, dataType_);
    }
}

void InMemColumn::saveToFile() {
    inMemFile->flush();
}

void InMemColumn::setElement(offset_t offset, const uint8_t* val) {
    auto cursor = getPageElementCursorForOffset(offset);
    inMemFile->getPage(cursor.pageIdx)
        ->write(cursor.elemPosInPage * numBytesForElement, cursor.elemPosInPage, val,
            numBytesForElement);
}

void InMemColumn::fillInMemColumnWithStrValFunc(InMemColumn* inMemColumn, uint8_t* defaultVal,
    PageByteCursor& pageByteCursor, offset_t nodeOffset, const DataType& dataType) {
    auto strVal = *reinterpret_cast<ku_string_t*>(defaultVal);
    if (strVal.len > ku_string_t::SHORT_STR_LENGTH) {
        inMemColumn->getInMemOverflowFile()->copyStringOverflow(
            pageByteCursor, reinterpret_cast<uint8_t*>(strVal.overflowPtr), &strVal);
    }
    inMemColumn->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&strVal));
}

void InMemColumn::fillInMemColumnWithListValFunc(InMemColumn* inMemColumn, uint8_t* defaultVal,
    PageByteCursor& pageByteCursor, offset_t nodeOffset, const DataType& dataType) {
    auto listVal = *reinterpret_cast<ku_list_t*>(defaultVal);
    inMemColumn->getInMemOverflowFile()->copyListOverflowToFile(
        pageByteCursor, &listVal, dataType.childType.get());
    inMemColumn->setElement(nodeOffset, reinterpret_cast<uint8_t*>(&listVal));
}

fill_in_mem_column_function_t InMemColumn::getFillInMemColumnFunc(const DataType& dataType) {
    switch (dataType.typeID) {
    case INT64:
    case DOUBLE:
    case BOOL:
    case DATE:
    case TIMESTAMP:
    case INTERVAL: {
        return fillInMemColumnWithNonOverflowValFunc;
    }
    case STRING: {
        return fillInMemColumnWithStrValFunc;
    }
    case VAR_LIST: {
        return fillInMemColumnWithListValFunc;
    }
    default: {
        assert(false);
    }
    }
}

InMemColumnWithOverflow::InMemColumnWithOverflow(
    std::string fName, DataType dataType, uint64_t numElements)
    : InMemColumn{
          std::move(fName), std::move(dataType), Types::getDataTypeSize(dataType), numElements} {
    assert(this->dataType.typeID == STRING || this->dataType.typeID == VAR_LIST);
    inMemOverflowFile =
        make_unique<InMemOverflowFile>(StorageUtils::getOverflowFileName(this->fName));
}

void InMemColumnWithOverflow::saveToFile() {
    inMemOverflowFile->flush();
    InMemColumn::saveToFile();
}

void InMemAdjColumn::setElement(offset_t offset, const uint8_t* val) {
    auto node = (nodeID_t*)val;
    auto cursor = getPageElementCursorForOffset(offset);
    inMemFile->getPage(cursor.pageIdx)
        ->writeNodeID(node, cursor.elemPosInPage * numBytesForElement, cursor.elemPosInPage);
}

std::unique_ptr<InMemColumn> InMemColumnFactory::getInMemPropertyColumn(
    const std::string& fName, const DataType& dataType, uint64_t numElements) {
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
        return make_unique<InMemColumn>(
            fName, dataType, Types::getDataTypeSize(dataType), numElements);
    case STRING:
        return make_unique<InMemStringColumn>(fName, numElements);
    case VAR_LIST:
        return make_unique<InMemListColumn>(fName, dataType, numElements);
    case INTERNAL_ID:
        return make_unique<InMemRelIDColumn>(fName, numElements);
    default:
        throw CopyException("Invalid type for property column creation.");
    }
}

} // namespace storage
} // namespace kuzu

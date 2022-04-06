#include "src/loader/include/in_mem_structure/in_mem_pages.h"

#include <fcntl.h>

#include "src/common/include/file_utils.h"
#include "src/common/include/type_utils.h"

namespace graphflow {
namespace loader {

InMemPages::InMemPages(
    string fName, uint16_t numBytesForElement, bool hasNULLBytes, uint64_t numPages)
    : fName{move(fName)} {
    auto numElementsInAPage =
        hasNULLBytes ? PageUtils::getNumElementsInAPageWithNULLBytes(numBytesForElement) :
                       PageUtils::getNumElementsInAPageWithoutNULLBytes(numBytesForElement);
    data = make_unique<uint8_t[]>(numPages << DEFAULT_PAGE_SIZE_LOG_2);
    pages.resize(numPages);
    for (auto i = 0u; i < numPages; i++) {
        pages[i] = make_unique<InMemPage>(
            numElementsInAPage, data.get() + (i << DEFAULT_PAGE_SIZE_LOG_2), hasNULLBytes);
    }
};

void InMemPages::saveToFile() {
    if (0 == fName.length()) {
        throw invalid_argument("InMemPages: Empty filename");
    }
    for (auto& page : pages) {
        page->encodeNULLBytes();
    }
    auto fileInfo = FileUtils::openFile(fName, O_WRONLY | O_CREAT);
    uint64_t byteSize = pages.size() << DEFAULT_PAGE_SIZE_LOG_2;
    FileUtils::writeToFile(fileInfo.get(), data.get(), byteSize, 0);
    FileUtils::closeFile(fileInfo->fd);
}

gf_string_t InMemOverflowPages::addString(const char* originalString, PageByteCursor& cursor) {
    gf_string_t encodedString;
    encodedString.len = strlen(originalString);
    memcpy(&encodedString.prefix, originalString, min(encodedString.len, 4u));
    if (encodedString.len <= 4) {
        return encodedString;
    }
    if (encodedString.len > 4 && encodedString.len <= 12) {
        memcpy(&encodedString.data, originalString + 4, encodedString.len - 4);
        return encodedString;
    }
    copyStringOverflow(cursor, (uint8_t*)originalString, &encodedString);
    return encodedString;
}

template<DataTypeID DT>
void InMemOverflowPages::copyOverflowValuesToPages(gf_list_t& result, const Literal& listVal,
    PageByteCursor& cursor, uint64_t numBytesOfSingleValue) {
    assert(DT == STRING || DT == LIST);
    auto overflowPageId = cursor.idx;
    auto overflowPageOffset = cursor.offset;
    cursor.offset += (result.size * numBytesOfSingleValue);
    for (auto i = 0u; i < listVal.listVal.size(); i++) {
        assert(listVal.listVal[i].dataType.typeID == DT);
        uint8_t* value = nullptr;
        if constexpr (DT == STRING) {
            auto gfStr = addString(listVal.listVal[i].strVal.c_str(), cursor);
            value = (uint8_t*)&gfStr;
        } else {
            auto gfList = addList(listVal.listVal[i], cursor);
            value = (uint8_t*)&gfList;
        }
        pages[overflowPageId]->write(overflowPageOffset + (i * numBytesOfSingleValue),
            overflowPageOffset + (i * numBytesOfSingleValue), value, numBytesOfSingleValue);
    }
}

gf_list_t InMemOverflowPages::addList(const Literal& listVal, PageByteCursor& cursor) {
    assert(listVal.dataType.typeID == LIST && !listVal.listVal.empty());
    gf_list_t result;
    auto childDataTypeID = listVal.listVal[0].dataType.typeID;
    auto numBytesOfSingleValue = Types::getDataTypeSize(childDataTypeID);
    result.size = listVal.listVal.size();
    if (cursor.offset + (result.size * numBytesOfSingleValue) >= DEFAULT_PAGE_SIZE ||
        0 > cursor.idx) {
        cursor.offset = 0;
        cursor.idx = getNewOverflowPageIdx();
    }
    TypeUtils::encodeOverflowPtr(result.overflowPtr, cursor.idx, cursor.offset);
    shared_lock lck(lock);
    switch (childDataTypeID) {
    case INT64:
    case DOUBLE:
    case BOOL:
    case DATE:
    case TIMESTAMP:
    case INTERVAL: {
        for (auto& literal : listVal.listVal) {
            assert(literal.dataType.typeID == childDataTypeID);
            pages[cursor.idx]->write(
                cursor.offset, cursor.offset, (uint8_t*)&literal.val, numBytesOfSingleValue);
            cursor.offset += numBytesOfSingleValue;
        }
    } break;
    case STRING: {
        copyOverflowValuesToPages<STRING>(result, listVal, cursor, numBytesOfSingleValue);
    } break;
    case LIST: {
        copyOverflowValuesToPages<LIST>(result, listVal, cursor, numBytesOfSingleValue);
    } break;
    default: {
        throw invalid_argument("Unsupported data type inside LIST.");
    }
    }
    return result;
}

void InMemOverflowPages::copyStringOverflow(
    PageByteCursor& cursor, uint8_t* ptrToCopy, gf_string_t* encodedString) {
    if (cursor.offset + encodedString->len - 4 >= DEFAULT_PAGE_SIZE || 0 > cursor.idx) {
        cursor.offset = 0;
        cursor.idx = getNewOverflowPageIdx();
    }
    shared_lock lck(lock);
    TypeUtils::encodeOverflowPtr(encodedString->overflowPtr, cursor.idx, cursor.offset);
    pages[cursor.idx]->write(cursor.offset, cursor.offset, ptrToCopy, encodedString->len);
    cursor.offset += encodedString->len;
}

void InMemOverflowPages::copyListOverflow(PageByteCursor& cursor, uint8_t* ptrToCopy,
    InMemOverflowPages* overflowPagesToCopy, gf_list_t* encodedList, DataType* childDataType) {
    auto numBytesOfSingleValue = Types::getDataTypeSize(*childDataType);
    if (cursor.offset + (encodedList->size * numBytesOfSingleValue) >= DEFAULT_PAGE_SIZE ||
        0 > cursor.idx) {
        cursor.offset = 0;
        cursor.idx = getNewOverflowPageIdx();
    }
    shared_lock lck(lock);
    TypeUtils::encodeOverflowPtr(encodedList->overflowPtr, cursor.idx, cursor.offset);
    pages[cursor.idx]->write(
        cursor.offset, cursor.offset, ptrToCopy, encodedList->size * numBytesOfSingleValue);
    cursor.offset += encodedList->size * numBytesOfSingleValue;
    if (childDataType->typeID == LIST) {
        auto childVals = (gf_list_t*)ptrToCopy;
        for (auto i = 0u; i < encodedList->size; i++) {
            PageByteCursor childCursor;
            TypeUtils::decodeOverflowPtr(
                childVals[i].overflowPtr, childCursor.idx, childCursor.offset);
            copyListOverflow(cursor,
                overflowPagesToCopy->pages[childCursor.idx]->data + childCursor.offset,
                overflowPagesToCopy, &childVals[i], childDataType->childType.get());
        }
    } else if (childDataType->typeID == STRING) {
        auto childVals = (gf_string_t*)ptrToCopy;
        for (auto i = 0u; i < encodedList->size; i++) {
            PageByteCursor childCursor;
            if (childVals[i].len > gf_string_t::SHORT_STR_LENGTH) {
                TypeUtils::decodeOverflowPtr(
                    childVals[i].overflowPtr, childCursor.idx, childCursor.offset);
                copyStringOverflow(cursor,
                    overflowPagesToCopy->pages[childCursor.idx]->data + childCursor.offset,
                    &childVals[i]);
            }
        }
    }
}

uint32_t InMemOverflowPages::getNewOverflowPageIdx() {
    unique_lock lck(lock);
    auto page = numUsedPages++;
    if (numUsedPages < pages.size()) {
        return page;
    }
    auto newNumPages = (size_t)(1.5 * pages.size());
    auto newData = new uint8_t[newNumPages << DEFAULT_PAGE_SIZE_LOG_2];
    memcpy(newData, data.get(), pages.size() << DEFAULT_PAGE_SIZE_LOG_2);
    data.reset(newData);
    auto oldNumPages = pages.size();
    pages.resize(newNumPages);
    for (auto i = 0; i < oldNumPages; i++) {
        pages[i]->data = data.get() + (i << DEFAULT_PAGE_SIZE_LOG_2);
    }
    auto numElementsInPage = PageUtils::getNumElementsInAPageWithoutNULLBytes(1);
    for (auto i = oldNumPages; i < newNumPages; i++) {
        pages[i] = make_unique<InMemPage>(
            numElementsInPage, data.get() + (i << DEFAULT_PAGE_SIZE_LOG_2), false /*hasNULLBytes*/);
    }
    return page;
}

void InMemOverflowPages::saveToFile() {
    if (0 == fName.length()) {
        throw invalid_argument("InMemPages: Empty filename");
    }
    for (auto& page : pages) {
        page->encodeNULLBytes();
    }
    auto fileInfo = FileUtils::openFile(fName, O_WRONLY | O_CREAT);
    auto bytesToWrite = numUsedPages << DEFAULT_PAGE_SIZE_LOG_2;
    FileUtils::writeToFile(fileInfo.get(), data.get(), bytesToWrite, 0);
    FileUtils::closeFile(fileInfo->fd);
}

} // namespace loader
} // namespace graphflow

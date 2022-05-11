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
        throw LoaderException("InMemPages: Empty filename");
    }
    for (auto& page : pages) {
        page->encodeNULLBytes();
    }
    auto fileInfo = FileUtils::openFile(fName, O_WRONLY | O_CREAT);
    uint64_t byteSize = pages.size() << DEFAULT_PAGE_SIZE_LOG_2;
    FileUtils::writeToFile(fileInfo.get(), data.get(), byteSize, 0);
    FileUtils::closeFile(fileInfo->fd);
}

gf_string_t InMemOverflowPages::addString(const char* rawString, PageByteCursor& overflowCursor) {
    gf_string_t gfString;
    gfString.len = strlen(rawString);
    memcpy(&gfString.prefix, rawString, min(gfString.len, 4u));
    if (gfString.len <= 4) {
        return gfString;
    }
    if (gfString.len > 4 && gfString.len <= 12) {
        memcpy(&gfString.data, rawString + 4, gfString.len - 4);
        return gfString;
    }
    copyStringOverflow(overflowCursor, (uint8_t*)rawString, &gfString);
    return gfString;
}

void InMemOverflowPages::copyFixedSizedValuesToPages(
    const Literal& listVal, PageByteCursor& overflowCursor, uint64_t numBytesOfListElement) {
    shared_lock lck(lock);
    for (auto& literal : listVal.listVal) {
        pages[overflowCursor.idx]->write(overflowCursor.offset, overflowCursor.offset,
            (uint8_t*)&literal.val, numBytesOfListElement);
        overflowCursor.offset += numBytesOfListElement;
    }
}

template<DataTypeID DT>
void InMemOverflowPages::copyVarSizedValuesToPages(gf_list_t& resultGFList,
    const Literal& listLiteral, PageByteCursor& overflowCursor, uint64_t numBytesOfListElement) {
    assert(DT == STRING || DT == LIST);
    auto overflowPageOffset = overflowCursor.offset;
    // Reserve space for gf_list or gf_string objects.
    overflowCursor.offset += (resultGFList.size * numBytesOfListElement);
    if constexpr (DT == STRING) {
        vector<gf_string_t> gfStrings(listLiteral.listVal.size());
        for (auto i = 0u; i < listLiteral.listVal.size(); i++) {
            assert(listLiteral.listVal[i].dataType.typeID == STRING);
            gfStrings[i] = addString(listLiteral.listVal[i].strVal.c_str(), overflowCursor);
        }
        shared_lock lck(lock);
        for (auto i = 0u; i < listLiteral.listVal.size(); i++) {
            pages[overflowCursor.idx]->write(overflowPageOffset + (i * numBytesOfListElement),
                overflowPageOffset + (i * numBytesOfListElement), (uint8_t*)&gfStrings[i],
                numBytesOfListElement);
        }
    } else if (DT == LIST) {
        vector<gf_list_t> gfLists(listLiteral.listVal.size());
        for (auto i = 0u; i < listLiteral.listVal.size(); i++) {
            assert(listLiteral.listVal[i].dataType.typeID == LIST);
            gfLists[i] = addList(listLiteral.listVal[i], overflowCursor);
        }
        shared_lock lck(lock);
        for (auto i = 0u; i < listLiteral.listVal.size(); i++) {
            pages[overflowCursor.idx]->write(overflowPageOffset + (i * numBytesOfListElement),
                overflowPageOffset + (i * numBytesOfListElement), (uint8_t*)&gfLists[i],
                numBytesOfListElement);
        }
    } else {
        assert(false);
    }
}

gf_list_t InMemOverflowPages::addList(const Literal& listLiteral, PageByteCursor& overflowCursor) {
    assert(listLiteral.dataType.typeID == LIST && !listLiteral.listVal.empty());
    gf_list_t resultGFList;
    auto childDataTypeID = listLiteral.listVal[0].dataType.typeID;
    auto numBytesOfListElement = Types::getDataTypeSize(childDataTypeID);
    resultGFList.size = listLiteral.listVal.size();
    // Allocate a new page if necessary.
    if (overflowCursor.offset + (resultGFList.size * numBytesOfListElement) >= DEFAULT_PAGE_SIZE ||
        0 > overflowCursor.idx) {
        overflowCursor.offset = 0;
        overflowCursor.idx = getNewOverflowPageIdx();
    }
    TypeUtils::encodeOverflowPtr(
        resultGFList.overflowPtr, overflowCursor.idx, overflowCursor.offset);
    switch (childDataTypeID) {
    case INT64:
    case DOUBLE:
    case BOOL:
    case DATE:
    case TIMESTAMP:
    case INTERVAL: {
        copyFixedSizedValuesToPages(listLiteral, overflowCursor, numBytesOfListElement);
    } break;
    case STRING: {
        copyVarSizedValuesToPages<STRING>(
            resultGFList, listLiteral, overflowCursor, numBytesOfListElement);
    } break;
    case LIST: {
        copyVarSizedValuesToPages<LIST>(
            resultGFList, listLiteral, overflowCursor, numBytesOfListElement);
    } break;
    default: {
        throw LoaderException("Unsupported data type inside LIST.");
    }
    }
    return resultGFList;
}

void InMemOverflowPages::copyStringOverflow(
    PageByteCursor& overflowCursor, uint8_t* srcOverflow, gf_string_t* dstGFString) {
    // Allocate a new page if necessary.
    if (overflowCursor.offset + dstGFString->len >= DEFAULT_PAGE_SIZE || 0 > overflowCursor.idx) {
        overflowCursor.offset = 0;
        overflowCursor.idx = getNewOverflowPageIdx();
    }
    TypeUtils::encodeOverflowPtr(
        dstGFString->overflowPtr, overflowCursor.idx, overflowCursor.offset);
    shared_lock lck(lock);
    pages[overflowCursor.idx]->write(
        overflowCursor.offset, overflowCursor.offset, srcOverflow, dstGFString->len);
    overflowCursor.offset += dstGFString->len;
}

void InMemOverflowPages::copyListOverflow(InMemOverflowPages* srcOverflowPages,
    const PageByteCursor& srcOverflowCursor, PageByteCursor& dstOverflowCursor,
    gf_list_t* dstGFList, DataType* listChildDataType) {
    auto numBytesOfListElement = Types::getDataTypeSize(*listChildDataType);
    // Allocate a new page if necessary.
    if (dstOverflowCursor.offset + (dstGFList->size * numBytesOfListElement) >= DEFAULT_PAGE_SIZE ||
        0 > dstOverflowCursor.idx) {
        dstOverflowCursor.offset = 0;
        dstOverflowCursor.idx = getNewOverflowPageIdx();
    }
    shared_lock lck(lock);
    TypeUtils::encodeOverflowPtr(
        dstGFList->overflowPtr, dstOverflowCursor.idx, dstOverflowCursor.offset);
    auto dataToCopyFrom =
        srcOverflowPages->pages[srcOverflowCursor.idx]->data + srcOverflowCursor.offset;
    auto gfListElements = pages[dstOverflowCursor.idx]->write(dstOverflowCursor.offset,
        dstOverflowCursor.offset, dataToCopyFrom, dstGFList->size * numBytesOfListElement);
    dstOverflowCursor.offset += dstGFList->size * numBytesOfListElement;
    if (listChildDataType->typeID == LIST) {
        auto elementsInList = (gf_list_t*)gfListElements;
        for (auto i = 0u; i < dstGFList->size; i++) {
            PageByteCursor elementCursor;
            TypeUtils::decodeOverflowPtr(
                elementsInList[i].overflowPtr, elementCursor.idx, elementCursor.offset);
            copyListOverflow(srcOverflowPages, elementCursor, dstOverflowCursor, &elementsInList[i],
                listChildDataType->childType.get());
        }
    } else if (listChildDataType->typeID == STRING) {
        auto elementsInList = (gf_string_t*)gfListElements;
        for (auto i = 0u; i < dstGFList->size; i++) {
            if (elementsInList[i].len > gf_string_t::SHORT_STR_LENGTH) {
                PageByteCursor elementCursor;
                TypeUtils::decodeOverflowPtr(
                    elementsInList[i].overflowPtr, elementCursor.idx, elementCursor.offset);
                copyStringOverflow(dstOverflowCursor,
                    srcOverflowPages->pages[elementCursor.idx]->data + elementCursor.offset,
                    &elementsInList[i]);
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
        throw LoaderException("InMemPages: Empty filename");
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

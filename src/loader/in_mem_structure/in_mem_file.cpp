#include "src/loader/include/in_mem_structure/in_mem_file.h"

#include "src/common/include/type_utils.h"
namespace graphflow {
namespace loader {

InMemFile::InMemFile(
    string filePath, uint16_t numBytesForElement, bool hasNullMask, uint64_t numPages)
    : filePath{move(filePath)}, numBytesForElement{numBytesForElement}, numUsedPages{0},
      hasNullMask{hasNullMask} {
    numElementsInAPage = PageUtils::getNumElementsInAPage(numBytesForElement, hasNullMask);
    for (auto i = 0u; i < numPages; i++) {
        addANewPage();
    }
};

uint32_t InMemFile::addANewPage() {
    auto newPageIdx = pages.size();
    pages.push_back(make_unique<InMemPage>(numElementsInAPage, numBytesForElement, hasNullMask));
    return newPageIdx;
}

void InMemFile::flush() {
    if (filePath.empty()) {
        throw LoaderException("InMemPages: Empty filename");
    }
    auto fileInfo = FileUtils::openFile(filePath, O_CREAT | O_WRONLY);
    for (auto pageIdx = 0u; pageIdx < pages.size(); pageIdx++) {
        FileUtils::writeToFile(
            fileInfo.get(), pages[pageIdx]->data, DEFAULT_PAGE_SIZE, pageIdx * DEFAULT_PAGE_SIZE);
    }
    FileUtils::closeFile(fileInfo->fd);
}

gf_string_t InMemOverflowFile::addString(const char* rawString, PageByteCursor& overflowCursor) {
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

void InMemOverflowFile::copyFixedSizedValuesToPages(
    const Literal& listVal, PageByteCursor& overflowCursor, uint64_t numBytesOfListElement) {
    shared_lock lck(lock);
    for (auto& literal : listVal.listVal) {
        pages[overflowCursor.pageIdx]->write(overflowCursor.offsetInPage,
            overflowCursor.offsetInPage, (uint8_t*)&literal.val, numBytesOfListElement);
        overflowCursor.offsetInPage += numBytesOfListElement;
    }
}

template<DataTypeID DT>
void InMemOverflowFile::copyVarSizedValuesToPages(gf_list_t& resultGFList,
    const Literal& listLiteral, PageByteCursor& overflowCursor, uint64_t numBytesOfListElement) {
    assert(DT == STRING || DT == LIST);
    auto overflowPageOffset = overflowCursor.offsetInPage;
    // Reserve space for gf_list or gf_string objects.
    overflowCursor.offsetInPage += (resultGFList.size * numBytesOfListElement);
    if constexpr (DT == STRING) {
        vector<gf_string_t> gfStrings(listLiteral.listVal.size());
        for (auto i = 0u; i < listLiteral.listVal.size(); i++) {
            assert(listLiteral.listVal[i].dataType.typeID == STRING);
            gfStrings[i] = addString(listLiteral.listVal[i].strVal.c_str(), overflowCursor);
        }
        shared_lock lck(lock);
        for (auto i = 0u; i < listLiteral.listVal.size(); i++) {
            pages[overflowCursor.pageIdx]->write(overflowPageOffset + (i * numBytesOfListElement),
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
            pages[overflowCursor.pageIdx]->write(overflowPageOffset + (i * numBytesOfListElement),
                overflowPageOffset + (i * numBytesOfListElement), (uint8_t*)&gfLists[i],
                numBytesOfListElement);
        }
    } else {
        assert(false);
    }
}

gf_list_t InMemOverflowFile::addList(const Literal& listLiteral, PageByteCursor& overflowCursor) {
    assert(listLiteral.dataType.typeID == LIST && !listLiteral.listVal.empty());
    gf_list_t resultGFList;
    auto childDataTypeID = listLiteral.listVal[0].dataType.typeID;
    auto numBytesOfListElement = Types::getDataTypeSize(childDataTypeID);
    resultGFList.size = listLiteral.listVal.size();
    // Allocate a new page if necessary.
    if (overflowCursor.offsetInPage + (resultGFList.size * numBytesOfListElement) >=
            DEFAULT_PAGE_SIZE ||
        overflowCursor.pageIdx == UINT32_MAX) {
        overflowCursor.offsetInPage = 0;
        overflowCursor.pageIdx = getNewOverflowPageIdx();
    }
    TypeUtils::encodeOverflowPtr(
        resultGFList.overflowPtr, overflowCursor.pageIdx, overflowCursor.offsetInPage);
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

void InMemOverflowFile::copyStringOverflow(
    PageByteCursor& overflowCursor, uint8_t* srcOverflow, gf_string_t* dstGFString) {
    // Allocate a new page if necessary.
    if (overflowCursor.offsetInPage + dstGFString->len >= DEFAULT_PAGE_SIZE ||
        overflowCursor.pageIdx == UINT32_MAX) {
        overflowCursor.offsetInPage = 0;
        overflowCursor.pageIdx = getNewOverflowPageIdx();
    }
    TypeUtils::encodeOverflowPtr(
        dstGFString->overflowPtr, overflowCursor.pageIdx, overflowCursor.offsetInPage);
    shared_lock lck(lock);
    pages[overflowCursor.pageIdx]->write(
        overflowCursor.offsetInPage, overflowCursor.offsetInPage, srcOverflow, dstGFString->len);
    overflowCursor.offsetInPage += dstGFString->len;
}

void InMemOverflowFile::copyListOverflow(InMemOverflowFile* srcOverflowPages,
    const PageByteCursor& srcOverflowCursor, PageByteCursor& dstOverflowCursor,
    gf_list_t* dstGFList, DataType* listChildDataType) {
    auto numBytesOfListElement = Types::getDataTypeSize(*listChildDataType);
    // Allocate a new page if necessary.
    if (dstOverflowCursor.offsetInPage + (dstGFList->size * numBytesOfListElement) >=
            DEFAULT_PAGE_SIZE ||
        dstOverflowCursor.pageIdx == UINT32_MAX) {
        dstOverflowCursor.offsetInPage = 0;
        dstOverflowCursor.pageIdx = getNewOverflowPageIdx();
    }
    shared_lock lck(lock);
    TypeUtils::encodeOverflowPtr(
        dstGFList->overflowPtr, dstOverflowCursor.pageIdx, dstOverflowCursor.offsetInPage);
    auto dataToCopyFrom =
        srcOverflowPages->pages[srcOverflowCursor.pageIdx]->data + srcOverflowCursor.offsetInPage;
    auto gfListElements = pages[dstOverflowCursor.pageIdx]->write(dstOverflowCursor.offsetInPage,
        dstOverflowCursor.offsetInPage, dataToCopyFrom, dstGFList->size * numBytesOfListElement);
    dstOverflowCursor.offsetInPage += dstGFList->size * numBytesOfListElement;
    if (listChildDataType->typeID == LIST) {
        auto elementsInList = (gf_list_t*)gfListElements;
        for (auto i = 0u; i < dstGFList->size; i++) {
            PageByteCursor elementCursor;
            TypeUtils::decodeOverflowPtr(
                elementsInList[i].overflowPtr, elementCursor.pageIdx, elementCursor.offsetInPage);
            copyListOverflow(srcOverflowPages, elementCursor, dstOverflowCursor, &elementsInList[i],
                listChildDataType->childType.get());
        }
    } else if (listChildDataType->typeID == STRING) {
        auto elementsInList = (gf_string_t*)gfListElements;
        for (auto i = 0u; i < dstGFList->size; i++) {
            if (elementsInList[i].len > gf_string_t::SHORT_STR_LENGTH) {
                PageByteCursor elementCursor;
                TypeUtils::decodeOverflowPtr(elementsInList[i].overflowPtr, elementCursor.pageIdx,
                    elementCursor.offsetInPage);
                copyStringOverflow(dstOverflowCursor,
                    srcOverflowPages->pages[elementCursor.pageIdx]->data +
                        elementCursor.offsetInPage,
                    &elementsInList[i]);
            }
        }
    }
}

uint32_t InMemOverflowFile::getNewOverflowPageIdx() {
    assert(numUsedPages < UINT32_MAX);
    unique_lock lck(lock);
    auto newPageIdx = pages.size();
    addANewPage();
    return newPageIdx;
}

} // namespace loader
} // namespace graphflow

#include "src/storage/storage_structure/include/in_mem_file.h"

#include "src/common/include/type_utils.h"

namespace graphflow {
namespace storage {

InMemFile::InMemFile(
    string filePath, uint16_t numBytesForElement, bool hasNullMask, uint64_t numPages)
    : filePath{move(filePath)}, numBytesForElement{numBytesForElement}, hasNullMask{hasNullMask} {
    numElementsInAPage = PageUtils::getNumElementsInAPage(numBytesForElement, hasNullMask);
    for (auto i = 0u; i < numPages; i++) {
        addANewPage();
    }
}

void InMemFile::addNewPages(uint64_t numNewPagesToAdd, bool setToZero) {
    for (uint64_t i = 0; i < numNewPagesToAdd; ++i) {
        addANewPage(setToZero);
    }
}

uint32_t InMemFile::addANewPage(bool setToZero) {
    auto newPageIdx = pages.size();
    pages.push_back(make_unique<InMemPage>(numElementsInAPage, numBytesForElement, hasNullMask));
    if (setToZero) {
        memset(pages[newPageIdx]->data, 0, DEFAULT_PAGE_SIZE);
    }
    return newPageIdx;
}

void InMemFile::flush() {
    if (filePath.empty()) {
        throw LoaderException("InMemPages: Empty filename");
    }
    auto fileInfo = FileUtils::openFile(filePath, O_CREAT | O_WRONLY);
    for (auto pageIdx = 0u; pageIdx < pages.size(); pageIdx++) {
        pages[pageIdx]->encodeNullBits();
        FileUtils::writeToFile(
            fileInfo.get(), pages[pageIdx]->data, DEFAULT_PAGE_SIZE, pageIdx * DEFAULT_PAGE_SIZE);
    }
    FileUtils::closeFile(fileInfo->fd);
}

gf_string_t InMemOverflowFile::appendString(const char* rawString) {
    gf_string_t result;
    auto length = strlen(rawString);
    result.len = length;
    if (length <= gf_string_t::SHORT_STR_LENGTH) {
        memcpy(result.prefix, rawString, length);
    } else {
        memcpy(&result.prefix, rawString, gf_string_t::PREFIX_LENGTH);
        unique_lock lck{lock};
        // Allocate a new page if necessary.
        if (nextOffsetInPageToAppend + length >= DEFAULT_PAGE_SIZE) {
            addANewPage();
            nextOffsetInPageToAppend = 0;
            nextPageIdxToAppend++;
        }
        pages[nextPageIdxToAppend]->write(
            nextOffsetInPageToAppend, nextOffsetInPageToAppend, (uint8_t*)rawString, length);
        TypeUtils::encodeOverflowPtr(
            result.overflowPtr, nextPageIdxToAppend, nextOffsetInPageToAppend);
        nextOffsetInPageToAppend += length;
    }
    return result;
}

gf_string_t InMemOverflowFile::copyString(const char* rawString, PageCursor& overflowCursor) {
    gf_string_t gfString;
    gfString.len = strlen(rawString);
    if (gfString.len <= gf_string_t::SHORT_STR_LENGTH) {
        memcpy(gfString.prefix, rawString, gfString.len);
        return gfString;
    } else {
        memcpy(&gfString.prefix, rawString, gf_string_t::PREFIX_LENGTH);
        copyStringOverflow(overflowCursor, (uint8_t*)rawString, &gfString);
    }
    return gfString;
}

void InMemOverflowFile::copyFixedSizedValuesToPages(
    const Literal& listVal, PageCursor& overflowCursor, uint64_t numBytesOfListElement) {
    shared_lock lck(lock);
    for (auto& literal : listVal.listVal) {
        pages[overflowCursor.pageIdx]->write(overflowCursor.posInPage, overflowCursor.posInPage,
            (uint8_t*)&literal.val, numBytesOfListElement);
        overflowCursor.posInPage += numBytesOfListElement;
    }
}

template<DataTypeID DT>
void InMemOverflowFile::copyVarSizedValuesToPages(gf_list_t& resultGFList,
    const Literal& listLiteral, PageCursor& overflowCursor, uint64_t numBytesOfListElement) {
    assert(DT == STRING || DT == LIST);
    auto overflowPageOffset = overflowCursor.posInPage;
    // Reserve space for gf_list or gf_string objects.
    overflowCursor.posInPage += (resultGFList.size * numBytesOfListElement);
    if constexpr (DT == STRING) {
        vector<gf_string_t> gfStrings(listLiteral.listVal.size());
        for (auto i = 0u; i < listLiteral.listVal.size(); i++) {
            assert(listLiteral.listVal[i].dataType.typeID == STRING);
            gfStrings[i] = copyString(listLiteral.listVal[i].strVal.c_str(), overflowCursor);
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
            gfLists[i] = copyList(listLiteral.listVal[i], overflowCursor);
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

gf_list_t InMemOverflowFile::copyList(const Literal& listLiteral, PageCursor& overflowCursor) {
    assert(listLiteral.dataType.typeID == LIST && !listLiteral.listVal.empty());
    gf_list_t resultGFList;
    auto childDataTypeID = listLiteral.listVal[0].dataType.typeID;
    auto numBytesOfListElement = Types::getDataTypeSize(childDataTypeID);
    resultGFList.size = listLiteral.listVal.size();
    // Allocate a new page if necessary.
    if (overflowCursor.posInPage + (resultGFList.size * numBytesOfListElement) >=
            DEFAULT_PAGE_SIZE ||
        overflowCursor.pageIdx == UINT32_MAX) {
        overflowCursor.posInPage = 0;
        overflowCursor.pageIdx = addANewOverflowPage();
    }
    TypeUtils::encodeOverflowPtr(
        resultGFList.overflowPtr, overflowCursor.pageIdx, overflowCursor.posInPage);
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
    PageCursor& overflowCursor, uint8_t* srcOverflow, gf_string_t* dstGFString) {
    // Allocate a new page if necessary.
    if (overflowCursor.posInPage + dstGFString->len >= DEFAULT_PAGE_SIZE ||
        overflowCursor.pageIdx == UINT32_MAX) {
        overflowCursor.posInPage = 0;
        overflowCursor.pageIdx = addANewOverflowPage();
    }
    TypeUtils::encodeOverflowPtr(
        dstGFString->overflowPtr, overflowCursor.pageIdx, overflowCursor.posInPage);
    shared_lock lck(lock);
    pages[overflowCursor.pageIdx]->write(
        overflowCursor.posInPage, overflowCursor.posInPage, srcOverflow, dstGFString->len);
    overflowCursor.posInPage += dstGFString->len;
}

void InMemOverflowFile::copyListOverflow(InMemOverflowFile* srcOverflowPages,
    const PageCursor& srcOverflowCursor, PageCursor& dstOverflowCursor, gf_list_t* dstGFList,
    DataType* listChildDataType) {
    auto numBytesOfListElement = Types::getDataTypeSize(*listChildDataType);
    // Allocate a new page if necessary.
    if (dstOverflowCursor.posInPage + (dstGFList->size * numBytesOfListElement) >=
            DEFAULT_PAGE_SIZE ||
        dstOverflowCursor.pageIdx == UINT32_MAX) {
        dstOverflowCursor.posInPage = 0;
        dstOverflowCursor.pageIdx = addANewOverflowPage();
    }
    shared_lock lck(lock);
    TypeUtils::encodeOverflowPtr(
        dstGFList->overflowPtr, dstOverflowCursor.pageIdx, dstOverflowCursor.posInPage);
    auto dataToCopyFrom =
        srcOverflowPages->pages[srcOverflowCursor.pageIdx]->data + srcOverflowCursor.posInPage;
    auto gfListElements = pages[dstOverflowCursor.pageIdx]->write(dstOverflowCursor.posInPage,
        dstOverflowCursor.posInPage, dataToCopyFrom, dstGFList->size * numBytesOfListElement);
    dstOverflowCursor.posInPage += dstGFList->size * numBytesOfListElement;
    if (listChildDataType->typeID == LIST) {
        auto elementsInList = (gf_list_t*)gfListElements;
        for (auto i = 0u; i < dstGFList->size; i++) {
            PageCursor elementCursor;
            TypeUtils::decodeOverflowPtr(
                elementsInList[i].overflowPtr, elementCursor.pageIdx, elementCursor.posInPage);
            copyListOverflow(srcOverflowPages, elementCursor, dstOverflowCursor, &elementsInList[i],
                listChildDataType->childType.get());
        }
    } else if (listChildDataType->typeID == STRING) {
        auto elementsInList = (gf_string_t*)gfListElements;
        for (auto i = 0u; i < dstGFList->size; i++) {
            if (elementsInList[i].len > gf_string_t::SHORT_STR_LENGTH) {
                PageCursor elementCursor;
                TypeUtils::decodeOverflowPtr(
                    elementsInList[i].overflowPtr, elementCursor.pageIdx, elementCursor.posInPage);
                copyStringOverflow(dstOverflowCursor,
                    srcOverflowPages->pages[elementCursor.pageIdx]->data + elementCursor.posInPage,
                    &elementsInList[i]);
            }
        }
    }
}

uint32_t InMemOverflowFile::addANewOverflowPage() {
    unique_lock lck(lock);
    auto newPageIdx = pages.size();
    addANewPage();
    return newPageIdx;
}

} // namespace storage
} // namespace graphflow

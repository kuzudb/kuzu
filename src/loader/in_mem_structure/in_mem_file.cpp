#include "src/loader/include/in_mem_structure/in_mem_file.h"

#include "src/common/include/type_utils.h"
namespace graphflow {
namespace loader {

InMemFile::InMemFile(
    string filePath, uint16_t numBytesForElement, bool hasNullMask, uint64_t numPages)
    : filePath{move(filePath)}, numUsedPages{0}, hasNullMask{hasNullMask} {
    numElementsInAPage = hasNullMask ?
                             PageUtils::getNumElementsInAPageWithNULLBytes(numBytesForElement) :
                             PageUtils::getNumElementsInAPageWithoutNULLBytes(numBytesForElement);
    for (auto i = 0u; i < numPages; i++) {
        addANewPage();
    }
};

uint32_t InMemFile::addANewPage() {
    auto newPageIdx = pages.size();
    pages.push_back(make_unique<InMemPage>(numElementsInAPage, hasNullMask));
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
        pages[overflowCursor.idx]->write(overflowCursor.offset, overflowCursor.offset,
            (uint8_t*)&literal.val, numBytesOfListElement);
        overflowCursor.offset += numBytesOfListElement;
    }
}

template<DataTypeID DT>
void InMemOverflowFile::copyVarSizedValuesToPages(gf_list_t& resultGFList,
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

gf_list_t InMemOverflowFile::addList(const Literal& listLiteral, PageByteCursor& overflowCursor) {
    assert(listLiteral.dataType.typeID == LIST && !listLiteral.listVal.empty());
    gf_list_t resultGFList;
    auto childDataTypeID = listLiteral.listVal[0].dataType.typeID;
    auto numBytesOfListElement = Types::getDataTypeSize(childDataTypeID);
    resultGFList.size = listLiteral.listVal.size();
    // Allocate a new page if necessary.
    if (overflowCursor.offset + (resultGFList.size * numBytesOfListElement) >= DEFAULT_PAGE_SIZE ||
        overflowCursor.idx == UINT64_MAX) {
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

void InMemOverflowFile::copyStringOverflow(
    PageByteCursor& overflowCursor, uint8_t* srcOverflow, gf_string_t* dstGFString) {
    // Allocate a new page if necessary.
    if (overflowCursor.offset + dstGFString->len >= DEFAULT_PAGE_SIZE ||
        overflowCursor.idx == UINT64_MAX) {
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

void InMemOverflowFile::copyListOverflow(InMemOverflowFile* srcOverflowPages,
    const PageByteCursor& srcOverflowCursor, PageByteCursor& dstOverflowCursor,
    gf_list_t* dstGFList, DataType* listChildDataType) {
    auto numBytesOfListElement = Types::getDataTypeSize(*listChildDataType);
    // Allocate a new page if necessary.
    if (dstOverflowCursor.offset + (dstGFList->size * numBytesOfListElement) >= DEFAULT_PAGE_SIZE ||
        dstOverflowCursor.idx == UINT64_MAX) {
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

uint32_t InMemOverflowFile::getNewOverflowPageIdx() {
    assert(numUsedPages < UINT32_MAX);
    unique_lock lck(lock);
    auto newPageIdx = pages.size();
    addANewPage();
    return newPageIdx;
}

} // namespace loader
} // namespace graphflow

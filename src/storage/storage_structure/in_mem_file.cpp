#include "storage/storage_structure/in_mem_file.h"

#include <mutex>

#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/type_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

InMemFile::InMemFile(
    std::string filePath, uint16_t numBytesForElement, bool hasNullMask, uint64_t numPages)
    : filePath{std::move(filePath)}, numBytesForElement{numBytesForElement}, hasNullMask{
                                                                                 hasNullMask} {
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
    pages.push_back(
        std::make_unique<InMemPage>(numElementsInAPage, numBytesForElement, hasNullMask));
    if (setToZero) {
        memset(pages[newPageIdx]->data, 0, BufferPoolConstants::PAGE_4KB_SIZE);
    }
    return newPageIdx;
}

void InMemFile::flush() {
    if (filePath.empty()) {
        throw CopyException("InMemPages: Empty filename");
    }
    auto fileInfo = FileUtils::openFile(filePath, O_CREAT | O_WRONLY);
    for (auto pageIdx = 0u; pageIdx < pages.size(); pageIdx++) {
        pages[pageIdx]->encodeNullBits();
        FileUtils::writeToFile(fileInfo.get(), pages[pageIdx]->data,
            BufferPoolConstants::PAGE_4KB_SIZE, pageIdx * BufferPoolConstants::PAGE_4KB_SIZE);
    }
}

ku_string_t InMemOverflowFile::appendString(const char* rawString) {
    ku_string_t result;
    auto length = strlen(rawString);
    result.len = length;
    std::memcpy(result.prefix, rawString,
        length <= ku_string_t::SHORT_STR_LENGTH ? length : ku_string_t::PREFIX_LENGTH);
    if (length > ku_string_t::SHORT_STR_LENGTH) {
        if (length > BufferPoolConstants::PAGE_4KB_SIZE) {
            throw CopyException(ExceptionMessage::overLargeStringPKValueException(length));
        }
        std::unique_lock lck{lock};
        // Allocate a new page if necessary.
        if (nextOffsetInPageToAppend + length >= BufferPoolConstants::PAGE_4KB_SIZE) {
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

void InMemOverflowFile::copyStringOverflow(
    PageByteCursor& dstOverflowCursor, uint8_t* srcOverflow, ku_string_t* dstKUString) {
    // Allocate a new page if necessary.
    if (dstOverflowCursor.offsetInPage + dstKUString->len >= BufferPoolConstants::PAGE_4KB_SIZE ||
        dstOverflowCursor.pageIdx == UINT32_MAX) {
        dstOverflowCursor.offsetInPage = 0;
        dstOverflowCursor.pageIdx = addANewOverflowPage();
    }
    TypeUtils::encodeOverflowPtr(
        dstKUString->overflowPtr, dstOverflowCursor.pageIdx, dstOverflowCursor.offsetInPage);
    std::shared_lock lck(lock);
    pages[dstOverflowCursor.pageIdx]->write(dstOverflowCursor.offsetInPage,
        dstOverflowCursor.offsetInPage, srcOverflow, dstKUString->len);
    dstOverflowCursor.offsetInPage += dstKUString->len;
}

void InMemOverflowFile::copyListOverflowFromFile(InMemOverflowFile* srcInMemOverflowFile,
    const PageByteCursor& srcOverflowCursor, PageByteCursor& dstOverflowCursor,
    ku_list_t* dstKUList, LogicalType* listChildDataType) {
    auto numBytesOfListElement = StorageUtils::getDataTypeSize(*listChildDataType);
    // Allocate a new page if necessary.
    if (dstOverflowCursor.offsetInPage + (dstKUList->size * numBytesOfListElement) >=
            BufferPoolConstants::PAGE_4KB_SIZE ||
        dstOverflowCursor.pageIdx == UINT32_MAX) {
        dstOverflowCursor.offsetInPage = 0;
        dstOverflowCursor.pageIdx = addANewOverflowPage();
    }
    TypeUtils::encodeOverflowPtr(
        dstKUList->overflowPtr, dstOverflowCursor.pageIdx, dstOverflowCursor.offsetInPage);
    auto dataToCopyFrom = srcInMemOverflowFile->pages[srcOverflowCursor.pageIdx]->data +
                          srcOverflowCursor.offsetInPage;
    auto offsetToCopyInto = dstOverflowCursor.offsetInPage;
    dstOverflowCursor.offsetInPage += dstKUList->size * numBytesOfListElement;
    if (listChildDataType->getLogicalTypeID() == LogicalTypeID::VAR_LIST) {
        auto elementsInList = (ku_list_t*)dataToCopyFrom;
        for (auto i = 0u; i < dstKUList->size; i++) {
            PageByteCursor elementCursor;
            TypeUtils::decodeOverflowPtr(
                elementsInList[i].overflowPtr, elementCursor.pageIdx, elementCursor.offsetInPage);
            copyListOverflowFromFile(srcInMemOverflowFile, elementCursor, dstOverflowCursor,
                &elementsInList[i], VarListType::getChildType(listChildDataType));
        }
    } else if (listChildDataType->getLogicalTypeID() == LogicalTypeID::STRING) {
        auto elementsInList = (ku_string_t*)dataToCopyFrom;
        for (auto i = 0u; i < dstKUList->size; i++) {
            if (elementsInList[i].len > ku_string_t::SHORT_STR_LENGTH) {
                PageByteCursor elementCursor;
                TypeUtils::decodeOverflowPtr(elementsInList[i].overflowPtr, elementCursor.pageIdx,
                    elementCursor.offsetInPage);
                copyStringOverflow(dstOverflowCursor,
                    srcInMemOverflowFile->pages[elementCursor.pageIdx]->data +
                        elementCursor.offsetInPage,
                    &elementsInList[i]);
            }
        }
    }
    std::shared_lock lck(lock);
    pages[dstOverflowCursor.pageIdx]->write(offsetToCopyInto, offsetToCopyInto, dataToCopyFrom,
        dstKUList->size * numBytesOfListElement);
}

void InMemOverflowFile::copyListOverflowToFile(
    PageByteCursor& pageByteCursor, ku_list_t* srcKUList, LogicalType* childDataType) {
    auto numBytesOfListElement = storage::StorageUtils::getDataTypeSize(*childDataType);
    // Allocate a new page if necessary.
    if (pageByteCursor.offsetInPage + (srcKUList->size * numBytesOfListElement) >=
            BufferPoolConstants::PAGE_4KB_SIZE ||
        pageByteCursor.pageIdx == UINT32_MAX) {
        pageByteCursor.offsetInPage = 0;
        pageByteCursor.pageIdx = addANewOverflowPage();
    }
    lock.lock();
    // Copy the overflow data of the srcKUList to inMemOverflowFile.
    pages[pageByteCursor.pageIdx]->write(pageByteCursor.offsetInPage, pageByteCursor.offsetInPage,
        reinterpret_cast<uint8_t*>(srcKUList->overflowPtr),
        srcKUList->size * numBytesOfListElement);
    auto inMemOverflowFileData = pages[pageByteCursor.pageIdx]->data + pageByteCursor.offsetInPage;
    lock.unlock();
    // Reset the overflowPtr of the srcKUList, so that it points to the inMemOverflowFile.
    TypeUtils::encodeOverflowPtr(
        srcKUList->overflowPtr, pageByteCursor.pageIdx, pageByteCursor.offsetInPage);
    pageByteCursor.offsetInPage += srcKUList->size * numBytesOfListElement;
    // If the element of the list also requires resetting overflowPtr, then do it recursively.
    resetElementsOverflowPtrIfNecessary(
        pageByteCursor, childDataType, srcKUList->size, inMemOverflowFileData);
}

page_idx_t InMemOverflowFile::addANewOverflowPage() {
    std::unique_lock lck(lock);
    auto newPageIdx = pages.size();
    addANewPage();
    return newPageIdx;
}

std::string InMemOverflowFile::readString(ku_string_t* strInInMemOvfFile) {
    if (ku_string_t::isShortString(strInInMemOvfFile->len)) {
        return strInInMemOvfFile->getAsShortString();
    } else {
        page_idx_t pageIdx = UINT32_MAX;
        uint16_t pagePos = UINT16_MAX;
        TypeUtils::decodeOverflowPtr(strInInMemOvfFile->overflowPtr, pageIdx, pagePos);
        std::shared_lock sLck{lock};
        return {
            reinterpret_cast<const char*>(pages[pageIdx]->data + pagePos), strInInMemOvfFile->len};
    }
}

void InMemOverflowFile::resetElementsOverflowPtrIfNecessary(PageByteCursor& pageByteCursor,
    LogicalType* elementType, uint64_t numElementsToReset, uint8_t* elementsToReset) {
    if (elementType->getLogicalTypeID() == LogicalTypeID::VAR_LIST) {
        auto kuListPtr = reinterpret_cast<ku_list_t*>(elementsToReset);
        for (auto i = 0u; i < numElementsToReset; i++) {
            copyListOverflowToFile(
                pageByteCursor, kuListPtr, VarListType::getChildType(elementType));
            kuListPtr++;
        }
    } else if (elementType->getLogicalTypeID() == LogicalTypeID::STRING) {
        auto kuStrPtr = reinterpret_cast<ku_string_t*>(elementsToReset);
        for (auto i = 0u; i < numElementsToReset; i++) {
            if (kuStrPtr->len > ku_string_t::SHORT_STR_LENGTH) {
                copyStringOverflow(
                    pageByteCursor, reinterpret_cast<uint8_t*>(kuStrPtr->overflowPtr), kuStrPtr);
            }
            kuStrPtr++;
        }
    }
}

} // namespace storage
} // namespace kuzu

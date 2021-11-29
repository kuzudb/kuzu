#include "src/loader/include/in_mem_structure/in_mem_pages.h"

#include <fcntl.h>

#include "src/common/include/file_utils.h"

namespace graphflow {
namespace loader {

InMemPages::InMemPages(
    string fName, uint16_t numBytesForElement, bool hasNULLBytes, uint64_t numPages)
    : fName(move(fName)), numBytesForElement{numBytesForElement} {
    auto numElementsInAPage =
        hasNULLBytes ? PageUtils::getNumElementsInAPageWithNULLBytes(numBytesForElement) :
                       PageUtils::getNumElementsInAPageWithoutNULLBytes(numBytesForElement);
    data = make_unique<uint8_t[]>(numPages << PAGE_SIZE_LOG_2);
    pages.resize(numPages);
    for (auto i = 0u; i < numPages; i++) {
        pages[i] = make_unique<InMemPage>(
            numElementsInAPage, data.get() + (i << PAGE_SIZE_LOG_2), hasNULLBytes);
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
    uint64_t byteSize = pages.size() << PAGE_SIZE_LOG_2;
    FileUtils::writeToFile(fileInfo.get(), data.get(), byteSize, 0);
    FileUtils::closeFile(fileInfo->fd);
}

void InMemAdjPages::set(const PageElementCursor& cursor, const nodeID_t& nbrNodeID) {
    pages[cursor.idx]->write(cursor.pos * (numBytesPerLabel + numBytesPerOffset), cursor.pos,
        (uint8_t*)&nbrNodeID.label, numBytesPerLabel, (uint8_t*)&nbrNodeID.offset,
        numBytesPerOffset);
}

void InMemPropertyPages::set(const PageElementCursor& cursor, const uint8_t* val) {
    pages[cursor.idx]->write(cursor.pos * numBytesPerElement, cursor.pos, val, numBytesPerElement);
}

void InMemUnstrPropertyPages::set(const PageByteCursor& cursor, uint32_t propertyKey,
    uint8_t dataTypeIdentifier, uint32_t valLen, const uint8_t* val) {
    PageByteCursor localCursor{cursor};
    setComponentOfUnstrProperty(localCursor, 4, reinterpret_cast<uint8_t*>(&propertyKey));
    setComponentOfUnstrProperty(localCursor, 1, reinterpret_cast<uint8_t*>(&dataTypeIdentifier));
    setComponentOfUnstrProperty(localCursor, valLen, val);
}

void InMemUnstrPropertyPages::setComponentOfUnstrProperty(
    PageByteCursor& localCursor, uint8_t len, const uint8_t* val) {
    if (PAGE_SIZE - localCursor.offset >= len) {
        auto writeOffset = getPtrToMemLoc(localCursor);
        memcpy(writeOffset, val, len);
        localCursor.offset += len;
    } else {
        auto diff = PAGE_SIZE - localCursor.offset;
        auto writeOffset = getPtrToMemLoc(localCursor);
        memcpy(writeOffset, val, diff);
        auto left = len - diff;
        localCursor.idx++;
        localCursor.offset = 0;
        writeOffset = getPtrToMemLoc(localCursor);
        memcpy(writeOffset, val + diff, left);
        localCursor.offset = left;
    }
}

void InMemStringOverflowPages::setStrInOvfPageAndPtrInEncString(
    const char* originalString, PageByteCursor& cursor, gf_string_t* encodedString) {
    encodedString->len = strlen(originalString);
    memcpy(&encodedString->prefix, originalString, min(encodedString->len, 4u));
    if (encodedString->len <= 4) {
        return;
    }
    if (encodedString->len > 4 && encodedString->len <= 12) {
        memcpy(&encodedString->data, originalString + 4, encodedString->len - 4);
        return;
    }
    copyOverflowString(cursor, (uint8_t*)originalString, encodedString);
}

void InMemStringOverflowPages::copyOverflowString(
    PageByteCursor& cursor, uint8_t* ptrToCopy, gf_string_t* encodedString) {
    if (cursor.offset + encodedString->len - 4 >= PAGE_SIZE || 0 > cursor.idx) {
        cursor.offset = 0;
        cursor.idx = getNewOverflowPageIdx();
    }
    shared_lock lck(lock);
    encodedString->setOverflowPtrInfo(cursor.idx, cursor.offset);
    pages[cursor.idx]->write(cursor.offset, cursor.offset, ptrToCopy, encodedString->len);
    cursor.offset += encodedString->len;
}

uint32_t InMemStringOverflowPages::getNewOverflowPageIdx() {
    unique_lock lck(lock);
    auto page = numUsedPages++;
    if (numUsedPages != pages.size()) {
        return page;
    }
    auto oldNumPages = pages.size();
    auto newNumPages = (size_t)(1.5 * pages.size());
    auto deltaPages = newNumPages - oldNumPages;
    auto newData = new uint8_t[deltaPages << PAGE_SIZE_LOG_2];
    additionalDataBlocks.emplace_back(newData);
    numPagesPerDataBlock.emplace_back(deltaPages);
    pages.resize(newNumPages);
    auto numElementsInPage = PageUtils::getNumElementsInAPageWithoutNULLBytes(1);
    for (auto i = 0; i < deltaPages; i++) {
        pages[oldNumPages + i] = make_unique<InMemPage>(
            numElementsInPage, newData + (i << PAGE_SIZE_LOG_2), false /*hasNULLBytes*/);
    }
    return page;
}

void InMemStringOverflowPages::saveToFile() {
    if (0 == fName.length()) {
        throw invalid_argument("InMemPages: Empty filename");
    }
    for (auto& page : pages) {
        page->encodeNULLBytes();
    }
    auto fileInfo = FileUtils::openFile(fName, O_WRONLY | O_CREAT);
    auto blockId = 0u;
    auto totalWrittenPages = 0u;
    while (numUsedPages) {
        auto numPagesToWrite = min(numUsedPages, numPagesPerDataBlock[blockId]);
        uint64_t sizeToWrite = numPagesToWrite << PAGE_SIZE_LOG_2;
        if (blockId == 0) {
            // write the primary block
            FileUtils::writeToFile(fileInfo.get(), data.get(), sizeToWrite, 0);
        } else {
            FileUtils::writeToFile(fileInfo.get(), additionalDataBlocks[blockId - 1].get(),
                sizeToWrite, totalWrittenPages << PAGE_SIZE_LOG_2);
        }
        numUsedPages -= numPagesToWrite;
        totalWrittenPages += numPagesToWrite;
        blockId++;
    }
    FileUtils::closeFile(fileInfo->fd);
}

} // namespace loader
} // namespace graphflow

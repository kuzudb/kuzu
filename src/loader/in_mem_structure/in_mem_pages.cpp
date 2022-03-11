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
    if (DEFAULT_PAGE_SIZE - localCursor.offset >= len) {
        auto writeOffset = getPtrToMemLoc(localCursor);
        memcpy(writeOffset, val, len);
        localCursor.offset += len;
    } else {
        auto diff = DEFAULT_PAGE_SIZE - localCursor.offset;
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
    copyOverflowString(cursor, (uint8_t*)originalString, &encodedString);
    return encodedString;
}

gf_list_t InMemOverflowPages::addList(const Literal& listVal, PageByteCursor& cursor) {
    assert(!listVal.listVal.empty());
    gf_list_t result;
    result.childType = listVal.listVal[0].dataType;
    auto numBytesOfSingleValue = TypeUtils::getDataTypeSize(result.childType);
    result.size = listVal.listVal.size();
    result.capacity = result.size;
    if (cursor.offset + (result.size * numBytesOfSingleValue) >= DEFAULT_PAGE_SIZE ||
        0 > cursor.idx) {
        cursor.offset = 0;
        cursor.idx = getNewOverflowPageIdx();
    }
    TypeUtils::encodeOverflowPtr(result.overflowPtr, cursor.idx, cursor.offset);
    shared_lock lck(lock);
    switch (result.childType) {
    case INT64:
    case DOUBLE:
    case BOOL:
    case DATE:
    case TIMESTAMP:
    case INTERVAL: {
        for (auto& literal : listVal.listVal) {
            assert(literal.dataType == result.childType);
            pages[cursor.idx]->write(
                cursor.offset, cursor.offset, (uint8_t*)&literal.val, numBytesOfSingleValue);
            cursor.offset += numBytesOfSingleValue;
        }
    } break;
    case STRING: {
        auto gfStrOverflowPageId = cursor.idx;
        auto gfStrOverflowPageOffset = cursor.offset;
        cursor.offset += (result.size * numBytesOfSingleValue); // Skip gf_strings.
        vector<gf_string_t> gfStrings;
        // Add original strings.
        for (auto& literal : listVal.listVal) {
            assert(literal.dataType == STRING);
            gfStrings.push_back(addString(literal.strVal.c_str(), cursor));
        }
        // Write gf_strings.
        for (auto i = 0u; i < listVal.listVal.size(); i++) {
            pages[gfStrOverflowPageId]->write(gfStrOverflowPageOffset + (i * numBytesOfSingleValue),
                gfStrOverflowPageOffset + (i * numBytesOfSingleValue), (uint8_t*)&gfStrings[i],
                numBytesOfSingleValue);
        }
    } break;
    default: {
        throw invalid_argument("Unsupported data type inside LIST.");
    }
    }
    return result;
}

void InMemOverflowPages::copyOverflowString(
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

uint32_t InMemOverflowPages::getNewOverflowPageIdx() {
    unique_lock lck(lock);
    auto page = numUsedPages++;
    if (numUsedPages != pages.size()) {
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

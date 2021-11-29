#include "src/loader/include/in_mem_structure/in_mem_pages.h"

#include <fcntl.h>

#include <cstring>

#include "src/common/include/file_utils.h"

namespace graphflow {
namespace loader {

void InMemPages::saveToFile() {
    if (0 == fName.length()) {
        throw invalid_argument("InMemPages: Empty filename");
    }
    auto fileInfo = FileUtils::openFile(fName, O_WRONLY | O_CREAT);
    uint64_t size = numPages << PAGE_SIZE_LOG_2;
    FileUtils::writeToFile(fileInfo.get(), data.get(), size, 0);
    FileUtils::closeFile(fileInfo->fd);
}

void InMemAdjPages::setNbrNode(const PageCursor& cursor, const nodeID_t& nbrNodeID) {
    auto writeOffset = data.get() + (cursor.idx << PAGE_SIZE_LOG_2) + cursor.offset;
    memcpy(writeOffset, &nbrNodeID.label, numBytesPerLabel);
    memcpy(writeOffset + numBytesPerLabel, &nbrNodeID.offset, numBytesPerOffset);
}

void InMemUnstrPropertyPages::setUnstrProperty(const PageCursor& cursor, uint32_t propertyKey,
    uint8_t dataTypeIdentifier, uint32_t valLen, const uint8_t* val) {
    auto writeOffset = getPtrToMemLoc(cursor);
    memcpy(writeOffset, &propertyKey, 4);
    memcpy(writeOffset + 4, &dataTypeIdentifier, 1);
    memcpy(writeOffset + 5, val, valLen);
}

void InMemStringOverflowPages::setStrInOvfPageAndPtrInEncString(
    const char* originalString, PageCursor& cursor, gf_string_t* encodedString) {
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
    PageCursor& cursor, uint8_t* ptrToCopy, gf_string_t* encodedString) {
    if (cursor.offset + encodedString->len - 4 >= PAGE_SIZE || 0 > cursor.idx) {
        cursor.offset = 0;
        cursor.idx = getNewOverflowPageIdx();
    }
    shared_lock lck(lock);
    encodedString->setOverflowPtrInfo(cursor.idx, cursor.offset);
    auto writeOffset = data.get() + (cursor.idx << PAGE_SIZE_LOG_2) + cursor.offset;
    memcpy(writeOffset, ptrToCopy, encodedString->len);
    cursor.offset += encodedString->len;
}

uint32_t InMemStringOverflowPages::getNewOverflowPageIdx() {
    unique_lock lck(lock);
    auto page = numPages++;
    if (numPages != maxPages) {
        return page;
    }
    uint64_t newMaxPages = 1.5 * maxPages;
    auto newPages = new uint8_t[newMaxPages << PAGE_SIZE_LOG_2];
    memcpy(newPages, data.get(), maxPages << PAGE_SIZE_LOG_2);
    data.reset(newPages);
    maxPages = newMaxPages;
    return page;
}

} // namespace loader
} // namespace graphflow

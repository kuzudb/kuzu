#include "storage/overflow_file.h"

#include <memory>

#include "common/type_utils.h"
#include "common/types/types.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/file_handle.h"
#include "storage/shadow_utils.h"
#include "storage/storage_utils.h"
#include "transaction/transaction.h"

using namespace kuzu::transaction;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

std::string OverflowFileHandle::readString(TransactionType trxType, const ku_string_t& str) const {
    if (ku_string_t::isShortString(str.len)) {
        return str.getAsShortString();
    }
    PageCursor cursor;
    TypeUtils::decodeOverflowPtr(str.overflowPtr, cursor.pageIdx, cursor.elemPosInPage);
    std::string retVal;
    retVal.reserve(str.len);
    int32_t remainingLength = str.len;
    while (remainingLength > 0) {
        auto numBytesToReadInPage =
            std::min(static_cast<uint32_t>(remainingLength), END_OF_PAGE - cursor.elemPosInPage);
        auto startPosInSrc = retVal.size();
        read(trxType, cursor.pageIdx, [&](uint8_t* frame) {
            // Replace rather than append, since optimistic read may call the function multiple
            // times
            retVal.replace(startPosInSrc, numBytesToReadInPage,
                std::string_view(reinterpret_cast<const char*>(frame) + cursor.elemPosInPage,
                    numBytesToReadInPage));
            cursor.pageIdx = *reinterpret_cast<page_idx_t*>(frame + END_OF_PAGE);
        });
        remainingLength -= numBytesToReadInPage;
        // After the first page we always start reading from the beginning of the page.
        cursor.elemPosInPage = 0;
    }
    return retVal;
}

bool OverflowFileHandle::equals(TransactionType trxType, std::string_view keyToLookup,
    const ku_string_t& keyInEntry) const {
    PageCursor cursor;
    TypeUtils::decodeOverflowPtr(keyInEntry.overflowPtr, cursor.pageIdx, cursor.elemPosInPage);
    auto lengthRead = 0u;
    while (lengthRead < keyInEntry.len) {
        auto numBytesToCheckInPage = std::min(static_cast<page_idx_t>(keyInEntry.len) - lengthRead,
            END_OF_PAGE - cursor.elemPosInPage);
        bool equal = true;
        read(trxType, cursor.pageIdx, [&](auto* frame) {
            equal = memcmp(keyToLookup.data() + lengthRead, frame + cursor.elemPosInPage,
                        numBytesToCheckInPage) == 0;
            // Update the next page index
            cursor.pageIdx = *reinterpret_cast<page_idx_t*>(frame + END_OF_PAGE);
        });
        if (!equal) {
            return false;
        }
        cursor.elemPosInPage = 0;
        lengthRead += numBytesToCheckInPage;
    }
    return true;
}

uint8_t* OverflowFileHandle::addANewPage() {
    page_idx_t newPageIdx = overflowFile.getNewPageIdx();
    if (pageWriteCache.size() > 0) {
        memcpy(pageWriteCache[nextPosToWriteTo.pageIdx]->getData() + END_OF_PAGE, &newPageIdx,
            sizeof(page_idx_t));
    }
    if (startPageIdx == INVALID_PAGE_IDX) {
        startPageIdx = newPageIdx;
    }
    pageWriteCache.emplace(newPageIdx,
        overflowFile.memoryManager.allocateBuffer(true /*initializeToZero*/, KUZU_PAGE_SIZE));
    nextPosToWriteTo.elemPosInPage = 0;
    nextPosToWriteTo.pageIdx = newPageIdx;
    return pageWriteCache[newPageIdx]->getData();
}

void OverflowFileHandle::setStringOverflow(const char* srcRawString, uint64_t len,
    ku_string_t& diskDstString) {
    if (len <= ku_string_t::SHORT_STR_LENGTH) {
        return;
    }
    overflowFile.headerChanged = true;
    uint8_t* pageToWrite = nullptr;
    if (nextPosToWriteTo.pageIdx == INVALID_PAGE_IDX) {
        pageToWrite = addANewPage();
    } else {
        auto cached = pageWriteCache.find(nextPosToWriteTo.pageIdx);
        if (cached != pageWriteCache.end()) {
            pageToWrite = cached->second->getData();
        } else {
            overflowFile.readFromDisk(TransactionType::CHECKPOINT, nextPosToWriteTo.pageIdx,
                [&](auto* frame) {
                    auto page = overflowFile.memoryManager.allocateBuffer(
                        false /*initializeToZero*/, KUZU_PAGE_SIZE);
                    memcpy(page->getData(), frame, KUZU_PAGE_SIZE);
                    pageToWrite = page->getData();
                    pageWriteCache.emplace(nextPosToWriteTo.pageIdx, std::move(page));
                });
        }
    }
    int32_t remainingLength = len;
    TypeUtils::encodeOverflowPtr(diskDstString.overflowPtr, nextPosToWriteTo.pageIdx,
        nextPosToWriteTo.elemPosInPage);
    while (remainingLength > 0) {
        auto bytesWritten = len - remainingLength;
        auto numBytesToWriteInPage = std::min(static_cast<uint32_t>(remainingLength),
            END_OF_PAGE - nextPosToWriteTo.elemPosInPage);
        memcpy(pageToWrite + nextPosToWriteTo.elemPosInPage, srcRawString + bytesWritten,
            numBytesToWriteInPage);
        remainingLength -= numBytesToWriteInPage;
        nextPosToWriteTo.elemPosInPage += numBytesToWriteInPage;
        if (nextPosToWriteTo.elemPosInPage >= END_OF_PAGE) {
            pageToWrite = addANewPage();
        }
    }
}

ku_string_t OverflowFileHandle::writeString(std::string_view rawString) {
    ku_string_t result;
    result.len = rawString.length();
    auto shortStrLen = ku_string_t::SHORT_STR_LENGTH;
    auto inlineLen = std::min(shortStrLen, static_cast<uint64_t>(result.len));
    memcpy(result.prefix, rawString.data(), inlineLen);
    setStringOverflow(rawString.data(), rawString.length(), result);
    return result;
}

void OverflowFileHandle::checkpoint() {
    for (auto& [pageIndex, page] : pageWriteCache) {
        overflowFile.writePageToDisk(pageIndex, page->getData());
    }
}

void OverflowFileHandle::reclaimStorage(PageManager& pageManager) {
    if (startPageIdx == INVALID_PAGE_IDX) {
        return;
    }

    auto pageIdx = startPageIdx;
    while (true) {
        if (pageIdx == 0 || pageIdx == INVALID_PAGE_IDX) [[unlikely]] {
            throw RuntimeException(
                "The overflow file has been corrupted, this should never happen.");
        }
        pageManager.freePage(pageIdx);

        if (pageIdx == nextPosToWriteTo.pageIdx) {
            break;
        }

        // reclaimStorage() is only called after the hash index is checkpointed
        // so the page write cache should always be cleared
        KU_ASSERT(!pageWriteCache.contains(pageIdx));
        overflowFile.readFromDisk(TransactionType::CHECKPOINT, pageIdx, [&pageIdx](auto* frame) {
            pageIdx = *reinterpret_cast<page_idx_t*>(frame + END_OF_PAGE);
        });
    }
}

void OverflowFileHandle::read(TransactionType trxType, page_idx_t pageIdx,
    const std::function<void(uint8_t*)>& func) const {
    auto cachedPage = pageWriteCache.find(pageIdx);
    if (cachedPage != pageWriteCache.end()) {
        return func(cachedPage->second->getData());
    }
    overflowFile.readFromDisk(trxType, pageIdx, func);
}

OverflowFile::OverflowFile(FileHandle* dataFH, MemoryManager& memoryManager, ShadowFile* shadowFile,
    page_idx_t headerPageIdx)
    : numPagesOnDisk{0}, fileHandle{dataFH}, shadowFile{shadowFile}, memoryManager{memoryManager},
      headerChanged{false}, headerPageIdx{headerPageIdx} {
    KU_ASSERT(shadowFile);
    if (headerPageIdx != INVALID_PAGE_IDX) {
        readFromDisk(TransactionType::READ_ONLY, headerPageIdx,
            [&](auto* frame) { memcpy(&header, frame, sizeof(header)); });
    } else {
        // Reserve a page for the header
        this->headerPageIdx = getNewPageIdx();
        header = StringOverflowFileHeader();
    }
}

OverflowFile::OverflowFile(FileHandle* dataFH, MemoryManager& memoryManager)
    : numPagesOnDisk{0}, fileHandle{dataFH}, shadowFile{nullptr}, memoryManager{memoryManager},
      headerChanged{false}, headerPageIdx{INVALID_PAGE_IDX} {
    if (fileHandle) {
        numPagesOnDisk = fileHandle->getNumPages();
    }
    // Reserve a page for the header
    this->headerPageIdx = getNewPageIdx();
    header = StringOverflowFileHeader();
}

void OverflowFile::readFromDisk(TransactionType trxType, page_idx_t pageIdx,
    const std::function<void(uint8_t*)>& func) const {
    KU_ASSERT(shadowFile);
    auto [fileHandleToPin, pageIdxToPin] = ShadowUtils::getFileHandleAndPhysicalPageIdxToPin(
        *getFileHandle(), pageIdx, *shadowFile, trxType);
    fileHandleToPin->optimisticReadPage(pageIdxToPin, func);
}

void OverflowFile::writePageToDisk(page_idx_t pageIdx, uint8_t* data) const {
    if (pageIdx < numPagesOnDisk) {
        KU_ASSERT(shadowFile);
        ShadowUtils::updatePage(*getFileHandle(), pageIdx, true /* overwriting entire page*/,
            *shadowFile, [&](auto* frame) { memcpy(frame, data, KUZU_PAGE_SIZE); });
    } else {
        KU_ASSERT(fileHandle);
        KU_ASSERT(!fileHandle->isInMemoryMode());
        fileHandle->writePageToFile(data, pageIdx);
    }
}

void OverflowFile::checkpoint(bool forceUpdateHeader) {
    KU_ASSERT(fileHandle);
    // TODO(bmwinger): Ideally this could be done separately and in parallel by each HashIndex
    // However fileHandle->addNewPages needs to be called beforehand,
    // but after each HashIndex::prepareCommit has written to the in-memory pages
    for (auto& handle : handles) {
        handle->checkpoint();
    }
    if (headerChanged || forceUpdateHeader) {
        uint8_t page[KUZU_PAGE_SIZE];
        memcpy(page, &header, sizeof(header));
        // Zero free space at the end of the header page
        std::fill(page + sizeof(header), page + KUZU_PAGE_SIZE, 0);
        writePageToDisk(headerPageIdx + HEADER_PAGE_IDX, page);
    }
}

void OverflowFile::checkpointInMemory() {
    headerChanged = false;
}

void OverflowFile::rollbackInMemory() {
    if (fileHandle->getNumPages() > headerPageIdx) {
        readFromDisk(TransactionType::READ_ONLY, headerPageIdx,
            [&](auto* frame) { memcpy(&header, frame, sizeof(header)); });
    }
    for (auto i = 0u; i < handles.size(); i++) {
        auto& handle = handles[i];
        handle->rollbackInMemory(header.entries[i].cursor);
    }
}

void OverflowFile::reclaimStorage(PageManager& pageManager) const {
    for (auto& handle : handles) {
        handle->reclaimStorage(pageManager);
    }
    if (headerPageIdx != INVALID_PAGE_IDX) {
        fileHandle->getPageManager()->freePage(headerPageIdx);
    }
}

} // namespace storage
} // namespace kuzu

#include "storage/storage_structure/overflow_file.h"

#include <memory>

#include "common/constants.h"
#include "common/type_utils.h"
#include "common/types/types.h"
#include "storage/buffer_manager/bm_file_handle.h"
#include "storage/file_handle.h"
#include "storage/storage_structure/db_file_utils.h"
#include "storage/storage_structure/in_mem_page.h"
#include "storage/storage_utils.h"
#include "transaction/transaction.h"

using namespace kuzu::transaction;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

std::string OverflowFileHandle::readString(TransactionType trxType, const ku_string_t& str) {
    if (ku_string_t::isShortString(str.len)) {
        return str.getAsShortString();
    } else {
        PageCursor cursor;
        TypeUtils::decodeOverflowPtr(str.overflowPtr, cursor.pageIdx, cursor.elemPosInPage);
        std::string retVal;
        retVal.reserve(str.len);
        int32_t remainingLength = str.len;
        while (remainingLength > 0) {
            auto numBytesToReadInPage = std::min(static_cast<uint32_t>(remainingLength),
                END_OF_PAGE - cursor.elemPosInPage);
            auto startPosInSrc = retVal.size();
            read(trxType, cursor.pageIdx, [&](uint8_t* frame) {
                // Replace rather than append, since optimistic read may call the function multiple
                // times
                retVal.replace(startPosInSrc, numBytesToReadInPage,
                    std::string_view(reinterpret_cast<const char*>(frame) + cursor.elemPosInPage,
                        numBytesToReadInPage));
                cursor.pageIdx = *(page_idx_t*)(frame + END_OF_PAGE);
            });
            remainingLength -= numBytesToReadInPage;
            // After the first page we always start reading from the beginning of the page.
            cursor.elemPosInPage = 0;
        }
        return retVal;
    }
}

bool OverflowFileHandle::equals(TransactionType trxType, std::string_view keyToLookup,
    const common::ku_string_t& keyInEntry) const {
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
            cursor.pageIdx = *(page_idx_t*)(frame + END_OF_PAGE);
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
        pageWriteCache[nextPosToWriteTo.pageIdx]->write(END_OF_PAGE,
            reinterpret_cast<uint8_t*>(&newPageIdx), sizeof(page_idx_t));
    }
    pageWriteCache.emplace(newPageIdx, std::make_unique<InMemPage>());
    nextPosToWriteTo.elemPosInPage = 0;
    nextPosToWriteTo.pageIdx = newPageIdx;
    return pageWriteCache[newPageIdx]->data;
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
            pageToWrite = cached->second->data;
        } else {
            overflowFile.readFromDisk(TransactionType::WRITE, nextPosToWriteTo.pageIdx,
                [&](auto* frame) {
                    auto page = std::make_unique<InMemPage>();
                    memcpy(page->data, frame, BufferPoolConstants::PAGE_4KB_SIZE);
                    pageToWrite = page->data;
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
    auto inlineLen = std::min(shortStrLen, (uint64_t)result.len);
    memcpy(result.prefix, rawString.data(), inlineLen);
    setStringOverflow(rawString.data(), result.len, result);
    return result;
}

void OverflowFileHandle::prepareCommit() {
    for (auto& [pageIndex, page] : pageWriteCache) {
        overflowFile.writePageToDisk(pageIndex, page->data);
    }
}

void OverflowFileHandle::read(transaction::TransactionType trxType, common::page_idx_t pageIdx,
    const std::function<void(uint8_t*)>& func) const {
    if (trxType == TransactionType::WRITE) {
        auto cachedPage = pageWriteCache.find(pageIdx);
        if (cachedPage != pageWriteCache.end()) {
            return func(cachedPage->second->data);
        }
    }
    overflowFile.readFromDisk(trxType, pageIdx, func);
}

static inline DBFileIDAndName constructDBFileIDAndName(
    const DBFileIDAndName& dbFileIdAndNameForMainDBFile) {
    DBFileIDAndName copy = dbFileIdAndNameForMainDBFile;
    copy.dbFileID.isOverflow = true;
    copy.fName = StorageUtils::getOverflowFileName(dbFileIdAndNameForMainDBFile.fName);
    return copy;
}

OverflowFile::OverflowFile(const DBFileIDAndName& dbFileIdAndName, BufferManager* bufferManager,
    WAL* wal, bool readOnly, common::VirtualFileSystem* vfs)
    : bufferManager{bufferManager}, wal{wal}, headerChanged{false} {
    auto overflowFileIDAndName = constructDBFileIDAndName(dbFileIdAndName);
    dbFileID = overflowFileIDAndName.dbFileID;
    fileHandle = bufferManager->getBMFileHandle(overflowFileIDAndName.fName,
        readOnly ? FileHandle::O_PERSISTENT_FILE_READ_ONLY :
                   FileHandle::O_PERSISTENT_FILE_NO_CREATE,
        BMFileHandle::FileVersionedType::VERSIONED_FILE, vfs);
    if (vfs->fileOrPathExists(overflowFileIDAndName.fName)) {
        readFromDisk(transaction::TransactionType::READ_ONLY, HEADER_PAGE_IDX,
            [&](auto* frame) { memcpy(&header, frame, sizeof(header)); });
        pageCounter = numPagesOnDisk = header.pages;
    } else {
        // Reserve a page for the header
        getNewPageIdx();
    }
}

void OverflowFile::createEmptyFiles(const std::string& fName, common::VirtualFileSystem* vfs) {
    FileHandle fileHandle(fName, FileHandle::O_PERSISTENT_FILE_CREATE_NOT_EXISTS, vfs);
    uint8_t page[common::BufferPoolConstants::PAGE_4KB_SIZE];
    StringOverflowFileHeader header;
    memcpy(page, &header, sizeof(StringOverflowFileHeader));
    // Zero free space at the end of the header page
    std::fill(page + sizeof(header), page + BufferPoolConstants::PAGE_4KB_SIZE, 0);
    fileHandle.addNewPage();
    fileHandle.writePage(page, HEADER_PAGE_IDX);
}

void OverflowFile::readFromDisk(transaction::TransactionType trxType, common::page_idx_t pageIdx,
    const std::function<void(uint8_t*)>& func) const {
    auto [fileHandleToPin, pageIdxToPin] =
        storage::DBFileUtils::getFileHandleAndPhysicalPageIdxToPin(*getBMFileHandle(), pageIdx,
            *wal, trxType);
    bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, func);
}

void OverflowFile::writePageToDisk(common::page_idx_t pageIdx, uint8_t* data) const {
    if (pageIdx < numPagesOnDisk) {
        // TODO: updatePage does an unnecessary read + copy. We just want to overwrite
        DBFileUtils::updatePage(*getBMFileHandle(), dbFileID, pageIdx, false, *bufferManager, *wal,
            [&](auto* frame) { memcpy(frame, data, BufferPoolConstants::PAGE_4KB_SIZE); });
    } else {
        fileHandle->writePage(data, pageIdx);
    }
}

void OverflowFile::prepareCommit() {
    if (fileHandle->getNumPages() < pageCounter) {
        fileHandle->addNewPages(pageCounter - fileHandle->getNumPages());
    }
    // TODO(bmwinger): Ideally this could be done separately and in parallel by each HashIndex
    // However fileHandle->addNewPages needs to be called beforehand,
    // but after each HashIndex::prepareCommit has written to the in-memory pages
    for (auto& handle : handles) {
        handle->prepareCommit();
    }
    if (headerChanged) {
        uint8_t page[BufferPoolConstants::PAGE_4KB_SIZE];
        header.pages = pageCounter;
        memcpy(page, &header, sizeof(header));
        // Zero free space at the end of the header page
        std::fill(page + sizeof(header), page + BufferPoolConstants::PAGE_4KB_SIZE, 0);
        writePageToDisk(HEADER_PAGE_IDX, page);
    }
}

void OverflowFile::checkpointInMemory() {
    headerChanged = false;
    numPagesOnDisk = pageCounter;
}

void OverflowFile::rollbackInMemory() {
    readFromDisk(transaction::TransactionType::READ_ONLY, HEADER_PAGE_IDX,
        [&](auto* frame) { memcpy(&header, frame, sizeof(header)); });
    numPagesOnDisk = pageCounter = header.pages;
    for (auto i = 0u; i < handles.size(); i++) {
        auto& handle = handles[i];
        handle->rollbackInMemory(header.cursors[i]);
    }
}

} // namespace storage
} // namespace kuzu

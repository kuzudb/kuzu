#include "storage/storage_structure/disk_overflow_file.h"

#include "common/string_format.h"
#include "common/type_utils.h"

using lock_t = std::unique_lock<std::mutex>;

using namespace kuzu::transaction;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

std::string DiskOverflowFile::readString(TransactionType trxType, const ku_string_t& str) {
    if (ku_string_t::isShortString(str.len)) {
        return str.getAsShortString();
    } else {
        PageByteCursor cursor;
        TypeUtils::decodeOverflowPtr(str.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
        auto [fileHandleToPin, pageIdxToPin] = DBFileUtils::getFileHandleAndPhysicalPageIdxToPin(
            *fileHandle, cursor.pageIdx, *wal, trxType);
        std::string retVal;
        bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) {
            retVal = std::string((char*)(frame + cursor.offsetInPage), str.len);
        });
        return retVal;
    }
}

void DiskOverflowFile::addNewPageIfNecessaryWithoutLock(uint32_t numBytesToAppend) {
    PageElementCursor byteCursor = PageUtils::getPageElementCursorForPos(
        nextBytePosToWriteTo, BufferPoolConstants::PAGE_4KB_SIZE);
    if ((byteCursor.elemPosInPage == 0) ||
        ((byteCursor.elemPosInPage + numBytesToAppend - 1) > BufferPoolConstants::PAGE_4KB_SIZE)) {
        // Note that if byteCursor.pos is already 0 the next operation keeps the nextBytePos
        // where it is.
        nextBytePosToWriteTo = (fileHandle->getNumPages() * BufferPoolConstants::PAGE_4KB_SIZE);
        DBFileUtils::insertNewPage(*fileHandle, dbFileID, *bufferManager, *wal);
    }
}

void DiskOverflowFile::setStringOverflowWithoutLock(
    const char* srcRawString, uint64_t len, ku_string_t& diskDstString) {
    if (len <= ku_string_t::SHORT_STR_LENGTH) {
        return;
    } else if (len > BufferPoolConstants::PAGE_4KB_SIZE) {
        throw RuntimeException(
            stringFormat("Maximum length of strings is {}. Input string's length is {}.",
                BufferPoolConstants::PAGE_4KB_SIZE, strlen(srcRawString)));
    }
    addNewPageIfNecessaryWithoutLock(len);
    auto updatedPageInfoAndWALPageFrame = createWALVersionOfPageIfNecessaryForElement(
        nextBytePosToWriteTo, BufferPoolConstants::PAGE_4KB_SIZE);
    memcpy(updatedPageInfoAndWALPageFrame.frame + updatedPageInfoAndWALPageFrame.posInPage,
        srcRawString, len);
    TypeUtils::encodeOverflowPtr(diskDstString.overflowPtr,
        updatedPageInfoAndWALPageFrame.originalPageIdx, updatedPageInfoAndWALPageFrame.posInPage);
    nextBytePosToWriteTo += len;
    DBFileUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, *fileHandle, *bufferManager, *wal);
}

ku_string_t DiskOverflowFile::writeString(const char* rawString) {
    lock_t lck{mtx};
    ku_string_t result;
    result.len = strlen(rawString);
    auto shortStrLen = ku_string_t::SHORT_STR_LENGTH;
    auto inlineLen = std::min(shortStrLen, (uint64_t)result.len);
    memcpy(result.prefix, rawString, inlineLen);
    logNewOverflowFileNextBytePosRecordIfNecessaryWithoutLock();
    setStringOverflowWithoutLock(rawString, result.len, result);
    return result;
}

void DiskOverflowFile::logNewOverflowFileNextBytePosRecordIfNecessaryWithoutLock() {
    if (!loggedNewOverflowFileNextBytePosRecord) {
        loggedNewOverflowFileNextBytePosRecord = true;
        wal->logOverflowFileNextBytePosRecord(dbFileID, nextBytePosToWriteTo);
    }
}

WALPageIdxPosInPageAndFrame DiskOverflowFile::createWALVersionOfPageIfNecessaryForElement(
    uint64_t elementOffset, uint64_t numElementsPerPage) {
    auto originalPageCursor =
        PageUtils::getPageElementCursorForPos(elementOffset, numElementsPerPage);
    bool insertingNewPage = false;
    if (originalPageCursor.pageIdx >= fileHandle->getNumPages()) {
        KU_ASSERT(originalPageCursor.pageIdx == fileHandle->getNumPages());
        DBFileUtils::insertNewPage(*fileHandle, dbFileID, *bufferManager, *wal);
        insertingNewPage = true;
    }
    auto walPageIdxAndFrame = DBFileUtils::createWALVersionIfNecessaryAndPinPage(
        originalPageCursor.pageIdx, insertingNewPage, *fileHandle, dbFileID, *bufferManager, *wal);
    return {walPageIdxAndFrame, originalPageCursor.elemPosInPage};
}

} // namespace storage
} // namespace kuzu

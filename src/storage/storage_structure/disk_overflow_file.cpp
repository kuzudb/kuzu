#include "storage/storage_structure/disk_overflow_file.h"

#include <memory>

#include "common/constants.h"
#include "common/exception/message.h"
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
        PageCursor cursor;
        TypeUtils::decodeOverflowPtr(str.overflowPtr, cursor.pageIdx, cursor.elemPosInPage);
        std::string retVal;
        retVal.reserve(str.len);
        int32_t remainingLength = str.len;
        while (remainingLength > 0) {
            auto [fileHandleToPin, pageIdxToPin] =
                DBFileUtils::getFileHandleAndPhysicalPageIdxToPin(
                    *fileHandle, cursor.pageIdx, *wal, trxType);
            auto numBytesToReadInPage = std::min(static_cast<uint64_t>(remainingLength),
                BufferPoolConstants::PAGE_4KB_SIZE - cursor.elemPosInPage);
            bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) {
                retVal +=
                    std::string_view(reinterpret_cast<const char*>(frame) + cursor.elemPosInPage,
                        numBytesToReadInPage);
            });
            remainingLength -= numBytesToReadInPage;
            // After the first page we always start reading from the beginning of the page.
            cursor.nextPage();
        }
        return retVal;
    }
}

void DiskOverflowFile::addNewPageIfNecessaryWithoutLock(uint32_t numBytesToAppend) {
    PageCursor byteCursor =
        PageUtils::getPageCursorForPos(nextBytePosToWriteTo, BufferPoolConstants::PAGE_4KB_SIZE);
    if ((byteCursor.elemPosInPage == 0) ||
        ((byteCursor.elemPosInPage + numBytesToAppend - 1) > BufferPoolConstants::PAGE_4KB_SIZE)) {
        DBFileUtils::insertNewPage(*fileHandle, dbFileID, *bufferManager, *wal);
    }
}

void DiskOverflowFile::setStringOverflowWithoutLock(
    const char* srcRawString, uint64_t len, ku_string_t& diskDstString) {
    if (len <= ku_string_t::SHORT_STR_LENGTH) {
        return;
    } else if (len > BufferPoolConstants::PAGE_256KB_SIZE) {
        throw RuntimeException(ExceptionMessage::overLargeStringPKValueException(len));
    }
    int32_t remainingLength = len;
    while (remainingLength > 0) {
        auto bytesWritten = len - remainingLength;
        auto numBytesToWriteInPage = std::min(static_cast<uint64_t>(remainingLength),
            BufferPoolConstants::PAGE_4KB_SIZE -
                (nextBytePosToWriteTo % BufferPoolConstants::PAGE_4KB_SIZE));
        addNewPageIfNecessaryWithoutLock(remainingLength);
        auto updatedPageInfoAndWALPageFrame = createWALVersionOfPageIfNecessaryForElement(
            nextBytePosToWriteTo, BufferPoolConstants::PAGE_4KB_SIZE);
        memcpy(updatedPageInfoAndWALPageFrame.frame + updatedPageInfoAndWALPageFrame.posInPage,
            srcRawString + bytesWritten, numBytesToWriteInPage);
        DBFileUtils::unpinWALPageAndReleaseOriginalPageLock(
            updatedPageInfoAndWALPageFrame, *fileHandle, *bufferManager, *wal);
        // The overflow pointer should point to the first position, so it must only run
        // the first iteration of the loop
        if (static_cast<uint64_t>(remainingLength) == len) {
            TypeUtils::encodeOverflowPtr(diskDstString.overflowPtr,
                updatedPageInfoAndWALPageFrame.originalPageIdx,
                updatedPageInfoAndWALPageFrame.posInPage);
        }
        remainingLength -= numBytesToWriteInPage;
        nextBytePosToWriteTo += numBytesToWriteInPage;
    }
}

ku_string_t DiskOverflowFile::writeString(std::string_view rawString) {
    lock_t lck{mtx};
    ku_string_t result;
    result.len = rawString.length();
    auto shortStrLen = ku_string_t::SHORT_STR_LENGTH;
    auto inlineLen = std::min(shortStrLen, (uint64_t)result.len);
    memcpy(result.prefix, rawString.data(), inlineLen);
    logNewOverflowFileNextBytePosRecordIfNecessaryWithoutLock();
    setStringOverflowWithoutLock(rawString.data(), result.len, result);
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
    auto originalPageCursor = PageUtils::getPageCursorForPos(elementOffset, numElementsPerPage);
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

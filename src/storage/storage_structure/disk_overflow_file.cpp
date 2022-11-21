#include "storage/storage_structure/disk_overflow_file.h"

#include "common/in_mem_overflow_buffer_utils.h"
#include "common/type_utils.h"

using lock_t = unique_lock<mutex>;

namespace kuzu {
namespace storage {

void DiskOverflowFile::readStringsToVector(TransactionType trxType, ValueVector& valueVector) {
    assert(!valueVector.state->isFlat());
    for (auto i = 0u; i < valueVector.state->selVector->selectedSize; i++) {
        auto pos = valueVector.state->selVector->selectedPositions[i];
        if (valueVector.isNull(pos)) {
            continue;
        }
        readStringToVector(
            trxType, ((ku_string_t*)valueVector.getData())[pos], valueVector.getOverflowBuffer());
    }
}

void DiskOverflowFile::readStringToVector(
    TransactionType trxType, ku_string_t& kuStr, InMemOverflowBuffer& inMemOverflowBuffer) {
    if (ku_string_t::isShortString(kuStr.len)) {
        return;
    }
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(kuStr.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            fileHandle, cursor.pageIdx, *wal, trxType);
    auto frame = bufferManager.pin(*fileHandleToPin, pageIdxToPin);
    InMemOverflowBufferUtils::copyString(
        (char*)(frame + cursor.offsetInPage), kuStr.len, kuStr, inMemOverflowBuffer);
    bufferManager.unpin(*fileHandleToPin, pageIdxToPin);
}

void DiskOverflowFile::scanSequentialStringOverflow(TransactionType trxType, ValueVector& vector) {
    FileHandle* cachedFileHandle = nullptr;
    page_idx_t cachedPageIdx = UINT32_MAX;
    uint8_t* cachedFrame = nullptr;
    for (auto i = 0u; i < vector.state->selVector->selectedSize; ++i) {
        auto pos = vector.state->selVector->selectedPositions[i];
        if (vector.isNull(pos)) {
            continue;
        }
        auto& kuString = ((ku_string_t*)vector.getData())[pos];
        if (ku_string_t::isShortString(kuString.len)) {
            continue;
        }
        page_idx_t pageIdx = UINT32_MAX;
        uint16_t pagePos = UINT16_MAX;
        TypeUtils::decodeOverflowPtr(kuString.overflowPtr, pageIdx, pagePos);
        auto [fileHandleToPin, pageIdxToPin] =
            StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
                fileHandle, pageIdx, *wal, trxType);
        if (pageIdxToPin == cachedPageIdx) { // cache hit
            InMemOverflowBufferUtils::copyString(
                (char*)(cachedFrame + pagePos), kuString.len, kuString, vector.getOverflowBuffer());
            continue;
        }
        // cache miss
        if (cachedPageIdx != UINT32_MAX) { // unpin cached frame
            bufferManager.unpin(*cachedFileHandle, cachedPageIdx);
        }
        // pin new frame and update cache
        auto frame = bufferManager.pin(*fileHandleToPin, pageIdxToPin);
        InMemOverflowBufferUtils::copyString(
            (char*)(frame + pagePos), kuString.len, kuString, vector.getOverflowBuffer());
        cachedFileHandle = fileHandleToPin;
        cachedPageIdx = pageIdxToPin;
        cachedFrame = frame;
    }
    if (cachedPageIdx != UINT32_MAX) {
        bufferManager.unpin(*cachedFileHandle, cachedPageIdx);
    }
}

void DiskOverflowFile::readListsToVector(TransactionType trxType, ValueVector& valueVector) {
    assert(!valueVector.state->isFlat());
    for (auto i = 0u; i < valueVector.state->selVector->selectedSize; i++) {
        auto pos = valueVector.state->selVector->selectedPositions[i];
        if (!valueVector.isNull(pos)) {
            readListToVector(trxType, ((ku_list_t*)valueVector.getData())[pos],
                valueVector.dataType, valueVector.getOverflowBuffer());
        }
    }
}

void DiskOverflowFile::readListToVector(TransactionType trxType, ku_list_t& kuList,
    const DataType& dataType, InMemOverflowBuffer& inMemOverflowBuffer) {
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(kuList.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            fileHandle, cursor.pageIdx, *wal, trxType);
    auto frame = bufferManager.pin(*fileHandleToPin, pageIdxToPin);
    InMemOverflowBufferUtils::copyListNonRecursive(
        frame + cursor.offsetInPage, kuList, dataType, inMemOverflowBuffer);
    bufferManager.unpin(*fileHandleToPin, pageIdxToPin);
    if (dataType.childType->typeID == STRING) {
        auto kuStrings = (ku_string_t*)(kuList.overflowPtr);
        for (auto i = 0u; i < kuList.size; i++) {
            readStringToVector(trxType, kuStrings[i], inMemOverflowBuffer);
        }
    } else if (dataType.childType->typeID == LIST) {
        auto kuLists = (ku_list_t*)(kuList.overflowPtr);
        for (auto i = 0u; i < kuList.size; i++) {
            readListToVector(trxType, kuLists[i], *dataType.childType, inMemOverflowBuffer);
        }
    }
}

string DiskOverflowFile::readString(TransactionType trxType, const ku_string_t& str) {
    if (ku_string_t::isShortString(str.len)) {
        return str.getAsShortString();
    } else {
        PageByteCursor cursor;
        TypeUtils::decodeOverflowPtr(str.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
        auto [fileHandleToPin, pageIdxToPin] =
            StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
                fileHandle, cursor.pageIdx, *wal, trxType);
        auto frame = bufferManager.pin(*fileHandleToPin, pageIdxToPin);
        auto retVal = string((char*)(frame + cursor.offsetInPage), str.len);
        bufferManager.unpin(*fileHandleToPin, pageIdxToPin);
        return retVal;
    }
}

vector<Literal> DiskOverflowFile::readList(
    TransactionType trxType, const ku_list_t& listVal, const DataType& dataType) {
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(listVal.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            fileHandle, cursor.pageIdx, *wal, trxType);
    auto frame = bufferManager.pin(*fileHandleToPin, pageIdxToPin);
    auto numBytesOfSingleValue = Types::getDataTypeSize(*dataType.childType);
    auto numValuesInList = listVal.size;
    vector<Literal> retLiterals;
    if (dataType.childType->typeID == STRING) {
        for (auto i = 0u; i < numValuesInList; i++) {
            auto kuListVal = *(ku_string_t*)(frame + cursor.offsetInPage);
            retLiterals.emplace_back(readString(trxType, kuListVal));
            cursor.offsetInPage += numBytesOfSingleValue;
        }
    } else if (dataType.childType->typeID == LIST) {
        for (auto i = 0u; i < numValuesInList; i++) {
            auto kuListVal = *(ku_list_t*)(frame + cursor.offsetInPage);
            retLiterals.emplace_back(
                readList(trxType, kuListVal, *dataType.childType), *dataType.childType);
            cursor.offsetInPage += numBytesOfSingleValue;
        }
    } else {
        for (auto i = 0u; i < numValuesInList; i++) {
            retLiterals.emplace_back(frame + cursor.offsetInPage, *dataType.childType);
            cursor.offsetInPage += numBytesOfSingleValue;
        }
    }
    bufferManager.unpin(*fileHandleToPin, pageIdxToPin);
    return retLiterals;
}

void DiskOverflowFile::addNewPageIfNecessaryWithoutLock(uint32_t numBytesToAppend) {
    PageElementCursor byteCursor =
        PageUtils::getPageElementCursorForPos(nextBytePosToWriteTo, DEFAULT_PAGE_SIZE);
    if ((byteCursor.elemPosInPage == 0) ||
        ((byteCursor.elemPosInPage + numBytesToAppend - 1) > DEFAULT_PAGE_SIZE)) {
        // Note that if byteCursor.pos is already 0 the next operation keeps the nextBytePos
        // where it is.
        nextBytePosToWriteTo = (fileHandle.getNumPages() * DEFAULT_PAGE_SIZE);
        addNewPageToFileHandle();
    }
}

void DiskOverflowFile::setStringOverflowWithoutLock(
    const char* srcRawString, uint64_t len, ku_string_t& diskDstString) {
    if (len <= ku_string_t::SHORT_STR_LENGTH) {
        return;
    } else if (len > DEFAULT_PAGE_SIZE) {
        throw RuntimeException(
            StringUtils::getLongStringErrorMessage(srcRawString, DEFAULT_PAGE_SIZE));
    }
    addNewPageIfNecessaryWithoutLock(len);
    auto updatedPageInfoAndWALPageFrame =
        createWALVersionOfPageIfNecessaryForElement(nextBytePosToWriteTo, DEFAULT_PAGE_SIZE);
    memcpy(updatedPageInfoAndWALPageFrame.frame + updatedPageInfoAndWALPageFrame.posInPage,
        srcRawString, len);
    TypeUtils::encodeOverflowPtr(diskDstString.overflowPtr,
        updatedPageInfoAndWALPageFrame.originalPageIdx, updatedPageInfoAndWALPageFrame.posInPage);
    nextBytePosToWriteTo += len;
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, fileHandle, bufferManager, *wal);
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

void DiskOverflowFile::writeStringOverflowAndUpdateOverflowPtr(
    const ku_string_t& strToWriteFrom, ku_string_t& strToWriteTo) {
    // Note: Currently we are completely serializing threads that want to write to the overflow
    // pages. We can relax this by releasing the lock after each thread 'reserves' their spot
    // to write to in an overflow page by getting a nextBytePosToWriteTo and updating the
    // nextBytePosToWriteTo field. Then they can coordinate by acquiring the page lock of
    // the page that this location corresponds to. However, it is still likely that concurrent
    // threads will still try to write to the same page and block each other because we
    // give nextBytePosToWriteTo consecutively.
    lock_t lck{mtx};
    logNewOverflowFileNextBytePosRecordIfNecessaryWithoutLock();
    setStringOverflowWithoutLock(
        (const char*)strToWriteFrom.overflowPtr, strToWriteFrom.len, strToWriteTo);
}

void DiskOverflowFile::setListRecursiveIfNestedWithoutLock(
    const ku_list_t& inMemSrcList, ku_list_t& diskDstList, const DataType& dataType) {
    auto elementSize = Types::getDataTypeSize(*dataType.childType);
    if (inMemSrcList.size * elementSize > DEFAULT_PAGE_SIZE) {
        throw RuntimeException(StringUtils::string_format(
            "Maximum num bytes of a LIST is %d. Input list's num bytes is %d.", DEFAULT_PAGE_SIZE,
            inMemSrcList.size * elementSize));
    }
    addNewPageIfNecessaryWithoutLock(inMemSrcList.size * elementSize);
    auto updatedPageInfoAndWALPageFrame =
        createWALVersionOfPageIfNecessaryForElement(nextBytePosToWriteTo, DEFAULT_PAGE_SIZE);
    diskDstList.size = inMemSrcList.size;
    // Copy non-overflow part for elements in the list.
    memcpy(updatedPageInfoAndWALPageFrame.frame + updatedPageInfoAndWALPageFrame.posInPage,
        (uint8_t*)inMemSrcList.overflowPtr, inMemSrcList.size * elementSize);
    nextBytePosToWriteTo += inMemSrcList.size * elementSize;
    TypeUtils::encodeOverflowPtr(diskDstList.overflowPtr,
        updatedPageInfoAndWALPageFrame.originalPageIdx, updatedPageInfoAndWALPageFrame.posInPage);
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, fileHandle, bufferManager, *wal);
    if (dataType.childType->typeID == STRING) {
        // Copy overflow for string elements in the list.
        auto dstListElements = (ku_string_t*)(updatedPageInfoAndWALPageFrame.frame +
                                              updatedPageInfoAndWALPageFrame.posInPage);
        for (auto i = 0u; i < diskDstList.size; i++) {
            auto kuString = ((ku_string_t*)inMemSrcList.overflowPtr)[i];
            setStringOverflowWithoutLock(
                (const char*)kuString.overflowPtr, kuString.len, dstListElements[i]);
        }
    } else if (dataType.childType->typeID == LIST) {
        // Recursively copy overflow for list elements in the list.
        auto dstListElements = (ku_list_t*)(updatedPageInfoAndWALPageFrame.frame +
                                            updatedPageInfoAndWALPageFrame.posInPage);
        for (auto i = 0u; i < diskDstList.size; i++) {
            setListRecursiveIfNestedWithoutLock(
                ((ku_list_t*)inMemSrcList.overflowPtr)[i], dstListElements[i], *dataType.childType);
        }
    }
}

void DiskOverflowFile::writeListOverflowAndUpdateOverflowPtr(
    const ku_list_t& listToWriteFrom, ku_list_t& listToWriteTo, const DataType& dataType) {
    lock_t lck{mtx};
    logNewOverflowFileNextBytePosRecordIfNecessaryWithoutLock();
    setListRecursiveIfNestedWithoutLock(listToWriteFrom, listToWriteTo, dataType);
}

void DiskOverflowFile::logNewOverflowFileNextBytePosRecordIfNecessaryWithoutLock() {
    if (!loggedNewOverflowFileNextBytePosRecord) {
        loggedNewOverflowFileNextBytePosRecord = true;
        wal->logOverflowFileNextBytePosRecord(
            fileHandle.getStorageStructureIDIDForWALRecord(), nextBytePosToWriteTo);
    }
}

} // namespace storage
} // namespace kuzu

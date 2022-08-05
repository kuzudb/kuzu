#include "src/storage/storage_structure/include/overflow_file.h"

#include "src/common/include/overflow_buffer_utils.h"
#include "src/common/include/type_utils.h"

using lock_t = unique_lock<mutex>;

namespace graphflow {
namespace storage {

void OverflowFile::readStringsToVector(Transaction* transaction, ValueVector& valueVector) {
    assert(!valueVector.state->isFlat());
    for (auto i = 0u; i < valueVector.state->selVector->selectedSize; i++) {
        auto pos = valueVector.state->selVector->selectedPositions[i];
        if (valueVector.isNull(pos)) {
            continue;
        }
        readStringToVector(
            transaction, ((gf_string_t*)valueVector.values)[pos], valueVector.getOverflowBuffer());
    }
}

void OverflowFile::readStringToVector(
    Transaction* transaction, gf_string_t& gfStr, OverflowBuffer& overflowBuffer) {
    if (gf_string_t::isShortString(gfStr.len)) {
        return;
    }
    page_idx_t pageIdx = UINT32_MAX;
    uint16_t pagePos = UINT16_MAX;
    TypeUtils::decodeOverflowPtr(gfStr.overflowPtr, pageIdx, pagePos);
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            fileHandle, pageIdx, *wal, transaction->isReadOnly());
    auto frame = bufferManager.pin(*fileHandleToPin, pageIdxToPin);
    OverflowBufferUtils::copyString((char*)(frame + pagePos), gfStr.len, gfStr, overflowBuffer);
    bufferManager.unpin(*fileHandleToPin, pageIdxToPin);
}

void OverflowFile::scanSequentialStringOverflow(Transaction* transaction, ValueVector& vector) {
    FileHandle* cachedFileHandle = nullptr;
    page_idx_t cachedPageIdx = UINT32_MAX;
    uint8_t* cachedFrame = nullptr;
    for (auto i = 0u; i < vector.state->selVector->selectedSize; ++i) {
        auto pos = vector.state->selVector->selectedPositions[i];
        if (vector.isNull(pos)) {
            continue;
        }
        auto& gfString = ((gf_string_t*)vector.values)[pos];
        if (gf_string_t::isShortString(gfString.len)) {
            continue;
        }
        page_idx_t pageIdx = UINT32_MAX;
        uint16_t pagePos = UINT16_MAX;
        TypeUtils::decodeOverflowPtr(gfString.overflowPtr, pageIdx, pagePos);
        auto [fileHandleToPin, pageIdxToPin] =
            StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
                fileHandle, pageIdx, *wal, transaction->isReadOnly());
        if (pageIdxToPin == cachedPageIdx) { // cache hit
            OverflowBufferUtils::copyString(
                (char*)(cachedFrame + pagePos), gfString.len, gfString, vector.getOverflowBuffer());
            continue;
        }
        // cache miss
        if (cachedPageIdx != UINT32_MAX) { // unpin cached frame
            bufferManager.unpin(*cachedFileHandle, cachedPageIdx);
        }
        // pin new frame and update cache
        auto frame = bufferManager.pin(*fileHandleToPin, pageIdxToPin);
        OverflowBufferUtils::copyString(
            (char*)(frame + pagePos), gfString.len, gfString, vector.getOverflowBuffer());
        cachedFileHandle = fileHandleToPin;
        cachedPageIdx = pageIdxToPin;
        cachedFrame = frame;
    }
    if (cachedPageIdx != UINT32_MAX) {
        bufferManager.unpin(*cachedFileHandle, cachedPageIdx);
    }
}

void OverflowFile::readListsToVector(ValueVector& valueVector) {
    assert(!valueVector.state->isFlat());
    for (auto i = 0u; i < valueVector.state->selVector->selectedSize; i++) {
        auto pos = valueVector.state->selVector->selectedPositions[i];
        if (!valueVector.isNull(pos)) {
            readListToVector(((gf_list_t*)valueVector.values)[pos], valueVector.dataType,
                valueVector.getOverflowBuffer());
        }
    }
}

void OverflowFile::readListToVector(
    gf_list_t& gfList, const DataType& dataType, OverflowBuffer& overflowBuffer) {
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(gfList.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
    auto frame = bufferManager.pin(fileHandle, cursor.pageIdx);
    OverflowBufferUtils::copyListNonRecursive(
        frame + cursor.offsetInPage, gfList, dataType, overflowBuffer);
    bufferManager.unpin(fileHandle, cursor.pageIdx);
    if (dataType.childType->typeID == STRING) {
        auto gfStrings = (gf_string_t*)(gfList.overflowPtr);
        for (auto i = 0u; i < gfList.size; i++) {
            Transaction tmpTransaction(READ_ONLY, -1);
            readStringToVector(&tmpTransaction, gfStrings[i], overflowBuffer);
        }
    } else if (dataType.childType->typeID == LIST) {
        auto gfLists = (gf_list_t*)(gfList.overflowPtr);
        for (auto i = 0u; i < gfList.size; i++) {
            readListToVector(gfLists[i], *dataType.childType, overflowBuffer);
        }
    }
}

string OverflowFile::readString(Transaction* transaction, const gf_string_t& str) {
    if (gf_string_t::isShortString(str.len)) {
        return str.getAsShortString();
    } else {
        PageByteCursor cursor;
        TypeUtils::decodeOverflowPtr(str.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
        auto [fileHandleToPin, pageIdxToPin] =
            StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
                fileHandle, cursor.pageIdx, *wal, transaction->isReadOnly());
        auto frame = bufferManager.pin(*fileHandleToPin, pageIdxToPin);
        auto retVal = string((char*)(frame + cursor.offsetInPage), str.len);
        bufferManager.unpin(*fileHandleToPin, pageIdxToPin);
        return retVal;
    }
}

vector<Literal> OverflowFile::readList(const gf_list_t& listVal, const DataType& dataType) {
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(listVal.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
    auto frame = bufferManager.pin(fileHandle, cursor.pageIdx);
    auto numBytesOfSingleValue = Types::getDataTypeSize(*dataType.childType);
    auto numValuesInList = listVal.size;
    vector<Literal> retLiterals;
    if (dataType.childType->typeID == STRING) {
        for (auto i = 0u; i < numValuesInList; i++) {
            auto gfListVal = *(gf_string_t*)(frame + cursor.offsetInPage);
            retLiterals.emplace_back(readString(gfListVal));
            cursor.offsetInPage += numBytesOfSingleValue;
        }
    } else if (dataType.childType->typeID == LIST) {
        for (auto i = 0u; i < numValuesInList; i++) {
            auto gfListVal = *(gf_list_t*)(frame + cursor.offsetInPage);
            retLiterals.emplace_back(readList(gfListVal, *dataType.childType), *dataType.childType);
            cursor.offsetInPage += numBytesOfSingleValue;
        }
    } else {
        for (auto i = 0u; i < numValuesInList; i++) {
            retLiterals.emplace_back(frame + cursor.offsetInPage, *dataType.childType);
            cursor.offsetInPage += numBytesOfSingleValue;
        }
    }
    bufferManager.unpin(fileHandle, cursor.pageIdx);
    return retLiterals;
}

void OverflowFile::addNewPageIfNecessaryWithoutLock(uint32_t numBytesToAppend) {
    PageElementCursor byteCursor =
        PageUtils::getPageElementCursorForPos(nextBytePosToWriteTo, DEFAULT_PAGE_SIZE);
    if ((byteCursor.posInPage == 0) ||
        ((byteCursor.posInPage + numBytesToAppend - 1) > DEFAULT_PAGE_SIZE)) {
        // Note that if byteCursor.pos is already 0 the next operation keeps the nextBytePos
        // where it is.
        nextBytePosToWriteTo = (fileHandle.getNumPages() * DEFAULT_PAGE_SIZE);
        addNewPageToFileHandle();
    }
}

void OverflowFile::setStringOverflowWithoutLock(const gf_string_t& src, gf_string_t& dst) {
    if (src.len < gf_string_t::SHORT_STR_LENGTH) {
        return;
    } else if (src.len > DEFAULT_PAGE_SIZE) {
        throw RuntimeException(
            StringUtils::getLongStringErrorMessage(src.getAsString().c_str(), DEFAULT_PAGE_SIZE));
    }
    addNewPageIfNecessaryWithoutLock(src.len);
    auto updatedPageInfoAndWALPageFrame =
        createWALVersionOfPageIfNecessaryForElement(nextBytePosToWriteTo, DEFAULT_PAGE_SIZE);
    memcpy(updatedPageInfoAndWALPageFrame.frame + updatedPageInfoAndWALPageFrame.posInPage,
        reinterpret_cast<char*>(src.overflowPtr), src.len);
    TypeUtils::encodeOverflowPtr(dst.overflowPtr, updatedPageInfoAndWALPageFrame.originalPageIdx,
        updatedPageInfoAndWALPageFrame.posInPage);
    nextBytePosToWriteTo += src.len;
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, fileHandle, bufferManager, *wal);
}

void OverflowFile::writeStringOverflowAndUpdateOverflowPtr(
    const gf_string_t& strToWriteFrom, gf_string_t& strToWriteTo) {
    // Note: Currently we are completely serializing threads that want to write to the overflow
    // pages. We can relax this by releasing the lock after each thread 'reserves' their spot
    // to write to in an overflow page by getting a nextBytePosToWriteTo and updating the
    // nextBytePosToWriteTo field. Then they can coordinate by acquiring the page lock of
    // the page that this location corresponds to. However it is still likely that concurrent
    // threads will still try to write to the same page and block each other because we
    // give nextBytePosToWriteTo consecutively.
    lock_t lck{mtx};
    logNewOverflowFileNextBytePosRecordIfNecessaryWithoutLock();
    setStringOverflowWithoutLock(strToWriteFrom, strToWriteTo);
}

void OverflowFile::setListRecursiveIfNestedWithoutLock(
    const gf_list_t& src, gf_list_t& dst, const DataType& dataType) {
    auto elementSize = Types::getDataTypeSize(*dataType.childType);
    if (src.size * elementSize > DEFAULT_PAGE_SIZE) {
        throw RuntimeException(StringUtils::string_format(
            "Maximum num bytes of a LIST is %d. Input list's num bytes is %d.", DEFAULT_PAGE_SIZE,
            src.size * elementSize));
    }
    addNewPageIfNecessaryWithoutLock(src.size * elementSize);
    auto updatedPageInfoAndWALPageFrame =
        createWALVersionOfPageIfNecessaryForElement(nextBytePosToWriteTo, DEFAULT_PAGE_SIZE);
    dst.size = src.size;
    // Copy non-overflow part for elements in the list.
    memcpy(updatedPageInfoAndWALPageFrame.frame + updatedPageInfoAndWALPageFrame.posInPage,
        (uint8_t*)src.overflowPtr, src.size * elementSize);
    nextBytePosToWriteTo += src.size * elementSize;
    TypeUtils::encodeOverflowPtr(dst.overflowPtr, updatedPageInfoAndWALPageFrame.originalPageIdx,
        updatedPageInfoAndWALPageFrame.posInPage);
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, fileHandle, bufferManager, *wal);
    if (dataType.childType->typeID == STRING) {
        // Copy overflow for string elements in the list.
        auto dstListElements = (gf_string_t*)(updatedPageInfoAndWALPageFrame.frame +
                                              updatedPageInfoAndWALPageFrame.posInPage);
        for (auto i = 0u; i < dst.size; i++) {
            setStringOverflowWithoutLock(((gf_string_t*)src.overflowPtr)[i], dstListElements[i]);
        }
    } else if (dataType.childType->typeID == LIST) {
        // Recursively copy overflow for list elements in the list.
        auto dstListElements = (gf_list_t*)(updatedPageInfoAndWALPageFrame.frame +
                                            updatedPageInfoAndWALPageFrame.posInPage);
        for (auto i = 0u; i < dst.size; i++) {
            setListRecursiveIfNestedWithoutLock(
                ((gf_list_t*)src.overflowPtr)[i], dstListElements[i], *dataType.childType);
        }
    }
}

void OverflowFile::writeListOverflowAndUpdateOverflowPtr(
    const gf_list_t& listToWriteFrom, gf_list_t& listToWriteTo, const DataType& dataType) {
    lock_t lck{mtx};
    logNewOverflowFileNextBytePosRecordIfNecessaryWithoutLock();
    setListRecursiveIfNestedWithoutLock(listToWriteFrom, listToWriteTo, dataType);
}

void OverflowFile::logNewOverflowFileNextBytePosRecordIfNecessaryWithoutLock() {
    if (!loggedNewOverflowFileNextBytePosRecord) {
        loggedNewOverflowFileNextBytePosRecord = true;
        wal->logOverflowFileNextBytePosRecord(
            fileHandle.getStorageStructureIDIDForWALRecord(), nextBytePosToWriteTo);
    }
}

} // namespace storage
} // namespace graphflow

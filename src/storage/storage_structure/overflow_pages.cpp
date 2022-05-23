#include "src/storage/include/storage_structure/overflow_pages.h"

#include "src/common/include/overflow_buffer_utils.h"
#include "src/common/include/type_utils.h"

using lock_t = unique_lock<mutex>;

namespace graphflow {
namespace storage {

void OverflowPages::readStringsToVector(ValueVector& valueVector) {
    Transaction tmpTransaction(READ_ONLY, -1);
    readStringsToVector(&tmpTransaction, valueVector);
}

void OverflowPages::readStringsToVector(Transaction* transaction, ValueVector& valueVector) {
    assert(!valueVector.state->isFlat());
    for (auto i = 0u; i < valueVector.state->selectedSize; i++) {
        auto pos = valueVector.state->selectedPositions[i];
        if (!valueVector.isNull(pos)) {
            readStringToVector(transaction, ((gf_string_t*)valueVector.values)[pos],
                valueVector.getOverflowBuffer());
        }
    }
}

void OverflowPages::readListsToVector(ValueVector& valueVector) {
    assert(!valueVector.state->isFlat());
    for (auto i = 0u; i < valueVector.state->selectedSize; i++) {
        auto pos = valueVector.state->selectedPositions[i];
        if (!valueVector.isNull(pos)) {
            readListToVector(((gf_list_t*)valueVector.values)[pos], valueVector.dataType,
                valueVector.getOverflowBuffer());
        }
    }
}

// TODO(Semih/Guodong): This overloaded function is used by Lists, which do not yet support
// updates, and so do not take transactions. To reuse the same readStringToVector function
// with columns, we pass in a readOnly transactions. Later when Lists support updates, this
// function should be removed.
void OverflowPages::readStringToVector(gf_string_t& gfStr, OverflowBuffer& overflowBuffer) {
    Transaction tmpTransaction(READ_ONLY, -1);
    readStringToVector(&tmpTransaction, gfStr, overflowBuffer);
}

void OverflowPages::readStringToVector(
    Transaction* transaction, gf_string_t& gfStr, OverflowBuffer& overflowBuffer) {
    PageElementCursor cursor;
    if (!gf_string_t::isShortString(gfStr.len)) {
        TypeUtils::decodeOverflowPtr(gfStr.overflowPtr, cursor.pageIdx, cursor.pos);
        auto [fileHandleToPin, pageIdxToPin] =
            getFileHandleAndPhysicalPageIdxToPin(transaction, cursor.pageIdx);
        auto frame = bufferManager.pin(*fileHandleToPin, pageIdxToPin);
        OverflowBufferUtils::copyString(
            (char*)(frame + cursor.pos), gfStr.len, gfStr, overflowBuffer);
        bufferManager.unpin(*fileHandleToPin, pageIdxToPin);
    }
}

void OverflowPages::readListToVector(
    gf_list_t& gfList, const DataType& dataType, OverflowBuffer& overflowBuffer) {
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(gfList.overflowPtr, cursor.idx, cursor.offset);
    auto frame = bufferManager.pin(fileHandle, cursor.idx);
    OverflowBufferUtils::copyListNonRecursive(
        frame + cursor.offset, gfList, dataType, overflowBuffer);
    bufferManager.unpin(fileHandle, cursor.idx);
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

string OverflowPages::readString(const gf_string_t& str) {
    if (gf_string_t::isShortString(str.len)) {
        return str.getAsShortString();
    } else {
        PageByteCursor cursor;
        TypeUtils::decodeOverflowPtr(str.overflowPtr, cursor.idx, cursor.offset);
        auto frame = bufferManager.pin(fileHandle, cursor.idx);
        auto retVal = string((char*)(frame + cursor.offset), str.len);
        bufferManager.unpin(fileHandle, cursor.idx);
        return retVal;
    }
}

void OverflowPages::insertNewOverflowPageIfNecessaryWithoutLock(gf_string_t& strToWriteFrom) {
    PageElementCursor byteCursor =
        PageUtils::getPageElementCursorForOffset(nextBytePosToWriteTo, DEFAULT_PAGE_SIZE);
    if ((byteCursor.pos == 0) || ((byteCursor.pos + strToWriteFrom.len - 1) > DEFAULT_PAGE_SIZE)) {
        // Note that if byteCursor.pos is already 0 the next operation keeps the nextBytePos
        // where it is.
        nextBytePosToWriteTo = (fileHandle.getNumPages() * DEFAULT_PAGE_SIZE);
        auto pageIdxInOriginalFile = fileHandle.addNewPage();
        auto pageIdxInWAL = wal->logPageInsertRecord(
            fileHandle.getStorageStructureIDIDForWALRecord(), pageIdxInOriginalFile);
        bufferManager.pinWithoutAcquiringPageLock(
            *wal->fileHandle, pageIdxInWAL, true /* do not read from file */);
        fileHandle.createPageVersionGroupIfNecessary(pageIdxInOriginalFile);
        fileHandle.setUpdatedWALPageVersionNoLock(pageIdxInOriginalFile, pageIdxInWAL);
        bufferManager.setPinnedPageDirty(*wal->fileHandle, pageIdxInWAL);
        bufferManager.unpinWithoutAcquiringPageLock(*wal->fileHandle, pageIdxInWAL);
    }
}

void OverflowPages::writeStringOverflowAndUpdateOverflowPtr(
    gf_string_t& strToWriteTo, gf_string_t& strToWriteFrom) {
    // Note: Currently we are completely serializing threads that want to write to the overflow
    // pages. We can relax this by releasing the lock after each thread 'reserves' their spot
    // to write to in an overflow page by getting a nextBytePosToWriteTo and updating the
    // nextBytePosToWriteTo field. Then they can coordinate by acquiring the page lock of
    // the page that this location corresponds to. However it is still likely that concurrent
    // threads will still try to write to the same page and block each other because we
    // give nextBytePosToWriteTo consecutively.
    lock_t lck{mtx};
    insertNewOverflowPageIfNecessaryWithoutLock(strToWriteFrom);
    auto updatedPageInfoAndWALPageFrame =
        getUpdatePageInfoForElementAndCreateWALVersionOfPageIfNecessary(
            nextBytePosToWriteTo, DEFAULT_PAGE_SIZE);
    memcpy(updatedPageInfoAndWALPageFrame.frame +
               updatedPageInfoAndWALPageFrame.originalPageCursor.pos,
        reinterpret_cast<char*>(strToWriteFrom.overflowPtr), strToWriteFrom.len);
    TypeUtils::encodeOverflowPtr(strToWriteTo.overflowPtr,
        updatedPageInfoAndWALPageFrame.originalPageCursor.pageIdx,
        updatedPageInfoAndWALPageFrame.originalPageCursor.pos);
    nextBytePosToWriteTo += strToWriteFrom.len;
    finishUpdatingPage(updatedPageInfoAndWALPageFrame);
}

vector<Literal> OverflowPages::readList(const gf_list_t& listVal, const DataType& dataType) {
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(listVal.overflowPtr, cursor.idx, cursor.offset);
    auto frame = bufferManager.pin(fileHandle, cursor.idx);
    auto numBytesOfSingleValue = Types::getDataTypeSize(*dataType.childType);
    auto numValuesInList = listVal.size;
    vector<Literal> retLiterals;
    if (dataType.childType->typeID == STRING) {
        for (auto i = 0u; i < numValuesInList; i++) {
            auto gfListVal = *(gf_string_t*)(frame + cursor.offset);
            retLiterals.emplace_back(readString(gfListVal));
            cursor.offset += numBytesOfSingleValue;
        }
    } else if (dataType.childType->typeID == LIST) {
        for (auto i = 0u; i < numValuesInList; i++) {
            auto gfListVal = *(gf_list_t*)(frame + cursor.offset);
            retLiterals.emplace_back(readList(gfListVal, *dataType.childType), *dataType.childType);
            cursor.offset += numBytesOfSingleValue;
        }
    } else {
        for (auto i = 0u; i < numValuesInList; i++) {
            retLiterals.emplace_back(frame + cursor.offset, *dataType.childType);
            cursor.offset += numBytesOfSingleValue;
        }
    }
    bufferManager.unpin(fileHandle, cursor.idx);
    return retLiterals;
}

} // namespace storage
} // namespace graphflow

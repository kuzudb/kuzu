#include "storage/storage_structure/disk_overflow_file.h"

#include "common/in_mem_overflow_buffer_utils.h"
#include "common/string_utils.h"
#include "common/type_utils.h"

using lock_t = std::unique_lock<std::mutex>;

using namespace kuzu::transaction;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

void DiskOverflowFile::pinOverflowPageCache(BMFileHandle* bmFileHandleToPin,
    page_idx_t pageIdxToPin, OverflowPageCache& overflowPageCache) {
    overflowPageCache.frame = bufferManager.pin(*bmFileHandleToPin, pageIdxToPin);
    overflowPageCache.bmFileHandle = bmFileHandleToPin;
    overflowPageCache.pageIdx = pageIdxToPin;
}

void DiskOverflowFile::unpinOverflowPageCache(OverflowPageCache& overflowPageCache) {
    if (overflowPageCache.pageIdx != UINT32_MAX) {
        bufferManager.unpin(*overflowPageCache.bmFileHandle, overflowPageCache.pageIdx);
    }
}

void DiskOverflowFile::scanStrings(TransactionType trxType, ValueVector& valueVector) {
    assert(!valueVector.state->isFlat());
    OverflowPageCache overflowPageCache;
    for (auto i = 0u; i < valueVector.state->selVector->selectedSize; i++) {
        auto pos = valueVector.state->selVector->selectedPositions[i];
        if (valueVector.isNull(pos)) {
            continue;
        }
        lookupString(trxType, ((ku_string_t*)valueVector.getData())[pos],
            valueVector.getOverflowBuffer(), overflowPageCache);
    }
    unpinOverflowPageCache(overflowPageCache);
}

void DiskOverflowFile::lookupString(
    TransactionType trxType, ku_string_t& kuStr, InMemOverflowBuffer& inMemOverflowBuffer) {
    if (ku_string_t::isShortString(kuStr.len)) {
        return;
    }
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(kuStr.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            *fileHandle, cursor.pageIdx, *wal, trxType);
    bufferManager.optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) {
        InMemOverflowBufferUtils::copyString(
            (char*)(frame + cursor.offsetInPage), kuStr.len, kuStr, inMemOverflowBuffer);
    });
}

void DiskOverflowFile::lookupString(TransactionType trxType, ku_string_t& kuStr,
    InMemOverflowBuffer& inMemOverflowBuffer, OverflowPageCache& overflowPageCache) {
    if (ku_string_t::isShortString(kuStr.len)) {
        return;
    }
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(kuStr.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            *fileHandle, cursor.pageIdx, *wal, trxType);
    if (pageIdxToPin != overflowPageCache.pageIdx) { // cache miss
        unpinOverflowPageCache(overflowPageCache);
        pinOverflowPageCache(fileHandleToPin, pageIdxToPin, overflowPageCache);
    }
    InMemOverflowBufferUtils::copyString((char*)(overflowPageCache.frame + cursor.offsetInPage),
        kuStr.len, kuStr, inMemOverflowBuffer);
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
            *fileHandle, cursor.pageIdx, *wal, trxType);
    bufferManager.optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) {
        InMemOverflowBufferUtils::copyListNonRecursive(
            frame + cursor.offsetInPage, kuList, dataType, inMemOverflowBuffer);
    });
    if (dataType.childType->typeID == STRING) {
        auto kuStrings = (ku_string_t*)(kuList.overflowPtr);
        OverflowPageCache overflowPageCache;
        for (auto i = 0u; i < kuList.size; i++) {
            lookupString(trxType, kuStrings[i], inMemOverflowBuffer, overflowPageCache);
        }
        unpinOverflowPageCache(overflowPageCache);
    } else if (dataType.childType->typeID == VAR_LIST) {
        auto kuLists = (ku_list_t*)(kuList.overflowPtr);
        for (auto i = 0u; i < kuList.size; i++) {
            readListToVector(trxType, kuLists[i], *dataType.childType, inMemOverflowBuffer);
        }
    }
}

std::string DiskOverflowFile::readString(TransactionType trxType, const ku_string_t& str) {
    if (ku_string_t::isShortString(str.len)) {
        return str.getAsShortString();
    } else {
        PageByteCursor cursor;
        TypeUtils::decodeOverflowPtr(str.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
        auto [fileHandleToPin, pageIdxToPin] =
            StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
                *fileHandle, cursor.pageIdx, *wal, trxType);
        std::string retVal;
        bufferManager.optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) {
            retVal = std::string((char*)(frame + cursor.offsetInPage), str.len);
        });
        return retVal;
    }
}

std::vector<std::unique_ptr<Value>> DiskOverflowFile::readList(
    TransactionType trxType, const ku_list_t& listVal, const DataType& dataType) {
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(listVal.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            *fileHandle, cursor.pageIdx, *wal, trxType);
    auto numBytesOfSingleValue = Types::getDataTypeSize(*dataType.childType);
    auto numValuesInList = listVal.size;
    std::vector<std::unique_ptr<Value>> retValues;
    bufferManager.optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) -> void {
        readValuesInList(
            trxType, dataType, retValues, numBytesOfSingleValue, numValuesInList, cursor, frame);
    });
    return retValues;
}

void DiskOverflowFile::readValuesInList(transaction::TransactionType trxType,
    const common::DataType& dataType, std::vector<std::unique_ptr<common::Value>>& retValues,
    uint32_t numBytesOfSingleValue, uint64_t numValuesInList, PageByteCursor& cursor,
    uint8_t* frame) {
    if (dataType.childType->typeID == STRING) {
        for (auto i = 0u; i < numValuesInList; i++) {
            auto kuListVal = *(ku_string_t*)(frame + cursor.offsetInPage);
            retValues.push_back(make_unique<Value>(readString(trxType, kuListVal)));
            cursor.offsetInPage += numBytesOfSingleValue;
        }
    } else if (dataType.childType->typeID == VAR_LIST) {
        for (auto i = 0u; i < numValuesInList; i++) {
            auto kuListVal = *(ku_list_t*)(frame + cursor.offsetInPage);
            retValues.push_back(make_unique<Value>(
                *dataType.childType, readList(trxType, kuListVal, *dataType.childType)));
            cursor.offsetInPage += numBytesOfSingleValue;
        }
    } else {
        for (auto i = 0u; i < numValuesInList; i++) {
            retValues.push_back(
                std::make_unique<Value>(*dataType.childType, frame + cursor.offsetInPage));
            cursor.offsetInPage += numBytesOfSingleValue;
        }
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
        addNewPageToFileHandle();
    }
}

void DiskOverflowFile::setStringOverflowWithoutLock(
    const char* srcRawString, uint64_t len, ku_string_t& diskDstString) {
    if (len <= ku_string_t::SHORT_STR_LENGTH) {
        return;
    } else if (len > BufferPoolConstants::PAGE_4KB_SIZE) {
        throw RuntimeException(StringUtils::getLongStringErrorMessage(
            srcRawString, BufferPoolConstants::PAGE_4KB_SIZE));
    }
    addNewPageIfNecessaryWithoutLock(len);
    auto updatedPageInfoAndWALPageFrame = createWALVersionOfPageIfNecessaryForElement(
        nextBytePosToWriteTo, BufferPoolConstants::PAGE_4KB_SIZE);
    memcpy(updatedPageInfoAndWALPageFrame.frame + updatedPageInfoAndWALPageFrame.posInPage,
        srcRawString, len);
    TypeUtils::encodeOverflowPtr(diskDstString.overflowPtr,
        updatedPageInfoAndWALPageFrame.originalPageIdx, updatedPageInfoAndWALPageFrame.posInPage);
    nextBytePosToWriteTo += len;
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, *fileHandle, bufferManager, *wal);
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
    if (inMemSrcList.size * elementSize > BufferPoolConstants::PAGE_4KB_SIZE) {
        throw RuntimeException(StringUtils::string_format(
            "Maximum num bytes of a LIST is %d. Input list's num bytes is %d.",
            BufferPoolConstants::PAGE_4KB_SIZE, inMemSrcList.size * elementSize));
    }
    addNewPageIfNecessaryWithoutLock(inMemSrcList.size * elementSize);
    auto updatedPageInfoAndWALPageFrame = createWALVersionOfPageIfNecessaryForElement(
        nextBytePosToWriteTo, BufferPoolConstants::PAGE_4KB_SIZE);
    diskDstList.size = inMemSrcList.size;
    // Copy non-overflow part for elements in the list.
    memcpy(updatedPageInfoAndWALPageFrame.frame + updatedPageInfoAndWALPageFrame.posInPage,
        (uint8_t*)inMemSrcList.overflowPtr, inMemSrcList.size * elementSize);
    nextBytePosToWriteTo += inMemSrcList.size * elementSize;
    TypeUtils::encodeOverflowPtr(diskDstList.overflowPtr,
        updatedPageInfoAndWALPageFrame.originalPageIdx, updatedPageInfoAndWALPageFrame.posInPage);
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, *fileHandle, bufferManager, *wal);
    if (dataType.childType->typeID == STRING) {
        // Copy overflow for string elements in the list.
        auto dstListElements = (ku_string_t*)(updatedPageInfoAndWALPageFrame.frame +
                                              updatedPageInfoAndWALPageFrame.posInPage);
        for (auto i = 0u; i < diskDstList.size; i++) {
            auto kuString = ((ku_string_t*)inMemSrcList.overflowPtr)[i];
            setStringOverflowWithoutLock(
                (const char*)kuString.overflowPtr, kuString.len, dstListElements[i]);
        }
    } else if (dataType.childType->typeID == VAR_LIST) {
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
        wal->logOverflowFileNextBytePosRecord(storageStructureID, nextBytePosToWriteTo);
    }
}

} // namespace storage
} // namespace kuzu

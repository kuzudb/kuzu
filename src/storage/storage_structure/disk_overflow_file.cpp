#include "storage/storage_structure/disk_overflow_file.h"

#include "common/null_buffer.h"
#include "common/string_utils.h"
#include "common/type_utils.h"

using lock_t = std::unique_lock<std::mutex>;

using namespace kuzu::transaction;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

void DiskOverflowFile::pinOverflowPageCache(BMFileHandle* bmFileHandleToPin,
    page_idx_t pageIdxToPin, OverflowPageCache& overflowPageCache) {
    overflowPageCache.frame = bufferManager->pin(*bmFileHandleToPin, pageIdxToPin);
    overflowPageCache.bmFileHandle = bmFileHandleToPin;
    overflowPageCache.pageIdx = pageIdxToPin;
}

void DiskOverflowFile::unpinOverflowPageCache(OverflowPageCache& overflowPageCache) {
    if (overflowPageCache.pageIdx != UINT32_MAX) {
        bufferManager->unpin(*overflowPageCache.bmFileHandle, overflowPageCache.pageIdx);
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
        lookupString(
            trxType, &valueVector, valueVector.getValue<ku_string_t>(pos), overflowPageCache);
    }
    unpinOverflowPageCache(overflowPageCache);
}

void DiskOverflowFile::lookupString(
    TransactionType trxType, common::ValueVector* vector, common::ku_string_t& dstStr) {
    if (ku_string_t::isShortString(dstStr.len)) {
        return;
    }
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(dstStr.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            *fileHandle, cursor.pageIdx, *wal, trxType);
    bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) {
        StringVector::addString(
            vector, dstStr, (const char*)(frame + cursor.offsetInPage), dstStr.len);
    });
}

void DiskOverflowFile::lookupString(TransactionType trxType, common::ValueVector* vector,
    common::ku_string_t& dstStr, OverflowPageCache& overflowPageCache) {
    if (ku_string_t::isShortString(dstStr.len)) {
        return;
    }
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(dstStr.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            *fileHandle, cursor.pageIdx, *wal, trxType);
    if (pageIdxToPin != overflowPageCache.pageIdx) { // cache miss
        unpinOverflowPageCache(overflowPageCache);
        pinOverflowPageCache(fileHandleToPin, pageIdxToPin, overflowPageCache);
    }
    StringVector::addString(
        vector, dstStr, (const char*)(overflowPageCache.frame + cursor.offsetInPage), dstStr.len);
}

void DiskOverflowFile::readListToVector(
    TransactionType trxType, ku_list_t& kuList, ValueVector* vector, uint64_t pos) {
    auto dataVector = common::ListVector::getDataVector(vector);
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(kuList.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            *fileHandle, cursor.pageIdx, *wal, trxType);
    auto listEntry = common::ListVector::addList(vector, kuList.size);
    vector->setValue(pos, listEntry);
    if (VarListType::getChildType(&vector->dataType)->getLogicalTypeID() ==
        common::LogicalTypeID::VAR_LIST) {
        bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) {
            for (auto i = 0u; i < kuList.size; i++) {
                readListToVector(trxType, ((ku_list_t*)(frame + cursor.offsetInPage))[i],
                    dataVector, listEntry.offset + i);
            }
        });
    } else {
        auto bufferToCopy = common::ListVector::getListValues(vector, listEntry);
        bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) {
            memcpy(bufferToCopy, frame + cursor.offsetInPage,
                dataVector->getNumBytesPerValue() * kuList.size);
        });
        if (dataVector->dataType.getLogicalTypeID() == LogicalTypeID::STRING) {
            auto kuStrings = (ku_string_t*)bufferToCopy;
            OverflowPageCache overflowPageCache;
            for (auto i = 0u; i < kuList.size; i++) {
                lookupString(trxType, dataVector, kuStrings[i], overflowPageCache);
            }
            unpinOverflowPageCache(overflowPageCache);
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
        bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) {
            retVal = std::string((char*)(frame + cursor.offsetInPage), str.len);
        });
        return retVal;
    }
}

std::vector<std::unique_ptr<Value>> DiskOverflowFile::readList(
    TransactionType trxType, const ku_list_t& listVal, const LogicalType& dataType) {
    PageByteCursor cursor;
    TypeUtils::decodeOverflowPtr(listVal.overflowPtr, cursor.pageIdx, cursor.offsetInPage);
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            *fileHandle, cursor.pageIdx, *wal, trxType);
    auto numBytesOfSingleValue =
        storage::StorageUtils::getDataTypeSize(*VarListType::getChildType(&dataType));
    auto numValuesInList = listVal.size;
    std::vector<std::unique_ptr<Value>> retValues;
    bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) -> void {
        readValuesInList(
            trxType, dataType, retValues, numBytesOfSingleValue, numValuesInList, cursor, frame);
    });
    return retValues;
}

void DiskOverflowFile::readValuesInList(transaction::TransactionType trxType,
    const common::LogicalType& dataType, std::vector<std::unique_ptr<common::Value>>& retValues,
    uint32_t numBytesOfSingleValue, uint64_t numValuesInList, PageByteCursor& cursor,
    uint8_t* frame) {
    auto childType = VarListType::getChildType(&dataType);
    if (childType->getLogicalTypeID() == LogicalTypeID::STRING) {
        for (auto i = 0u; i < numValuesInList; i++) {
            auto kuListVal = *(ku_string_t*)(frame + cursor.offsetInPage);
            retValues.push_back(make_unique<Value>(
                LogicalType{LogicalTypeID::STRING}, readString(trxType, kuListVal)));
            cursor.offsetInPage += numBytesOfSingleValue;
        }
    } else if (childType->getLogicalTypeID() == LogicalTypeID::VAR_LIST) {
        for (auto i = 0u; i < numValuesInList; i++) {
            auto kuListVal = *(ku_list_t*)(frame + cursor.offsetInPage);
            retValues.push_back(
                make_unique<Value>(*childType, readList(trxType, kuListVal, *childType)));
            cursor.offsetInPage += numBytesOfSingleValue;
        }
    } else {
        for (auto i = 0u; i < numValuesInList; i++) {
            retValues.push_back(std::make_unique<Value>(*childType, frame + cursor.offsetInPage));
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
    const ku_list_t& inMemSrcList, ku_list_t& diskDstList, const LogicalType& dataType) {
    auto childType = VarListType::getChildType(&dataType);
    auto elementSize = storage::StorageUtils::getDataTypeSize(*childType);
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
    // TODO(Ziyi): Current storage design doesn't support nulls within a list, so we can't read the
    // nullBits from factorizedTable to InMemLists.
    auto listValues = reinterpret_cast<uint8_t*>(inMemSrcList.overflowPtr) +
                      NullBuffer::getNumBytesForNullValues(inMemSrcList.size);
    memcpy(updatedPageInfoAndWALPageFrame.frame + updatedPageInfoAndWALPageFrame.posInPage,
        listValues, inMemSrcList.size * elementSize);
    nextBytePosToWriteTo += inMemSrcList.size * elementSize;
    TypeUtils::encodeOverflowPtr(diskDstList.overflowPtr,
        updatedPageInfoAndWALPageFrame.originalPageIdx, updatedPageInfoAndWALPageFrame.posInPage);
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, *fileHandle, *bufferManager, *wal);
    if (childType->getLogicalTypeID() == LogicalTypeID::STRING) {
        // Copy overflow for string elements in the list.
        auto dstListElements = reinterpret_cast<ku_string_t*>(
            updatedPageInfoAndWALPageFrame.frame + updatedPageInfoAndWALPageFrame.posInPage);
        for (auto i = 0u; i < diskDstList.size; i++) {
            auto kuString = ((ku_string_t*)listValues)[i];
            setStringOverflowWithoutLock(
                (const char*)kuString.overflowPtr, kuString.len, dstListElements[i]);
        }
    } else if (childType->getLogicalTypeID() == LogicalTypeID::VAR_LIST) {
        // Recursively copy overflow for list elements in the list.
        auto dstListElements = reinterpret_cast<ku_list_t*>(
            updatedPageInfoAndWALPageFrame.frame + updatedPageInfoAndWALPageFrame.posInPage);
        for (auto i = 0u; i < diskDstList.size; i++) {
            setListRecursiveIfNestedWithoutLock(
                (reinterpret_cast<ku_list_t*>(listValues))[i], dstListElements[i], *childType);
        }
    }
}

void DiskOverflowFile::writeListOverflowAndUpdateOverflowPtr(
    const ku_list_t& listToWriteFrom, ku_list_t& listToWriteTo, const LogicalType& valueType) {
    lock_t lck{mtx};
    logNewOverflowFileNextBytePosRecordIfNecessaryWithoutLock();
    setListRecursiveIfNestedWithoutLock(listToWriteFrom, listToWriteTo, valueType);
}

void DiskOverflowFile::logNewOverflowFileNextBytePosRecordIfNecessaryWithoutLock() {
    if (!loggedNewOverflowFileNextBytePosRecord) {
        loggedNewOverflowFileNextBytePosRecord = true;
        wal->logOverflowFileNextBytePosRecord(storageStructureID, nextBytePosToWriteTo);
    }
}

} // namespace storage
} // namespace kuzu

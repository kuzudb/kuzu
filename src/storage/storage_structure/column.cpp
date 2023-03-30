#include "storage/storage_structure/column.h"

#include "common/in_mem_overflow_buffer_utils.h"
#include "storage/storage_structure/storage_structure_utils.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

void Column::scan(const common::offset_t* nodeOffsets, size_t size, uint8_t* result) {
    for (auto i = 0u; i < size; ++i) {
        auto nodeOffset = nodeOffsets[i];
        auto cursor = PageUtils::getPageElementCursorForPos(nodeOffset, numElementsPerPage);
        auto [fileHandleToPin, pageIdxToPin] =
            StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
                *fileHandle, cursor.pageIdx, *wal, TransactionType::READ_ONLY);
        bufferManager.optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) -> void {
            auto frameBytesOffset = getElemByteOffset(cursor.elemPosInPage);
            memcpy(result + i * elementSize, frame + frameBytesOffset, elementSize);
        });
    }
}

void Column::read(Transaction* transaction, common::ValueVector* nodeIDVector,
    common::ValueVector* resultVector) {
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[0];
        lookup(transaction, nodeIDVector, resultVector, pos);
    } else if (nodeIDVector->isSequential()) {
        // In sequential read, we fetch start offset regardless of selected position.
        auto startOffset = nodeIDVector->readNodeOffset(0);
        auto pageCursor = PageUtils::getPageElementCursorForPos(startOffset, numElementsPerPage);
        if (nodeIDVector->state->selVector->isUnfiltered()) {
            scan(transaction, resultVector, pageCursor);
        } else {
            scanWithSelState(transaction, resultVector, pageCursor);
        }
    } else {
        for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            lookup(transaction, nodeIDVector, resultVector, pos);
        }
    }
}

void Column::writeValues(
    common::ValueVector* nodeIDVector, common::ValueVector* vectorToWriteFrom) {
    if (nodeIDVector->state->isFlat() && vectorToWriteFrom->state->isFlat()) {
        auto nodeOffset =
            nodeIDVector->readNodeOffset(nodeIDVector->state->selVector->selectedPositions[0]);
        writeValueForSingleNodeIDPosition(nodeOffset, vectorToWriteFrom,
            vectorToWriteFrom->state->selVector->selectedPositions[0]);
    } else if (nodeIDVector->state->isFlat() && !vectorToWriteFrom->state->isFlat()) {
        auto nodeOffset =
            nodeIDVector->readNodeOffset(nodeIDVector->state->selVector->selectedPositions[0]);
        auto lastPos = vectorToWriteFrom->state->selVector->selectedSize - 1;
        writeValueForSingleNodeIDPosition(nodeOffset, vectorToWriteFrom, lastPos);
    } else if (!nodeIDVector->state->isFlat() && vectorToWriteFrom->state->isFlat()) {
        for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
            auto nodeOffset =
                nodeIDVector->readNodeOffset(nodeIDVector->state->selVector->selectedPositions[i]);
            writeValueForSingleNodeIDPosition(nodeOffset, vectorToWriteFrom,
                vectorToWriteFrom->state->selVector->selectedPositions[0]);
        }
    } else if (!nodeIDVector->state->isFlat() && !vectorToWriteFrom->state->isFlat()) {
        for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            auto nodeOffset = nodeIDVector->readNodeOffset(pos);
            writeValueForSingleNodeIDPosition(nodeOffset, vectorToWriteFrom, pos);
        }
    }
}

Value Column::readValueForTestingOnly(offset_t offset) {
    auto cursor = PageUtils::getPageElementCursorForPos(offset, numElementsPerPage);
    Value retVal = Value::createDefaultValue(dataType);
    bufferManager.optimisticRead(*fileHandle, cursor.pageIdx, [&](uint8_t* frame) {
        retVal.copyValueFrom(frame + mapElementPosToByteOffset(cursor.elemPosInPage));
    });
    return retVal;
}

bool Column::isNull(offset_t nodeOffset, Transaction* transaction) {
    auto cursor = PageUtils::getPageElementCursorForPos(nodeOffset, numElementsPerPage);
    auto originalPageIdx = cursor.pageIdx;
    page_idx_t pageIdxInWAL;
    uint8_t* frame;
    bool readFromWALVersionPage = false;
    if (transaction->isWriteTransaction() && fileHandle->hasWALPageGroup(originalPageIdx)) {
        fileHandle->acquireWALPageIdxLock(originalPageIdx);
        if (fileHandle->hasWALPageVersionNoWALPageIdxLock(originalPageIdx)) {
            pageIdxInWAL = fileHandle->getWALPageIdxNoWALPageIdxLock(originalPageIdx);
            readFromWALVersionPage = true;
        } else {
            fileHandle->releaseWALPageIdxLock(originalPageIdx);
        }
    }
    if (readFromWALVersionPage) {
        frame = bufferManager.pin(
            *wal->fileHandle, pageIdxInWAL, BufferManager::PageReadPolicy::READ_PAGE);
    } else {
        frame = bufferManager.pin(
            *fileHandle, originalPageIdx, BufferManager::PageReadPolicy::READ_PAGE);
    }
    auto nullEntries = (uint64_t*)(frame + (elementSize * numElementsPerPage));
    auto isNull = NullMask::isNull(nullEntries, cursor.elemPosInPage);
    if (readFromWALVersionPage) {
        bufferManager.unpin(*wal->fileHandle, pageIdxInWAL);
        fileHandle->releaseWALPageIdxLock(originalPageIdx);
    } else {
        bufferManager.unpin(*fileHandle, originalPageIdx);
    }
    return isNull;
}

void Column::setNodeOffsetToNull(offset_t nodeOffset) {
    auto updatedPageInfoAndWALPageFrame =
        beginUpdatingPageAndWriteOnlyNullBit(nodeOffset, true /* isNull */);
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, *fileHandle, bufferManager, *wal);
}

void Column::lookup(Transaction* transaction, common::ValueVector* nodeIDVector,
    common::ValueVector* resultVector, uint32_t vectorPos) {
    if (nodeIDVector->isNull(vectorPos)) {
        resultVector->setNull(vectorPos, true);
        return;
    }
    auto nodeOffset = nodeIDVector->readNodeOffset(vectorPos);
    auto pageCursor = PageUtils::getPageElementCursorForPos(nodeOffset, numElementsPerPage);
    lookup(transaction, resultVector, vectorPos, pageCursor);
}

void Column::lookup(Transaction* transaction, common::ValueVector* resultVector, uint32_t vectorPos,
    PageElementCursor& cursor) {
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            *fileHandle, cursor.pageIdx, *wal, transaction->getType());
    bufferManager.optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) -> void {
        auto vectorBytesOffset = getElemByteOffset(vectorPos);
        auto frameBytesOffset = getElemByteOffset(cursor.elemPosInPage);
        memcpy(resultVector->getData() + vectorBytesOffset, frame + frameBytesOffset, elementSize);
        readSingleNullBit(resultVector, frame, cursor.elemPosInPage, vectorPos);
    });
}

WALPageIdxPosInPageAndFrame Column::beginUpdatingPage(
    offset_t nodeOffset, common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto isNull = vectorToWriteFrom->isNull(posInVectorToWriteFrom);
    auto walPageInfo = beginUpdatingPageAndWriteOnlyNullBit(nodeOffset, isNull);
    if (!isNull) {
        writeToPage(walPageInfo, vectorToWriteFrom, posInVectorToWriteFrom);
    }
    return walPageInfo;
}

WALPageIdxPosInPageAndFrame Column::beginUpdatingPageAndWriteOnlyNullBit(
    offset_t nodeOffset, bool isNull) {
    auto walPageInfo = createWALVersionOfPageIfNecessaryForElement(nodeOffset, numElementsPerPage);
    setNullBitOfAPosInFrame(walPageInfo.frame, walPageInfo.posInPage, isNull);
    return walPageInfo;
}

void Column::writeValueForSingleNodeIDPosition(
    offset_t nodeOffset, common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto updatedPageInfoAndWALPageFrame =
        beginUpdatingPage(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, *fileHandle, bufferManager, *wal);
}

void StringPropertyColumn::writeValueForSingleNodeIDPosition(
    offset_t nodeOffset, common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto updatedPageInfoAndWALPageFrame =
        beginUpdatingPage(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
    if (!vectorToWriteFrom->isNull(posInVectorToWriteFrom)) {
        auto stringToWriteTo =
            ((ku_string_t*)(updatedPageInfoAndWALPageFrame.frame +
                            mapElementPosToByteOffset(updatedPageInfoAndWALPageFrame.posInPage)));
        auto stringToWriteFrom = vectorToWriteFrom->getValue<ku_string_t>(posInVectorToWriteFrom);
        // If the string we write is a long string, it's overflowPtr is currently pointing to
        // the overflow buffer of vectorToWriteFrom. We need to move it to storage.
        if (!ku_string_t::isShortString(stringToWriteFrom.len)) {
            try {
                diskOverflowFile.writeStringOverflowAndUpdateOverflowPtr(
                    stringToWriteFrom, *stringToWriteTo);
            } catch (RuntimeException& e) {
                // Note: The try catch block is to make sure that we correctly unpin the WAL page
                // and release the page lock of the original page, which was acquired inside
                // `beginUpdatingPage`. Otherwise, we may cause a deadlock in other threads or tasks
                // which requires the same page lock.
                StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
                    updatedPageInfoAndWALPageFrame, *fileHandle, bufferManager, *wal);
                throw e;
            }
        }
    }
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, *fileHandle, bufferManager, *wal);
}

Value StringPropertyColumn::readValueForTestingOnly(offset_t offset) {
    auto cursor = PageUtils::getPageElementCursorForPos(offset, numElementsPerPage);
    ku_string_t kuString;
    bufferManager.optimisticRead(*fileHandle, cursor.pageIdx, [&](uint8_t* frame) -> void {
        memcpy(&kuString, frame + mapElementPosToByteOffset(cursor.elemPosInPage),
            sizeof(ku_string_t));
    });
    return Value(diskOverflowFile.readString(TransactionType::READ_ONLY, kuString));
}

void ListPropertyColumn::writeValueForSingleNodeIDPosition(
    offset_t nodeOffset, common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    assert(vectorToWriteFrom->dataType.typeID == VAR_LIST);
    auto updatedPageInfoAndWALPageFrame =
        beginUpdatingPage(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
    if (!vectorToWriteFrom->isNull(posInVectorToWriteFrom)) {
        auto kuListToWriteTo =
            ((ku_list_t*)(updatedPageInfoAndWALPageFrame.frame +
                          mapElementPosToByteOffset(updatedPageInfoAndWALPageFrame.posInPage)));
        auto kuListToWriteFrom = vectorToWriteFrom->getValue<ku_list_t>(posInVectorToWriteFrom);
        try {
            diskOverflowFile.writeListOverflowAndUpdateOverflowPtr(
                kuListToWriteFrom, *kuListToWriteTo, vectorToWriteFrom->dataType);
        } catch (RuntimeException& e) {
            // Note: The try catch block is to make sure that we correctly unpin the WAL page
            // and release the page lock of the original page, which was acquired inside
            // `beginUpdatingPage`. Otherwise, we may cause a deadlock in other threads or tasks
            // which requires the same page lock.
            StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
                updatedPageInfoAndWALPageFrame, *fileHandle, bufferManager, *wal);
            throw e;
        }
    }
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, *fileHandle, bufferManager, *wal);
}

Value ListPropertyColumn::readValueForTestingOnly(offset_t offset) {
    auto cursor = PageUtils::getPageElementCursorForPos(offset, numElementsPerPage);
    ku_list_t kuList;
    bufferManager.optimisticRead(*fileHandle, cursor.pageIdx, [&](uint8_t* frame) -> void {
        memcpy(&kuList, frame + mapElementPosToByteOffset(cursor.elemPosInPage), sizeof(ku_list_t));
    });
    return Value(dataType, diskOverflowFile.readList(TransactionType::READ_ONLY, kuList, dataType));
}

} // namespace storage
} // namespace kuzu

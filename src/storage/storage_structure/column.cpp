#include "storage/storage_structure/column.h"

#include "common/in_mem_overflow_buffer_utils.h"
#include "storage/storage_structure/storage_structure_utils.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

void Column::read(Transaction* transaction, const std::shared_ptr<ValueVector>& nodeIDVector,
    const std::shared_ptr<ValueVector>& resultVector) {
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

void Column::writeValues(const std::shared_ptr<ValueVector>& nodeIDVector,
    const std::shared_ptr<ValueVector>& vectorToWriteFrom) {
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

Value Column::readValue(offset_t offset) {
    auto cursor = PageUtils::getPageElementCursorForPos(offset, numElementsPerPage);
    auto frame = bufferManager.pin(fileHandle, cursor.pageIdx);
    auto retVal = Value(dataType, frame + mapElementPosToByteOffset(cursor.elemPosInPage));
    bufferManager.unpin(fileHandle, cursor.pageIdx);
    return retVal;
}

bool Column::isNull(offset_t nodeOffset, Transaction* transaction) {
    auto cursor = PageUtils::getPageElementCursorForPos(nodeOffset, numElementsPerPage);
    auto originalPageIdx = cursor.pageIdx;
    fileHandle.acquirePageLock(originalPageIdx, true /* block */);
    auto checkWALVersionOfPage =
        !transaction->isReadOnly() && fileHandle.hasWALPageVersionNoPageLock(originalPageIdx);
    uint8_t* frame;
    page_idx_t pageIdxInWAL;
    if (checkWALVersionOfPage) {
        pageIdxInWAL = fileHandle.getWALPageVersionNoPageLock(originalPageIdx);
        frame = bufferManager.pinWithoutAcquiringPageLock(
            *wal->fileHandle, pageIdxInWAL, false /* read from file */);
    } else {
        frame = bufferManager.pinWithoutAcquiringPageLock(
            fileHandle, originalPageIdx, false /* read from file */);
    }
    auto nullEntries = (uint64_t*)(frame + (elementSize * numElementsPerPage));
    auto isNull = NullMask::isNull(nullEntries, cursor.elemPosInPage);
    if (checkWALVersionOfPage) {
        bufferManager.unpinWithoutAcquiringPageLock(*wal->fileHandle, pageIdxInWAL);
    } else {
        bufferManager.unpinWithoutAcquiringPageLock(fileHandle, originalPageIdx);
    }
    fileHandle.releasePageLock(originalPageIdx);
    return isNull;
}

void Column::setNodeOffsetToNull(offset_t nodeOffset) {
    auto updatedPageInfoAndWALPageFrame =
        beginUpdatingPageAndWriteOnlyNullBit(nodeOffset, true /* isNull */);
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, fileHandle, bufferManager, *wal);
}

void Column::lookup(Transaction* transaction, const std::shared_ptr<ValueVector>& nodeIDVector,
    const std::shared_ptr<ValueVector>& resultVector, uint32_t vectorPos) {
    if (nodeIDVector->isNull(vectorPos)) {
        resultVector->setNull(vectorPos, true);
        return;
    }
    auto nodeOffset = nodeIDVector->readNodeOffset(vectorPos);
    auto pageCursor = PageUtils::getPageElementCursorForPos(nodeOffset, numElementsPerPage);
    lookup(transaction, resultVector, vectorPos, pageCursor);
}

void Column::lookup(Transaction* transaction, const std::shared_ptr<ValueVector>& resultVector,
    uint32_t vectorPos, PageElementCursor& cursor) {
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            fileHandle, cursor.pageIdx, *wal, transaction->getType());
    auto frame = bufferManager.pin(*fileHandleToPin, pageIdxToPin);
    auto vectorBytesOffset = getElemByteOffset(vectorPos);
    auto frameBytesOffset = getElemByteOffset(cursor.elemPosInPage);
    memcpy(resultVector->getData() + vectorBytesOffset, frame + frameBytesOffset, elementSize);
    readSingleNullBit(resultVector, frame, cursor.elemPosInPage, vectorPos);
    bufferManager.unpin(*fileHandleToPin, pageIdxToPin);
}

WALPageIdxPosInPageAndFrame Column::beginUpdatingPage(offset_t nodeOffset,
    const std::shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
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

void Column::writeValueForSingleNodeIDPosition(offset_t nodeOffset,
    const std::shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto updatedPageInfoAndWALPageFrame =
        beginUpdatingPage(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, fileHandle, bufferManager, *wal);
}

void StringPropertyColumn::writeValueForSingleNodeIDPosition(offset_t nodeOffset,
    const std::shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
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
            diskOverflowFile.writeStringOverflowAndUpdateOverflowPtr(
                stringToWriteFrom, *stringToWriteTo);
        }
    }
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, fileHandle, bufferManager, *wal);
}

Value StringPropertyColumn::readValue(offset_t offset) {
    auto cursor = PageUtils::getPageElementCursorForPos(offset, numElementsPerPage);
    ku_string_t kuString;
    auto frame = bufferManager.pin(fileHandle, cursor.pageIdx);
    memcpy(&kuString, frame + mapElementPosToByteOffset(cursor.elemPosInPage), sizeof(ku_string_t));
    bufferManager.unpin(fileHandle, cursor.pageIdx);
    return Value(diskOverflowFile.readString(TransactionType::READ_ONLY, kuString));
}

void ListPropertyColumn::writeValueForSingleNodeIDPosition(offset_t nodeOffset,
    const std::shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    assert(vectorToWriteFrom->dataType.typeID == LIST);
    auto updatedPageInfoAndWALPageFrame =
        beginUpdatingPage(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
    if (!vectorToWriteFrom->isNull(posInVectorToWriteFrom)) {
        auto kuListToWriteTo =
            ((ku_list_t*)(updatedPageInfoAndWALPageFrame.frame +
                          mapElementPosToByteOffset(updatedPageInfoAndWALPageFrame.posInPage)));
        auto kuListToWriteFrom = vectorToWriteFrom->getValue<ku_list_t>(posInVectorToWriteFrom);
        diskOverflowFile.writeListOverflowAndUpdateOverflowPtr(
            kuListToWriteFrom, *kuListToWriteTo, vectorToWriteFrom->dataType);
    }
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, fileHandle, bufferManager, *wal);
}

Value ListPropertyColumn::readValue(offset_t offset) {
    auto cursor = PageUtils::getPageElementCursorForPos(offset, numElementsPerPage);
    ku_list_t kuList;
    auto frame = bufferManager.pin(fileHandle, cursor.pageIdx);
    memcpy(&kuList, frame + mapElementPosToByteOffset(cursor.elemPosInPage), sizeof(ku_list_t));
    bufferManager.unpin(fileHandle, cursor.pageIdx);
    return Value(dataType, diskOverflowFile.readList(TransactionType::READ_ONLY, kuList, dataType));
}

} // namespace storage
} // namespace kuzu

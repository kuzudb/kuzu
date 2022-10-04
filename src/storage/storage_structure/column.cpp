#include "src/storage/storage_structure/include/column.h"

#include "src/common/include/overflow_buffer_utils.h"
#include "src/storage/storage_structure/include/storage_structure_utils.h"

namespace graphflow {
namespace storage {

void Column::read(Transaction* transaction, const shared_ptr<ValueVector>& nodeIDVector,
    const shared_ptr<ValueVector>& resultVector) {
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->getPositionOfCurrIdx();
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
    const shared_ptr<ValueVector>& nodeIDVector, const shared_ptr<ValueVector>& vectorToWriteFrom) {
    if (nodeIDVector->state->isFlat() && vectorToWriteFrom->state->isFlat()) {
        auto nodeOffset = nodeIDVector->readNodeOffset(nodeIDVector->state->getPositionOfCurrIdx());
        writeValueForSingleNodeIDPosition(
            nodeOffset, vectorToWriteFrom, vectorToWriteFrom->state->getPositionOfCurrIdx());
    } else if (nodeIDVector->state->isFlat() && !vectorToWriteFrom->state->isFlat()) {
        auto nodeOffset = nodeIDVector->readNodeOffset(nodeIDVector->state->getPositionOfCurrIdx());
        auto lastPos = vectorToWriteFrom->state->selVector->selectedSize - 1;
        writeValueForSingleNodeIDPosition(nodeOffset, vectorToWriteFrom, lastPos);
    } else if (!nodeIDVector->state->isFlat() && vectorToWriteFrom->state->isFlat()) {
        for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
            auto nodeOffset =
                nodeIDVector->readNodeOffset(nodeIDVector->state->selVector->selectedPositions[i]);
            writeValueForSingleNodeIDPosition(
                nodeOffset, vectorToWriteFrom, vectorToWriteFrom->state->getPositionOfCurrIdx());
        }
    } else if (!nodeIDVector->state->isFlat() && !vectorToWriteFrom->state->isFlat()) {
        for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            auto nodeOffset = nodeIDVector->readNodeOffset(pos);
            writeValueForSingleNodeIDPosition(nodeOffset, vectorToWriteFrom, pos);
        }
    }
}

Literal Column::readValue(node_offset_t offset) {
    auto cursor = PageUtils::getPageElementCursorForPos(offset, numElementsPerPage);
    auto frame = bufferManager.pin(fileHandle, cursor.pageIdx);
    auto retVal = Literal(frame + mapElementPosToByteOffset(cursor.elemPosInPage), dataType);
    bufferManager.unpin(fileHandle, cursor.pageIdx);
    return retVal;
}

// Note this is only used for tests
bool Column::isNull(node_offset_t nodeOffset) {
    auto cursor = PageUtils::getPageElementCursorForPos(nodeOffset, numElementsPerPage);
    auto frame = bufferManager.pin(fileHandle, cursor.pageIdx);
    auto nullEntries = (uint64_t*)(frame + (elementSize * numElementsPerPage));
    auto isNull = NullMask::isNull(nullEntries, cursor.elemPosInPage);
    bufferManager.unpin(fileHandle, cursor.pageIdx);
    return isNull;
}

void Column::lookup(Transaction* transaction, const shared_ptr<ValueVector>& nodeIDVector,
    const shared_ptr<ValueVector>& resultVector, uint32_t vectorPos) {
    if (nodeIDVector->isNull(vectorPos)) {
        resultVector->setNull(vectorPos, true);
        return;
    }
    auto nodeOffset = nodeIDVector->readNodeOffset(vectorPos);
    auto pageCursor = PageUtils::getPageElementCursorForPos(nodeOffset, numElementsPerPage);
    lookup(transaction, resultVector, vectorPos, pageCursor);
}

void Column::lookup(Transaction* transaction, const shared_ptr<ValueVector>& resultVector,
    uint32_t vectorPos, PageElementCursor& cursor) {
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            fileHandle, cursor.pageIdx, *wal, transaction->isReadOnly());
    auto frame = bufferManager.pin(*fileHandleToPin, pageIdxToPin);
    auto vectorBytesOffset = vectorPos * elementSize;
    auto frameBytesOffset = cursor.elemPosInPage * elementSize;
    memcpy(resultVector->values + vectorBytesOffset, frame + frameBytesOffset, elementSize);
    readSingleNullBit(resultVector, frame, cursor.elemPosInPage, vectorPos);
    bufferManager.unpin(*fileHandleToPin, pageIdxToPin);
}

WALPageIdxPosInPageAndFrame Column::beginUpdatingPage(node_offset_t nodeOffset,
    const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto walPageInfo = createWALVersionOfPageIfNecessaryForElement(nodeOffset, numElementsPerPage);
    memcpy(walPageInfo.frame + mapElementPosToByteOffset(walPageInfo.posInPage),
        vectorToWriteFrom->values + posInVectorToWriteFrom * elementSize, elementSize);
    setNullBitOfAPosInFrame(walPageInfo.frame, walPageInfo.posInPage,
        vectorToWriteFrom->isNull(posInVectorToWriteFrom));
    return walPageInfo;
}

void Column::writeValueForSingleNodeIDPosition(node_offset_t nodeOffset,
    const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto updatedPageInfoAndWALPageFrame =
        beginUpdatingPage(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, fileHandle, bufferManager, *wal);
}

void StringPropertyColumn::writeValueForSingleNodeIDPosition(node_offset_t nodeOffset,
    const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto updatedPageInfoAndWALPageFrame =
        beginUpdatingPage(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
    if (!vectorToWriteFrom->isNull(posInVectorToWriteFrom)) {
        auto stringToWriteTo =
            ((gf_string_t*)(updatedPageInfoAndWALPageFrame.frame +
                            mapElementPosToByteOffset(updatedPageInfoAndWALPageFrame.posInPage)));
        auto stringToWriteFrom = ((gf_string_t*)vectorToWriteFrom->values)[posInVectorToWriteFrom];
        // If the string we write is a long string, it's overflowPtr is currently pointing to the
        // overflow buffer of vectorToWriteFrom. We need to move it to storage.
        if (!gf_string_t::isShortString(stringToWriteFrom.len)) {
            overflowFile.writeStringOverflowAndUpdateOverflowPtr(
                stringToWriteFrom, *stringToWriteTo);
        }
    }
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, fileHandle, bufferManager, *wal);
}

Literal StringPropertyColumn::readValue(node_offset_t offset) {
    auto cursor = PageUtils::getPageElementCursorForPos(offset, numElementsPerPage);
    gf_string_t gfString;
    auto frame = bufferManager.pin(fileHandle, cursor.pageIdx);
    memcpy(&gfString, frame + mapElementPosToByteOffset(cursor.elemPosInPage), sizeof(gf_string_t));
    bufferManager.unpin(fileHandle, cursor.pageIdx);
    return Literal(overflowFile.readString(gfString));
}

void ListPropertyColumn::writeValueForSingleNodeIDPosition(node_offset_t nodeOffset,
    const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    assert(vectorToWriteFrom->dataType.typeID == LIST);
    auto updatedPageInfoAndWALPageFrame =
        beginUpdatingPage(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
    if (!vectorToWriteFrom->isNull(posInVectorToWriteFrom)) {
        auto gfListToWriteTo =
            ((gf_list_t*)(updatedPageInfoAndWALPageFrame.frame +
                          mapElementPosToByteOffset(updatedPageInfoAndWALPageFrame.posInPage)));
        auto gfListToWriteFrom = ((gf_list_t*)vectorToWriteFrom->values)[posInVectorToWriteFrom];
        listOverflowPages.writeListOverflowAndUpdateOverflowPtr(
            gfListToWriteFrom, *gfListToWriteTo, vectorToWriteFrom->dataType);
    }
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, fileHandle, bufferManager, *wal);
}

Literal ListPropertyColumn::readValue(node_offset_t offset) {
    auto cursor = PageUtils::getPageElementCursorForPos(offset, numElementsPerPage);
    gf_list_t gfList;
    auto frame = bufferManager.pin(fileHandle, cursor.pageIdx);
    memcpy(&gfList, frame + mapElementPosToByteOffset(cursor.elemPosInPage), sizeof(gf_list_t));
    bufferManager.unpin(fileHandle, cursor.pageIdx);
    return Literal(listOverflowPages.readList(gfList, dataType), dataType);
}

} // namespace storage
} // namespace graphflow

#include "src/storage/include/storage_structure/column.h"

namespace graphflow {
namespace storage {

void Column::readValues(Transaction* transaction, const shared_ptr<ValueVector>& nodeIDVector,
    const shared_ptr<ValueVector>& resultVector) {
    assert(nodeIDVector->dataType.typeID == NODE_ID);
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->getPositionOfCurrIdx();
        readForSingleNodeIDPosition(transaction, pos, nodeIDVector, resultVector);
    } else if (nodeIDVector->isSequential()) {
        // In sequential read, we fetch start offset regardless of selected position.
        auto startOffset = nodeIDVector->readNodeOffset(0);
        auto pageCursor = PageUtils::getPageElementCursorForOffset(startOffset, numElementsPerPage);
        // no logical-physical page mapping is required for columns
        auto logicalToPhysicalPageMapper = [](uint32_t i) { return i; };
        if (nodeIDVector->state->isUnfiltered()) {
            readValuesSequential(
                transaction, resultVector, pageCursor, logicalToPhysicalPageMapper);
        } else {
            readValuesSequentialWithSelState(
                transaction, resultVector, pageCursor, logicalToPhysicalPageMapper);
        }
    } else {
        for (auto i = 0ul; i < nodeIDVector->state->selectedSize; i++) {
            auto pos = nodeIDVector->state->selectedPositions[i];
            readForSingleNodeIDPosition(transaction, pos, nodeIDVector, resultVector);
        }
    }
}

Literal Column::readValue(node_offset_t offset) {
    auto cursor = PageUtils::getPageElementCursorForOffset(offset, numElementsPerPage);
    auto frame = bufferManager.pin(fileHandle, cursor.pageIdx);
    auto retVal = Literal(frame + mapElementPosToByteOffset(cursor.posInPage), dataType);
    bufferManager.unpin(fileHandle, cursor.pageIdx);
    return retVal;
}

// Note this is only used for tests
bool Column::isNull(node_offset_t nodeOffset) {
    auto cursor = PageUtils::getPageElementCursorForOffset(nodeOffset, numElementsPerPage);
    auto frame = bufferManager.pin(fileHandle, cursor.pageIdx);
    auto NULLByteAndByteLevelOffset =
        PageUtils::getNULLByteAndByteLevelOffsetPair(frame, cursor.posInPage);
    bufferManager.unpin(fileHandle, cursor.pageIdx);
    return isNullFromNULLByte(NULLByteAndByteLevelOffset.first, NULLByteAndByteLevelOffset.second);
}

void Column::writeValues(
    const shared_ptr<ValueVector>& nodeIDVector, const shared_ptr<ValueVector>& vectorToWriteFrom) {
    if (nodeIDVector->state->isFlat() && vectorToWriteFrom->state->isFlat()) {
        auto nodeOffset = nodeIDVector->readNodeOffset(nodeIDVector->state->getPositionOfCurrIdx());
        writeValueForSingleNodeIDPosition(
            nodeOffset, vectorToWriteFrom, vectorToWriteFrom->state->getPositionOfCurrIdx());
    } else if (nodeIDVector->state->isFlat() && !vectorToWriteFrom->state->isFlat()) {
        auto nodeOffset = nodeIDVector->readNodeOffset(nodeIDVector->state->getPositionOfCurrIdx());
        auto lastPos = vectorToWriteFrom->state->selectedSize - 1;
        writeValueForSingleNodeIDPosition(nodeOffset, vectorToWriteFrom, lastPos);
    } else if (!nodeIDVector->state->isFlat() && vectorToWriteFrom->state->isFlat()) {
        for (auto i = 0u; i < nodeIDVector->state->selectedSize; ++i) {
            auto nodeOffset =
                nodeIDVector->readNodeOffset(nodeIDVector->state->selectedPositions[i]);
            writeValueForSingleNodeIDPosition(
                nodeOffset, vectorToWriteFrom, vectorToWriteFrom->state->getPositionOfCurrIdx());
        }
    } else if (!nodeIDVector->state->isFlat() && !vectorToWriteFrom->state->isFlat()) {
        for (auto i = 0u; i < nodeIDVector->state->selectedSize; ++i) {
            auto pos = nodeIDVector->state->selectedPositions[i];
            auto nodeOffset = nodeIDVector->readNodeOffset(pos);
            writeValueForSingleNodeIDPosition(nodeOffset, vectorToWriteFrom, pos);
        }
    }
}

UpdatedPageInfoAndWALPageFrame Column::beginUpdatingPage(node_offset_t nodeOffset,
    const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto updatedPageInfoAndWALPageFrame =
        getUpdatePageInfoForElementAndCreateWALVersionOfPageIfNecessary(
            nodeOffset, numElementsPerPage);
    memcpy(
        updatedPageInfoAndWALPageFrame.frame +
            mapElementPosToByteOffset(updatedPageInfoAndWALPageFrame.originalPageCursor.posInPage),
        vectorToWriteFrom->values + posInVectorToWriteFrom * elementSize, elementSize);
    setNullBitOfAPosInFrame(updatedPageInfoAndWALPageFrame.frame,
        updatedPageInfoAndWALPageFrame.originalPageCursor.posInPage,
        vectorToWriteFrom->isNull(posInVectorToWriteFrom));
    return UpdatedPageInfoAndWALPageFrame(updatedPageInfoAndWALPageFrame.originalPageCursor,
        updatedPageInfoAndWALPageFrame.pageIdxInWAL, updatedPageInfoAndWALPageFrame.frame);
}

void Column::writeValueForSingleNodeIDPosition(node_offset_t nodeOffset,
    const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto updatedPageInfoAndWALPageFrame =
        beginUpdatingPage(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
    finishUpdatingPage(updatedPageInfoAndWALPageFrame);
}

void Column::readForSingleNodeIDPosition(Transaction* transaction, uint32_t pos,
    const shared_ptr<ValueVector>& nodeIDVector, const shared_ptr<ValueVector>& resultVector) {
    if (nodeIDVector->isNull(pos)) {
        resultVector->setNull(pos, true);
        return;
    }
    auto pageCursor = PageUtils::getPageElementCursorForOffset(
        nodeIDVector->readNodeOffset(pos), numElementsPerPage);
    auto [fileHandleToPin, pageIdxToPin] =
        getFileHandleAndPhysicalPageIdxToPin(transaction, pageCursor.pageIdx);
    auto frame = bufferManager.pin(*fileHandleToPin, pageIdxToPin);
    memcpy(resultVector->values + pos * elementSize,
        frame + mapElementPosToByteOffset(pageCursor.posInPage), elementSize);
    setNULLBitsForAPos(resultVector, frame, pageCursor.posInPage, pos);
    bufferManager.unpin(*fileHandleToPin, pageIdxToPin);
}

void StringPropertyColumn::writeValueForSingleNodeIDPosition(node_offset_t nodeOffset,
    const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto updatedPageInfoAndWALPageFrame =
        beginUpdatingPage(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
    if (!vectorToWriteFrom->isNull(posInVectorToWriteFrom)) {
        auto stringToWriteToPtr =
            ((gf_string_t*)(updatedPageInfoAndWALPageFrame.frame +
                            mapElementPosToByteOffset(
                                updatedPageInfoAndWALPageFrame.originalPageCursor.posInPage)));
        auto stringToWriteFrom = ((gf_string_t*)vectorToWriteFrom->values)[posInVectorToWriteFrom];
        if (stringToWriteFrom.len > DEFAULT_PAGE_SIZE) {
            throw RuntimeException(StringUtils::getLongStringErrorMessage(
                stringToWriteFrom.getAsString().c_str(), DEFAULT_PAGE_SIZE));
        }
        // If the string we write is a long string, it's overflowptr is current pointing to the
        // overflow buffer of vectorToWriteFrom. We need to move it to storage.
        if (!gf_string_t::isShortString(stringToWriteFrom.len)) {
            stringOverflowPages.writeStringOverflowAndUpdateOverflowPtr(
                *stringToWriteToPtr, stringToWriteFrom);
        }
    }
    finishUpdatingPage(updatedPageInfoAndWALPageFrame);
}

void StringPropertyColumn::readValues(Transaction* transaction,
    const shared_ptr<ValueVector>& nodeIDVector, const shared_ptr<ValueVector>& valueVector) {
    Column::readValues(transaction, nodeIDVector, valueVector);
    stringOverflowPages.readStringsToVector(transaction, *valueVector);
}

void ListPropertyColumn::readValues(Transaction* transaction,
    const shared_ptr<ValueVector>& nodeIDVector, const shared_ptr<ValueVector>& valueVector) {
    Column::readValues(transaction, nodeIDVector, valueVector);
    listOverflowPages.readListsToVector(*valueVector);
}

Literal StringPropertyColumn::readValue(node_offset_t offset) {
    auto cursor = PageUtils::getPageElementCursorForOffset(offset, numElementsPerPage);
    gf_string_t gfString;
    auto frame = bufferManager.pin(fileHandle, cursor.pageIdx);
    memcpy(&gfString, frame + mapElementPosToByteOffset(cursor.posInPage), sizeof(gf_string_t));
    bufferManager.unpin(fileHandle, cursor.pageIdx);
    return Literal(stringOverflowPages.readString(gfString));
}

Literal ListPropertyColumn::readValue(node_offset_t offset) {
    auto cursor = PageUtils::getPageElementCursorForOffset(offset, numElementsPerPage);
    gf_list_t gfList;
    auto frame = bufferManager.pin(fileHandle, cursor.pageIdx);
    memcpy(&gfList, frame + mapElementPosToByteOffset(cursor.posInPage), sizeof(gf_list_t));
    bufferManager.unpin(fileHandle, cursor.pageIdx);
    return Literal(listOverflowPages.readList(gfList, dataType), dataType);
}

void AdjColumn::readForSingleNodeIDPosition(Transaction* transaction, uint32_t pos,
    const shared_ptr<ValueVector>& nodeIDVector, const shared_ptr<ValueVector>& resultVector) {
    if (nodeIDVector->isNull(pos)) {
        resultVector->setNull(pos, true);
        return;
    }
    auto pageCursor = PageUtils::getPageElementCursorForOffset(
        nodeIDVector->readNodeOffset(pos), numElementsPerPage);
    readNodeIDsFromAPageBySequentialCopy(resultVector, pos, pageCursor.pageIdx,
        pageCursor.posInPage, 1 /* numValuesToCopy */, nodeIDCompressionScheme,
        false /*isAdjLists*/);
}

} // namespace storage
} // namespace graphflow

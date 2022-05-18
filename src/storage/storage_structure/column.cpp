#include "src/storage/include/storage_structure/column.h"

namespace graphflow {
namespace storage {

void Column::readValues(Transaction* transaction, const shared_ptr<ValueVector>& nodeIDVector,
    const shared_ptr<ValueVector>& valueVector) {
    assert(nodeIDVector->dataType.typeID == NODE);
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->getPositionOfCurrIdx();
        readForSingleNodeIDPosition(transaction, pos, nodeIDVector, valueVector);
    } else {
        for (auto i = 0ul; i < nodeIDVector->state->selectedSize; i++) {
            auto pos = nodeIDVector->state->selectedPositions[i];
            readForSingleNodeIDPosition(transaction, pos, nodeIDVector, valueVector);
        }
    }
}

Literal Column::readValue(node_offset_t offset) {
    auto cursor = PageUtils::getPageElementCursorForOffset(offset, numElementsPerPage);
    auto frame = bufferManager.pin(fileHandle, cursor.idx);
    auto retVal = Literal(frame + mapElementPosToByteOffset(cursor.pos), dataType);
    bufferManager.unpin(fileHandle, cursor.idx);
    return retVal;
}

// Note this is only used for tests
bool Column::isNull(node_offset_t nodeOffset) {
    auto cursor = PageUtils::getPageElementCursorForOffset(nodeOffset, numElementsPerPage);
    auto frame = bufferManager.pin(fileHandle, cursor.idx);
    auto NULLByteAndByteLevelOffset =
        PageUtils::getNULLByteAndByteLevelOffsetPair(frame, cursor.pos);
    bufferManager.unpin(fileHandle, cursor.idx);
    return isNullFromNULLByte(NULLByteAndByteLevelOffset.first, NULLByteAndByteLevelOffset.second);
}

void Column::writeValues(Transaction* transaction, const shared_ptr<ValueVector>& nodeIDVector,
    const shared_ptr<ValueVector>& vectorToWriteFrom) {
    if (nodeIDVector->state->isFlat() && vectorToWriteFrom->state->isFlat()) {
        auto nodeOffset = nodeIDVector->readNodeOffset(nodeIDVector->state->getPositionOfCurrIdx());
        writeValueForSingleNodeIDPosition(transaction, nodeOffset, vectorToWriteFrom,
            vectorToWriteFrom->state->getPositionOfCurrIdx());
    } else if (nodeIDVector->state->isFlat() && !vectorToWriteFrom->state->isFlat()) {
        auto nodeOffset = nodeIDVector->readNodeOffset(nodeIDVector->state->getPositionOfCurrIdx());
        auto lastPos = vectorToWriteFrom->state->selectedSize - 1;
        writeValueForSingleNodeIDPosition(transaction, nodeOffset, vectorToWriteFrom, lastPos);
    } else if (!nodeIDVector->state->isFlat() && vectorToWriteFrom->state->isFlat()) {
        for (auto i = 0u; i < nodeIDVector->state->selectedSize; ++i) {
            auto nodeOffset =
                nodeIDVector->readNodeOffset(nodeIDVector->state->selectedPositions[i]);
            writeValueForSingleNodeIDPosition(transaction, nodeOffset, vectorToWriteFrom,
                vectorToWriteFrom->state->getPositionOfCurrIdx());
        }
    } else if (!nodeIDVector->state->isFlat() && !vectorToWriteFrom->state->isFlat()) {
        for (auto i = 0u; i < nodeIDVector->state->selectedSize; ++i) {
            auto pos = nodeIDVector->state->selectedPositions[i];
            auto nodeOffset = nodeIDVector->readNodeOffset(pos);
            writeValueForSingleNodeIDPosition(transaction, nodeOffset, vectorToWriteFrom, pos);
        }
    }
}

void Column::writeValueForSingleNodeIDPosition(Transaction* transaction, node_offset_t nodeOffset,
    const shared_ptr<ValueVector>& vectorToWriteFrom, uint32_t resultVectorPos) {
    auto cursor = PageUtils::getPageElementCursorForOffset(nodeOffset, numElementsPerPage);
    pageVersionInfo.acquireLockForWritingToPage(cursor.idx);
    uint32_t pageIdxInWAL;
    uint8_t* frame;
    if (pageVersionInfo.hasUpdatedWALPageVersionNoLock(cursor.idx)) {
        pageIdxInWAL = pageVersionInfo.getUpdatedWALPageVersionNoLock(cursor.idx);
        frame = bufferManager.pin(*wal->fileHandle, pageIdxInWAL);
    } else {
        pageIdxInWAL = wal->logStructuredNodePropertyPageRecord(
            property.label, property.propertyID, cursor.idx /* pageIdxInOriginalFile */);
        frame = bufferManager.pinWithoutReadingFromFile(*wal->fileHandle, pageIdxInWAL);
        uint8_t* originalFrame = bufferManager.pin(fileHandle, cursor.idx);
        // Note: This logic only works for db files with DEFAULT_PAGE_SIZEs.
        memcpy(frame, originalFrame, DEFAULT_PAGE_SIZE);
        bufferManager.unpin(fileHandle, cursor.idx);
        pageVersionInfo.setUpdatedWALPageVersionNoLock(
            cursor.idx /* pageIdxInOriginalFile */, pageIdxInWAL);
    }
    bufferManager.setPinnedPageDirty(*wal->fileHandle, pageIdxInWAL);
    memcpy(frame + mapElementPosToByteOffset(cursor.pos),
        vectorToWriteFrom->values + resultVectorPos * elementSize, elementSize);
    setNullBitOfAPosInFrame(frame, cursor.pos, vectorToWriteFrom->isNull(resultVectorPos));
    pageVersionInfo.releaseLock(cursor.idx);
    bufferManager.unpin(*wal->fileHandle, pageIdxInWAL);
}

void Column::readForSingleNodeIDPosition(Transaction* transaction, uint32_t pos,
    const shared_ptr<ValueVector>& nodeIDVector, const shared_ptr<ValueVector>& resultVector) {
    if (nodeIDVector->isNull(pos)) {
        resultVector->setNull(pos, true);
        return;
    }
    auto pageCursor = PageUtils::getPageElementCursorForOffset(
        nodeIDVector->readNodeOffset(pos), numElementsPerPage);
    // Note: The below code assumes that we do not have to acquire a lock on the page lock inside
    // pageVersionInfo if the transaction is write only. That is because we assume that we do not
    // have concurrent pipelines P1, P2, where P1 reads from and P2 writes to original DB files.
    FileHandle& fileHandleToPin =
        transaction->isReadOnly() ||
                !pageVersionInfo.hasUpdatedWALPageVersionNoLock(pageCursor.idx) ?
            fileHandle :
            *wal->fileHandle;
    uint64_t pageIdxToPin =
        transaction->isReadOnly() ||
                !pageVersionInfo.hasUpdatedWALPageVersionNoLock(pageCursor.idx) ?
            pageCursor.idx :
            pageVersionInfo.getUpdatedWALPageVersionNoLock(pageCursor.idx);
    auto frame = bufferManager.pin(fileHandleToPin, pageIdxToPin);

    memcpy(resultVector->values + pos * elementSize,
        frame + mapElementPosToByteOffset(pageCursor.pos), elementSize);
    setNULLBitsForAPos(resultVector, frame, pageCursor.pos, pos);
    bufferManager.unpin(fileHandleToPin, pageIdxToPin);
}

void StringPropertyColumn::readValues(Transaction* transaction,
    const shared_ptr<ValueVector>& nodeIDVector, const shared_ptr<ValueVector>& valueVector) {
    Column::readValues(transaction, nodeIDVector, valueVector);
    stringOverflowPages.readStringsToVector(*valueVector);
}

void ListPropertyColumn::readValues(Transaction* transaction,
    const shared_ptr<ValueVector>& nodeIDVector, const shared_ptr<ValueVector>& valueVector) {
    Column::readValues(transaction, nodeIDVector, valueVector);
    listOverflowPages.readListsToVector(*valueVector);
}

Literal StringPropertyColumn::readValue(node_offset_t offset) {
    auto cursor = PageUtils::getPageElementCursorForOffset(offset, numElementsPerPage);
    gf_string_t gfString;
    auto frame = bufferManager.pin(fileHandle, cursor.idx);
    memcpy(&gfString, frame + mapElementPosToByteOffset(cursor.pos), sizeof(gf_string_t));
    bufferManager.unpin(fileHandle, cursor.idx);
    return Literal(stringOverflowPages.readString(gfString));
}

Literal ListPropertyColumn::readValue(node_offset_t offset) {
    auto cursor = PageUtils::getPageElementCursorForOffset(offset, numElementsPerPage);
    gf_list_t gfList;
    auto frame = bufferManager.pin(fileHandle, cursor.idx);
    memcpy(&gfList, frame + mapElementPosToByteOffset(cursor.pos), sizeof(gf_list_t));
    bufferManager.unpin(fileHandle, cursor.idx);
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
    readNodeIDsFromAPage(resultVector, pos, pageCursor.idx, pageCursor.pos, 1 /* numValuesToCopy */,
        nodeIDCompressionScheme, false /*isAdjLists*/);
}

} // namespace storage
} // namespace graphflow

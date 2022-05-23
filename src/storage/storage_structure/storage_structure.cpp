#include "src/storage/include/storage_structure/storage_structure.h"

#include "src/common/include/utils.h"

namespace graphflow {
namespace storage {

pair<FileHandle*, uint64_t> StorageStructure::getFileHandleAndPageIdxToPin(
    Transaction* transaction, PageElementCursor pageCursor) {
    if (transaction->isReadOnly() ||
        !fileHandle.hasUpdatedWALPageVersionNoLock(pageCursor.pageIdx)) {
        return make_pair(&fileHandle, pageCursor.pageIdx);
    } else {
        return make_pair(
            wal->fileHandle.get(), fileHandle.getUpdatedWALPageVersionNoLock(pageCursor.pageIdx));
    }
}

UpdatedPageInfoAndWALPageFrame
StorageStructure::getUpdatePageInfoForElementAndCreateWALVersionOfPageIfNecessary(
    uint64_t elementOffset, uint64_t numElementsPerPage) {
    auto originalPageCursor =
        PageUtils::getPageElementCursorForOffset(elementOffset, numElementsPerPage);
    fileHandle.createPageVersionGroupIfNecessary(originalPageCursor.pageIdx);
    fileHandle.acquirePageLock(originalPageCursor.pageIdx, true /* block */);
    uint32_t pageIdxInWAL;
    uint8_t* frame;
    if (fileHandle.hasUpdatedWALPageVersionNoLock(originalPageCursor.pageIdx)) {
        pageIdxInWAL = fileHandle.getUpdatedWALPageVersionNoLock(originalPageCursor.pageIdx);
        frame = bufferManager.pinWithoutAcquiringPageLock(
            *wal->fileHandle, pageIdxInWAL, false /* read from file */);
    } else {
        pageIdxInWAL = wal->logPageUpdateRecord(fileHandle.getStorageStructureIDIDForWALRecord(),
            originalPageCursor.pageIdx /* pageIdxInOriginalFile */);
        frame = bufferManager.pinWithoutAcquiringPageLock(
            *wal->fileHandle, pageIdxInWAL, true /* do not read from file */);
        uint8_t* originalFrame = bufferManager.pinWithoutAcquiringPageLock(
            fileHandle, originalPageCursor.pageIdx, false /* read from file */);
        // Note: This logic only works for db files with DEFAULT_PAGE_SIZEs.
        memcpy(frame, originalFrame, DEFAULT_PAGE_SIZE);
        bufferManager.unpinWithoutAcquiringPageLock(fileHandle, originalPageCursor.pageIdx);
        fileHandle.setUpdatedWALPageVersionNoLock(
            originalPageCursor.pageIdx /* pageIdxInOriginalFile */, pageIdxInWAL);
        bufferManager.setPinnedPageDirty(*wal->fileHandle, pageIdxInWAL);
    }
    return UpdatedPageInfoAndWALPageFrame(originalPageCursor, pageIdxInWAL, frame);
}

void StorageStructure::finishUpdatingPage(
    UpdatedPageInfoAndWALPageFrame& updatedPageInfoAndWALPageFrame) {
    bufferManager.unpinWithoutAcquiringPageLock(
        *wal->fileHandle, updatedPageInfoAndWALPageFrame.pageIdxInWAL);
    fileHandle.releasePageLock(updatedPageInfoAndWALPageFrame.originalPageCursor.pageIdx);
}

BaseColumnOrList::BaseColumnOrList(const StorageStructureIDAndFName storageStructureIDAndFName,
    const DataType& dataType, const size_t& elementSize, BufferManager& bufferManager,
    bool hasNULLBytes, bool isInMemory, WAL* wal)
    : StorageStructure(storageStructureIDAndFName, bufferManager, isInMemory, wal),
      dataType{dataType}, elementSize{elementSize} {
    numElementsPerPage = hasNULLBytes ?
                             PageUtils::getNumElementsInAPageWithNULLBytes(elementSize) :
                             PageUtils::getNumElementsInAPageWithoutNULLBytes(elementSize);
}

void BaseColumnOrList::readBySequentialCopy(const shared_ptr<ValueVector>& valueVector,
    uint64_t sizeLeftToCopy, PageElementCursor& cursor,
    const function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper) {
    auto values = valueVector->values;
    auto offsetInVector = 0;
    while (sizeLeftToCopy) {
        auto physicalPageIdx = logicalToPhysicalPageMapper(cursor.pageIdx);
        auto sizeToCopyInPage =
            min((uint64_t)(numElementsPerPage - cursor.pos) * elementSize, sizeLeftToCopy);
        auto numValuesToCopyInPage = sizeToCopyInPage / elementSize;
        auto frame = bufferManager.pin(fileHandle, physicalPageIdx);
        memcpy(values, frame + mapElementPosToByteOffset(cursor.pos), sizeToCopyInPage);
        setNULLBitsForRange(valueVector, frame, cursor.pos, offsetInVector, numValuesToCopyInPage);
        bufferManager.unpin(fileHandle, physicalPageIdx);
        values += sizeToCopyInPage;
        sizeLeftToCopy -= sizeToCopyInPage;
        offsetInVector += numValuesToCopyInPage;
        cursor.pos = 0;
        cursor.pageIdx++;
    }
}

void BaseColumnOrList::readNodeIDsFromSequentialPages(const shared_ptr<ValueVector>& valueVector,
    PageElementCursor& cursor, const std::function<uint32_t(uint32_t)>& logicalToPhysicalPageMapper,
    NodeIDCompressionScheme compressionScheme, bool isAdjLists) {
    auto numValuesToCopy = valueVector->state->originalSize;
    auto posInVector = 0;
    while (numValuesToCopy > 0) {
        auto numValuesToCopyInPage =
            min(numValuesToCopy, (uint64_t)(numElementsPerPage - cursor.pos));
        auto physicalPageId = logicalToPhysicalPageMapper(cursor.pageIdx);
        readNodeIDsFromAPage(valueVector, posInVector, physicalPageId, cursor.pos,
            numValuesToCopyInPage, compressionScheme, isAdjLists);
        cursor.pageIdx++;
        cursor.pos = 0;
        numValuesToCopy -= numValuesToCopyInPage;
        posInVector += numValuesToCopyInPage;
    }
}

void BaseColumnOrList::readNodeIDsFromAPage(const shared_ptr<ValueVector>& valueVector,
    uint32_t posInVector, uint32_t physicalPageId, uint32_t elementPos, uint64_t numValuesToCopy,
    NodeIDCompressionScheme& compressionScheme, bool isAdjLists) {
    auto nodeValues = (nodeID_t*)valueVector->values;
    auto labelSize = compressionScheme.getNumBytesForLabel();
    auto offsetSize = compressionScheme.getNumBytesForOffset();
    auto frame = bufferManager.pin(fileHandle, physicalPageId);
    if (isAdjLists) {
        valueVector->setRangeNonNull(posInVector, numValuesToCopy);
    } else {
        setNULLBitsForRange(valueVector, frame, elementPos, posInVector, numValuesToCopy);
    }
    frame += mapElementPosToByteOffset(elementPos);
    for (auto i = 0u; i < numValuesToCopy; i++) {
        nodeID_t nodeID{0, 0};
        memcpy(&nodeID.label, frame, labelSize);
        memcpy(&nodeID.offset, frame + labelSize, offsetSize);
        nodeID.label = labelSize == 0 ? compressionScheme.getCommonLabel() : nodeID.label;
        nodeValues[posInVector + i] = nodeID;
        frame += (labelSize + offsetSize);
    }
    bufferManager.unpin(fileHandle, physicalPageId);
}

void BaseColumnOrList::setNULLBitsForRange(const shared_ptr<ValueVector>& valueVector,
    uint8_t* frame, uint64_t elementPos, uint64_t offsetInVector, uint64_t num) {
    while (num) {
        auto numInCurrentByte = min(num, 8 - (elementPos % 8));
        auto NULLByteAndByteLevelOffset =
            PageUtils::getNULLByteAndByteLevelOffsetPair(frame, elementPos);
        setNULLBitsFromANULLByte(
            valueVector, NULLByteAndByteLevelOffset, numInCurrentByte, offsetInVector);
        elementPos += numInCurrentByte;
        offsetInVector += numInCurrentByte;
        num -= numInCurrentByte;
    }
}

void BaseColumnOrList::setNULLBitsForAPos(const shared_ptr<ValueVector>& valueVector,
    uint8_t* frame, uint64_t elementPos, uint64_t offsetInVector) {
    auto NULLByteAndByteLevelOffset =
        PageUtils::getNULLByteAndByteLevelOffsetPair(frame, elementPos);
    setNULLBitsFromANULLByte(valueVector, NULLByteAndByteLevelOffset, 1, offsetInVector);
}

void BaseColumnOrList::setNULLBitsFromANULLByte(const shared_ptr<ValueVector>& valueVector,
    pair<uint8_t, uint8_t> NULLByteAndByteLevelStartOffset, uint8_t num, uint64_t offsetInVector) {
    auto NULLByte = NULLByteAndByteLevelStartOffset.first;
    auto startPos = NULLByteAndByteLevelStartOffset.second;
    while (num--) {
        valueVector->setNull(offsetInVector++, isNullFromNULLByte(NULLByte, startPos++));
    }
}

void BaseColumnOrList::setNullBitOfAPosInFrame(uint8_t* frame, uint16_t elementPos, bool isNull) {
    auto NULLBytePtrAndByteLevelOffset =
        PageUtils::getNULLBytePtrAndByteLevelOffsetPair(frame, elementPos);
    uint8_t NULLByte = *NULLBytePtrAndByteLevelOffset.first;
    if (isNull) {
        *NULLBytePtrAndByteLevelOffset.first =
            NULLByte | bitMasksWithSingle1s[NULLBytePtrAndByteLevelOffset.second];
    } else {
        *NULLBytePtrAndByteLevelOffset.first =
            NULLByte & bitMasksWithSingle0s[NULLBytePtrAndByteLevelOffset.second];
    }
}

} // namespace storage
} // namespace graphflow

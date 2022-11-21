#include "storage/storage_structure/storage_structure.h"

#include "common/utils.h"

namespace kuzu {
namespace storage {

// check if val is in range [start, end)
static inline bool isInRange(uint64_t val, uint64_t start, uint64_t end) {
    return val >= start && val < end;
}

void StorageStructure::addNewPageToFileHandle() {
    auto pageIdxInOriginalFile = fileHandle.addNewPage();
    auto pageIdxInWAL = wal->logPageInsertRecord(
        fileHandle.getStorageStructureIDIDForWALRecord(), pageIdxInOriginalFile);
    bufferManager.pinWithoutAcquiringPageLock(
        *wal->fileHandle, pageIdxInWAL, true /* do not read from file */);
    fileHandle.createPageVersionGroupIfNecessary(pageIdxInOriginalFile);
    fileHandle.setWALPageVersion(pageIdxInOriginalFile, pageIdxInWAL);
    bufferManager.setPinnedPageDirty(*wal->fileHandle, pageIdxInWAL);
    bufferManager.unpinWithoutAcquiringPageLock(*wal->fileHandle, pageIdxInWAL);
}

WALPageIdxPosInPageAndFrame StorageStructure::createWALVersionOfPageIfNecessaryForElement(
    uint64_t elementOffset, uint64_t numElementsPerPage) {
    auto originalPageCursor =
        PageUtils::getPageElementCursorForPos(elementOffset, numElementsPerPage);
    bool insertingNewPage = false;
    if (originalPageCursor.pageIdx >= fileHandle.getNumPages()) {
        assert(originalPageCursor.pageIdx == fileHandle.getNumPages());
        // TODO(Semih/Guodong): What if the column is in memory? How do we ensure that the file
        // remains pinned?
        addNewPageToFileHandle();
        insertingNewPage = true;
    }
    auto walPageIdxAndFrame = StorageStructureUtils::createWALVersionIfNecessaryAndPinPage(
        originalPageCursor.pageIdx, insertingNewPage, fileHandle, bufferManager, *wal);
    return WALPageIdxPosInPageAndFrame(walPageIdxAndFrame, originalPageCursor.elemPosInPage);
}

BaseColumnOrList::BaseColumnOrList(const StorageStructureIDAndFName& storageStructureIDAndFName,
    DataType dataType, const size_t& elementSize, BufferManager& bufferManager, bool hasNULLBytes,
    bool isInMemory, WAL* wal)
    : StorageStructure(storageStructureIDAndFName, bufferManager, isInMemory, wal),
      dataType{std::move(dataType)}, elementSize{elementSize} {
    numElementsPerPage = PageUtils::getNumElementsInAPage(elementSize, hasNULLBytes);
}

void BaseColumnOrList::readBySequentialCopy(Transaction* transaction,
    const shared_ptr<ValueVector>& vector, PageElementCursor& cursor,
    const std::function<page_idx_t(page_idx_t)>& logicalToPhysicalPageMapper) {
    uint64_t numValuesToRead = vector->state->originalSize;
    uint64_t vectorPos = 0;
    while (vectorPos != numValuesToRead) {
        uint64_t numValuesInPage = numElementsPerPage - cursor.elemPosInPage;
        uint64_t numValuesToReadInPage = min(numValuesInPage, numValuesToRead - vectorPos);
        auto physicalPageIdx = logicalToPhysicalPageMapper(cursor.pageIdx);
        readAPageBySequentialCopy(transaction, vector, vectorPos, physicalPageIdx,
            cursor.elemPosInPage, numValuesToReadInPage);
        vectorPos += numValuesToReadInPage;
        cursor.nextPage();
    }
}

void BaseColumnOrList::readBySequentialCopyWithSelState(Transaction* transaction,
    const shared_ptr<ValueVector>& vector, PageElementCursor& cursor,
    const std::function<page_idx_t(page_idx_t)>& logicalToPhysicalPageMapper) {
    auto selectedState = vector->state;
    auto numValuesToRead = vector->state->originalSize;
    uint64_t selectedStatePos = 0;
    uint64_t vectorPos = 0;
    while (true) {
        uint64_t numValuesInPage = numElementsPerPage - cursor.elemPosInPage;
        uint64_t numValuesToReadInPage = min(numValuesInPage, numValuesToRead - vectorPos);
        if (isInRange(selectedState->selVector->selectedPositions[selectedStatePos], vectorPos,
                vectorPos + numValuesToReadInPage)) {
            auto physicalPageIdx = logicalToPhysicalPageMapper(cursor.pageIdx);
            readAPageBySequentialCopy(transaction, vector, vectorPos, physicalPageIdx,
                cursor.elemPosInPage, numValuesToReadInPage);
        }
        vectorPos += numValuesToReadInPage;
        while (selectedState->selVector->selectedPositions[selectedStatePos] < vectorPos) {
            selectedStatePos++;
            if (selectedStatePos == selectedState->selVector->selectedSize) {
                return;
            }
        }
        cursor.nextPage();
    }
}

void BaseColumnOrList::readNodeIDsBySequentialCopy(Transaction* transaction,
    const shared_ptr<ValueVector>& valueVector, PageElementCursor& cursor,
    const std::function<page_idx_t(page_idx_t)>& logicalToPhysicalPageMapper,
    NodeIDCompressionScheme nodeIDCompressionScheme, bool isAdjLists) {
    uint64_t numValuesToRead = valueVector->state->originalSize;
    uint64_t vectorPos = 0;
    while (vectorPos != numValuesToRead) {
        uint64_t numValuesInPage = numElementsPerPage - cursor.elemPosInPage;
        uint64_t numValuesToReadInPage = min(numValuesInPage, numValuesToRead - vectorPos);
        auto physicalPageId = logicalToPhysicalPageMapper(cursor.pageIdx);
        readNodeIDsFromAPageBySequentialCopy(transaction, valueVector, vectorPos, physicalPageId,
            cursor.elemPosInPage, numValuesToReadInPage, nodeIDCompressionScheme, isAdjLists);
        vectorPos += numValuesToReadInPage;
        cursor.nextPage();
    }
}

void BaseColumnOrList::readNodeIDsBySequentialCopyWithSelState(Transaction* transaction,
    const shared_ptr<ValueVector>& vector, PageElementCursor& cursor,
    const std::function<page_idx_t(page_idx_t)>& logicalToPhysicalPageMapper,
    NodeIDCompressionScheme nodeIDCompressionScheme) {
    auto selectedState = vector->state;
    uint64_t numValuesToRead = vector->state->originalSize;
    uint64_t selectedStatePos = 0;
    uint64_t vectorPos = 0;
    while (true) {
        uint64_t numValuesInPage = numElementsPerPage - cursor.elemPosInPage;
        uint64_t numValuesToReadInPage = min(numValuesInPage, numValuesToRead - vectorPos);
        if (isInRange(selectedState->selVector->selectedPositions[selectedStatePos], vectorPos,
                vectorPos + numValuesToReadInPage)) {
            auto physicalPageIdx = logicalToPhysicalPageMapper(cursor.pageIdx);
            readNodeIDsFromAPageBySequentialCopy(transaction, vector, vectorPos, physicalPageIdx,
                cursor.elemPosInPage, numValuesToReadInPage, nodeIDCompressionScheme,
                false /* isAdjList */);
        }
        vectorPos += numValuesToReadInPage;
        while (selectedState->selVector->selectedPositions[selectedStatePos] < vectorPos) {
            selectedStatePos++;
            if (selectedStatePos == selectedState->selVector->selectedSize) {
                return;
            }
        }
        cursor.nextPage();
    }
}

void BaseColumnOrList::readNodeIDsFromAPageBySequentialCopy(Transaction* transaction,
    const shared_ptr<ValueVector>& vector, uint64_t vectorStartPos, page_idx_t physicalPageIdx,
    uint16_t pagePosOfFirstElement, uint64_t numValuesToRead,
    NodeIDCompressionScheme& nodeIDCompressionScheme, bool isAdjLists) {
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            fileHandle, physicalPageIdx, *wal, transaction->getType());
    auto frame = bufferManager.pin(*fileHandleToPin, pageIdxToPin);
    if (isAdjLists) {
        vector->setRangeNonNull(vectorStartPos, numValuesToRead);
    } else {
        readNullBitsFromAPage(
            vector, frame, pagePosOfFirstElement, vectorStartPos, numValuesToRead);
    }
    auto currentFrameHead = frame + pagePosOfFirstElement * elementSize;
    for (auto i = 0u; i < numValuesToRead; i++) {
        nodeID_t nodeID{0, 0};
        nodeIDCompressionScheme.readNodeID(currentFrameHead, &nodeID);
        currentFrameHead += nodeIDCompressionScheme.getNumBytesForNodeIDAfterCompression();
        vector->setValue(vectorStartPos + i, nodeID);
    }
    bufferManager.unpin(*fileHandleToPin, pageIdxToPin);
}

void BaseColumnOrList::readSingleNullBit(const shared_ptr<ValueVector>& valueVector,
    const uint8_t* frame, uint64_t elementPos, uint64_t offsetInVector) const {
    auto inputNullEntries = (uint64_t*)(frame + (numElementsPerPage * elementSize));
    bool isNull = NullMask::isNull(inputNullEntries, elementPos);
    valueVector->setNull(offsetInVector, isNull);
}

void BaseColumnOrList::readAPageBySequentialCopy(Transaction* transaction,
    const shared_ptr<ValueVector>& vector, uint64_t vectorStartPos, page_idx_t physicalPageIdx,
    uint16_t pagePosOfFirstElement, uint64_t numValuesToRead) {
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            fileHandle, physicalPageIdx, *wal, transaction->getType());
    auto vectorBytesOffset = vectorStartPos * elementSize;
    auto frameBytesOffset = pagePosOfFirstElement * elementSize;
    auto frame = bufferManager.pin(*fileHandleToPin, pageIdxToPin);
    memcpy(vector->getData() + vectorBytesOffset, frame + frameBytesOffset,
        numValuesToRead * elementSize);
    readNullBitsFromAPage(vector, frame, pagePosOfFirstElement, vectorStartPos, numValuesToRead);
    bufferManager.unpin(*fileHandleToPin, pageIdxToPin);
}

void BaseColumnOrList::readNullBitsFromAPage(const shared_ptr<ValueVector>& valueVector,
    const uint8_t* frame, uint64_t posInPage, uint64_t posInVector, uint64_t numBitsToRead) const {
    auto hasNullInSrcNullMask =
        NullMask::copyNullMask((uint64_t*)(frame + (numElementsPerPage * elementSize)), posInPage,
            valueVector->getNullMaskData(), posInVector, numBitsToRead);
    if (hasNullInSrcNullMask) {
        valueVector->setMayContainNulls();
    }
}

void BaseColumnOrList::setNullBitOfAPosInFrame(
    uint8_t* frame, uint16_t elementPosInPage, bool isNull) const {
    auto nullMask = (uint64_t*)(frame + (numElementsPerPage * elementSize));
    auto nullEntryPos = elementPosInPage >> NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2;
    auto bitOffsetInEntry =
        elementPosInPage - (nullEntryPos << NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2);
    if (isNull) {
        nullMask[nullEntryPos] |= NULL_BITMASKS_WITH_SINGLE_ONE[bitOffsetInEntry];
    } else {
        nullMask[nullEntryPos] &= NULL_BITMASKS_WITH_SINGLE_ZERO[bitOffsetInEntry];
    }
}

} // namespace storage
} // namespace kuzu

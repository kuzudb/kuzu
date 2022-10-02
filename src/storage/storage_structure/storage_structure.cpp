#include "src/storage/storage_structure/include/storage_structure.h"

#include "src/common/include/utils.h"

namespace graphflow {
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
      dataType{move(dataType)}, elementSize{elementSize} {
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

void BaseColumnOrList::readNodeIDsBySequentialCopy(const shared_ptr<ValueVector>& valueVector,
    PageElementCursor& cursor,
    const std::function<page_idx_t(page_idx_t)>& logicalToPhysicalPageMapper,
    NodeIDCompressionScheme nodeIDCompressionScheme, bool isAdjLists) {
    uint64_t numValuesToRead = valueVector->state->originalSize;
    uint64_t vectorPos = 0;
    while (vectorPos != numValuesToRead) {
        uint64_t numValuesInPage = numElementsPerPage - cursor.elemPosInPage;
        uint64_t numValuesToReadInPage = min(numValuesInPage, numValuesToRead - vectorPos);
        auto physicalPageId = logicalToPhysicalPageMapper(cursor.pageIdx);
        readNodeIDsFromAPageBySequentialCopy(valueVector, vectorPos, physicalPageId,
            cursor.elemPosInPage, numValuesToReadInPage, nodeIDCompressionScheme, isAdjLists);
        vectorPos += numValuesToReadInPage;
        cursor.nextPage();
    }
}

void BaseColumnOrList::readNodeIDsBySequentialCopyWithSelState(
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
            readNodeIDsFromAPageBySequentialCopy(vector, vectorPos, physicalPageIdx,
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

void BaseColumnOrList::readNodeIDsFromAPageBySequentialCopy(const shared_ptr<ValueVector>& vector,
    uint64_t vectorStartPos, page_idx_t physicalPageIdx, uint16_t pagePosOfFirstElement,
    uint64_t numValuesToRead, NodeIDCompressionScheme& nodeIDCompressionScheme, bool isAdjLists) {
    auto nodeValues = (nodeID_t*)vector->values;
    auto frame = bufferManager.pin(fileHandle, physicalPageIdx);
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
        nodeValues[vectorStartPos + i] = nodeID;
    }
    bufferManager.unpin(fileHandle, physicalPageIdx);
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
            fileHandle, physicalPageIdx, *wal, transaction->isReadOnly());
    auto vectorBytesOffset = vectorStartPos * elementSize;
    auto frameBytesOffset = pagePosOfFirstElement * elementSize;
    auto frame = bufferManager.pin(*fileHandleToPin, pageIdxToPin);
    memcpy(vector->values + vectorBytesOffset, frame + frameBytesOffset,
        numValuesToRead * elementSize);
    readNullBitsFromAPage(vector, frame, pagePosOfFirstElement, vectorStartPos, numValuesToRead);
    bufferManager.unpin(*fileHandleToPin, pageIdxToPin);
}

void BaseColumnOrList::readNullBitsFromAPage(const shared_ptr<ValueVector>& valueVector,
    const uint8_t* frame, uint64_t posInPage, uint64_t posInVector, uint64_t numBitsToRead) const {
    auto [nullBitPosInPageEntry, nullEntryPosInPage] = NullMask::getNullBitAndEntryPos(posInPage);
    auto [nullBitPosInVectorEntry, nullEntryPosInVector] =
        NullMask::getNullBitAndEntryPos(posInVector);
    auto hasNullInSrcNullMask = NullMask::copyNullMask(nullBitPosInPageEntry, nullEntryPosInPage,
        (uint64_t*)(frame + (numElementsPerPage * elementSize)), nullBitPosInVectorEntry,
        nullEntryPosInVector, valueVector->getNullMaskData(), numBitsToRead);
    if (hasNullInSrcNullMask) {
        valueVector->setMayContainNulls();
    }
}

//void BaseColumnOrList::readNullBitsFromAPage(const shared_ptr<ValueVector>& valueVector,
//    const uint8_t* frame, uint64_t posInPage, uint64_t posInVector, uint64_t numBitsToRead) const
//    { auto nullEntryPosInPage = posInPage >> NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2; auto
//    nullBitPosInPageEntry =
//        posInPage - (nullEntryPosInPage << NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2);
//    auto nullEntryPosInVector = posInVector >> NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2;
//    auto nullBitPosInVectorEntry =
//        posInVector - (nullEntryPosInVector << NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2);
//    auto nullEntriesInPage = (uint64_t*)(frame + (numElementsPerPage * elementSize));
//    uint64_t bitPos = 0;
//    while (bitPos < numBitsToRead) {
//        auto currentNullEntryPosInVector = nullEntryPosInVector;
//        auto currentNullBitPosInVectorEntry = nullBitPosInVectorEntry;
//        uint64_t numBitsToReadInCurrentEntry = 0;
//        uint64_t nullMaskEntry = nullEntriesInPage[nullEntryPosInPage];
//        if (nullBitPosInVectorEntry < nullBitPosInPageEntry) {
//            numBitsToReadInCurrentEntry = min(
//                NullMask::NUM_BITS_PER_NULL_ENTRY - nullBitPosInPageEntry, numBitsToRead -
//                bitPos);
//            // Mask higher bits out of current read range to 0.
//            nullMaskEntry &=
//                ~NULL_HIGH_MASKS[NullMask::NUM_BITS_PER_NULL_ENTRY -
//                                 (nullBitPosInPageEntry + numBitsToReadInCurrentEntry)];
//            // Shift right to align the bit in the page entry and vector entry.
//            auto numBitsToShift = nullBitPosInPageEntry - nullBitPosInVectorEntry;
//            nullMaskEntry = nullMaskEntry >> numBitsToShift;
//            // Mask lower bits out of current read range to 0.
//            nullMaskEntry &= ~NULL_LOWER_MASKS[nullBitPosInVectorEntry];
//            // Move to the next null entry in page.
//            nullEntryPosInPage++;
//            nullBitPosInPageEntry = 0;
//            nullBitPosInVectorEntry += numBitsToReadInCurrentEntry;
//        } else if (nullBitPosInVectorEntry > nullBitPosInPageEntry) {
//            numBitsToReadInCurrentEntry =
//                min(NullMask::NUM_BITS_PER_NULL_ENTRY - nullBitPosInVectorEntry,
//                    numBitsToRead - bitPos);
//            auto numBitsToShift = nullBitPosInVectorEntry - nullBitPosInPageEntry;
//            // Mask lower bits out of current read range to 0.
//            nullMaskEntry &= ~NULL_LOWER_MASKS[nullBitPosInPageEntry];
//            // Shift left to align the bit in the page entry and vector entry.
//            nullMaskEntry = nullMaskEntry << numBitsToShift;
//            // Mask higher bits out of current read range to 0.
//            nullMaskEntry &=
//                ~NULL_HIGH_MASKS[NullMask::NUM_BITS_PER_NULL_ENTRY -
//                                 (nullBitPosInVectorEntry + numBitsToReadInCurrentEntry)];
//            // Move to the next null entry in vector.
//            nullEntryPosInVector++;
//            nullBitPosInVectorEntry = 0;
//            nullBitPosInPageEntry += numBitsToReadInCurrentEntry;
//        } else {
//            numBitsToReadInCurrentEntry =
//                min(NullMask::NUM_BITS_PER_NULL_ENTRY - nullBitPosInVectorEntry,
//                    numBitsToRead - bitPos);
//            // Mask lower bits out of current read range to 0.
//            nullMaskEntry &= ~NULL_LOWER_MASKS[nullBitPosInPageEntry];
//            // Mask higher bits out of current read range to 0.
//            nullMaskEntry &=
//                ~NULL_HIGH_MASKS[NullMask::NUM_BITS_PER_NULL_ENTRY -
//                                 (nullBitPosInVectorEntry + numBitsToReadInCurrentEntry)];
//            // The input entry and the result entry are already aligned.
//            nullEntryPosInVector++;
//            nullEntryPosInPage++;
//            nullBitPosInVectorEntry = nullBitPosInPageEntry = 0;
//        }
//        bitPos += numBitsToReadInCurrentEntry;
//        // Mask all bits to set in vector to 0.
//        auto resultNullEntries = valueVector->getNullMaskData();
//        resultNullEntries[currentNullEntryPosInVector] &=
//            ~(NULL_LOWER_MASKS[numBitsToReadInCurrentEntry] << currentNullBitPosInVectorEntry);
//        if (nullMaskEntry != 0) {
//            resultNullEntries[currentNullEntryPosInVector] |= nullMaskEntry;
//            valueVector->setMayContainNulls();
//        }
//    }
//}

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
} // namespace graphflow

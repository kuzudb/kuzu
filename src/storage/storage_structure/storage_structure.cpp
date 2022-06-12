#include "src/storage/include/storage_structure/storage_structure.h"

#include "src/common/include/utils.h"

namespace graphflow {
namespace storage {

// check if val is in range [start, end)
static inline bool isInRange(uint64_t val, uint64_t start, uint64_t end) {
    return val >= start && val < end;
}

pair<FileHandle*, page_idx_t> StorageStructure::getFileHandleAndPhysicalPageIdxToPin(
    Transaction* transaction, page_idx_t physicalPageIdx) {
    if (transaction->isReadOnly() || !fileHandle.hasUpdatedWALPageVersionNoLock(physicalPageIdx)) {
        return make_pair(&fileHandle, physicalPageIdx);
    } else {
        return make_pair(
            wal->fileHandle.get(), fileHandle.getUpdatedWALPageVersionNoLock(physicalPageIdx));
    }
}

UpdatedPageInfoAndWALPageFrame
StorageStructure::getUpdatePageInfoForElementAndCreateWALVersionOfPageIfNecessary(
    uint64_t elementOffset, uint64_t numElementsPerPage) {
    auto originalPageCursor =
        PageUtils::getPageElementCursorForPos(elementOffset, numElementsPerPage);
    fileHandle.createPageVersionGroupIfNecessary(originalPageCursor.pageIdx);
    fileHandle.acquirePageLock(originalPageCursor.pageIdx, true /* block */);
    page_idx_t pageIdxInWAL;
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
        uint64_t numValuesInPage = numElementsPerPage - cursor.posInPage;
        uint64_t numValuesToReadInPage = min(numValuesInPage, numValuesToRead - vectorPos);
        auto physicalPageIdx = logicalToPhysicalPageMapper(cursor.pageIdx);
        readAPageBySequentialCopy(transaction, vector, vectorPos, physicalPageIdx, cursor.posInPage,
            numValuesToReadInPage);
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
    uint64_t numValuesInFirstPage = numElementsPerPage - cursor.posInPage;
    uint64_t vectorPos = getNumValuesToSkipInSequentialCopy(
        selectedState->selectedPositions[selectedStatePos], numValuesInFirstPage);
    while (selectedStatePos < selectedState->selectedSize) {
        uint64_t numValuesInPage = numElementsPerPage - cursor.posInPage;
        uint64_t numValuesToReadInPage = min(numValuesInPage, numValuesToRead - vectorPos);
        auto vectorPosAfterRead = vectorPos + numValuesToReadInPage;
        if (isInRange(selectedState->selectedPositions[selectedStatePos], vectorPos,
                vectorPosAfterRead)) {
            auto physicalPageIdx = logicalToPhysicalPageMapper(cursor.pageIdx);
            readAPageBySequentialCopyWithSelState(transaction, vector, selectedStatePos,
                physicalPageIdx, cursor.posInPage, vectorPos, vectorPosAfterRead);
        }
        vectorPos += numValuesToReadInPage;
        cursor.nextPage();
    }
}

void BaseColumnOrList::readNodeIDsBySequentialCopy(const shared_ptr<ValueVector>& valueVector,
    PageElementCursor& cursor,
    const std::function<page_idx_t(page_idx_t)>& logicalToPhysicalPageMapper,
    NodeIDCompressionScheme compressionScheme, bool isAdjLists) {
    uint64_t numValuesToRead = valueVector->state->originalSize;
    uint64_t vectorPos = 0;
    while (vectorPos != numValuesToRead) {
        uint64_t numValuesInPage = numElementsPerPage - cursor.posInPage;
        uint64_t numValuesToReadInPage = min(numValuesInPage, numValuesToRead - vectorPos);
        auto physicalPageId = logicalToPhysicalPageMapper(cursor.pageIdx);
        readNodeIDsFromAPageBySequentialCopy(valueVector, vectorPos, physicalPageId,
            cursor.posInPage, numValuesToReadInPage, compressionScheme, isAdjLists);
        vectorPos += numValuesToReadInPage;
        cursor.nextPage();
    }
}

void BaseColumnOrList::readNodeIDsBySequentialCopyWithSelState(
    const shared_ptr<ValueVector>& vector, PageElementCursor& cursor,
    const std::function<page_idx_t(page_idx_t)>& logicalToPhysicalPageMapper,
    NodeIDCompressionScheme compressionScheme) {
    auto selectedState = vector->state;
    uint64_t numValuesToRead = vector->state->originalSize;
    uint64_t selectedStatePos = 0;
    uint64_t numValuesInFirstPage = numElementsPerPage - cursor.posInPage;
    uint64_t vectorPos = getNumValuesToSkipInSequentialCopy(
        selectedState->selectedPositions[selectedStatePos], numValuesInFirstPage);
    while (vectorPos != numValuesToRead && selectedStatePos < selectedState->selectedSize) {
        uint64_t numValuesInPage = numElementsPerPage - cursor.posInPage;
        uint64_t numValuesToReadInPage = min(numValuesInPage, numValuesToRead - vectorPos);
        auto vectorPosAfterRead = vectorPos + numValuesToReadInPage;
        if (isInRange(selectedState->selectedPositions[selectedStatePos], vectorPos,
                vectorPosAfterRead)) {
            auto physicalPageIdx = logicalToPhysicalPageMapper(cursor.pageIdx);
            readNodeIDsFromAPageBySequentialCopyWithSelState(vector, selectedStatePos,
                physicalPageIdx, cursor.posInPage, vectorPos, vectorPosAfterRead,
                compressionScheme);
        }
        vectorPos += numValuesToReadInPage;
        cursor.nextPage();
    }
}

void BaseColumnOrList::readNodeIDsFromAPageBySequentialCopy(const shared_ptr<ValueVector>& vector,
    uint64_t vectorStartPos, page_idx_t physicalPageIdx, uint16_t pagePosOfFirstElement,
    uint64_t numValuesToRead, NodeIDCompressionScheme& compressionScheme, bool isAdjLists) {
    auto nodeValues = (nodeID_t*)vector->values;
    auto labelSize = compressionScheme.getNumBytesForLabel();
    auto offsetSize = compressionScheme.getNumBytesForOffset();
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
        if (labelSize == 0) {
            nodeID.label = compressionScheme.getCommonLabel();
        } else {
            memcpy(&nodeID.label, currentFrameHead, labelSize);
            currentFrameHead += labelSize;
        }
        memcpy(&nodeID.offset, currentFrameHead, offsetSize);
        currentFrameHead += offsetSize;
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

uint64_t BaseColumnOrList::getNumValuesToSkipInSequentialCopy(
    uint64_t numValuesTryToSkip, uint64_t numValuesInFirstPage) const {
    uint64_t result = 0;
    if (numValuesTryToSkip >= numValuesInFirstPage) {
        result += numValuesInFirstPage;
        uint64_t numCompletePageToSkip =
            (numValuesTryToSkip - numValuesInFirstPage) / numElementsPerPage;
        result += numCompletePageToSkip * numElementsPerPage;
    }
    assert(result <= numValuesTryToSkip);
    return result;
}

void BaseColumnOrList::readAPageBySequentialCopy(Transaction* transaction,
    const shared_ptr<ValueVector>& vector, uint64_t vectorStartPos, page_idx_t physicalPageIdx,
    uint16_t pagePosOfFirstElement, uint64_t numValuesToRead) {
    auto [fileHandleToPin, pageIdxToPin] =
        getFileHandleAndPhysicalPageIdxToPin(transaction, physicalPageIdx);
    auto vectorBytesOffset = vectorStartPos * elementSize;
    auto frameBytesOffset = pagePosOfFirstElement * elementSize;
    auto frame = bufferManager.pin(*fileHandleToPin, pageIdxToPin);
    memcpy(vector->values + vectorBytesOffset, frame + frameBytesOffset,
        numValuesToRead * elementSize);
    readNullBitsFromAPage(vector, frame, pagePosOfFirstElement, vectorStartPos, numValuesToRead);
    bufferManager.unpin(*fileHandleToPin, pageIdxToPin);
}

/**
 * This function tries to read 1 or many elements from the page (starting from
 * pagePosOfFirstElement) based on selected positions. The position of these elements in vector
 * falls in the range of [vectorPosOfFirstUnselElement, vectorPosOfLastUnselElement)
 *
 * @param pagePosOfFirstElement
 *    Element pos in the page of the first unselected element.
 * @param vectorPosOfFirstUnselElement
 *    Position of first element in vector, ignoring any selected position, whose value falls in this
 *    page. The element stored in vector[vectorPosOfFirstUnselElement] is called as "first unselect
 *    element"
 * @param vectorPosOfLastUnselElement
 *    Position of last element in vector, ignoring any selected position, whose value falls in this
 *    page. The element stored in vector[vectorPosOfFirstUnselElement] is called as "last unselect
 *    element"
 */
void BaseColumnOrList::readAPageBySequentialCopyWithSelState(Transaction* transaction,
    const shared_ptr<ValueVector>& vector, uint64_t& nextSelectedPos, page_idx_t physicalPageIdx,
    uint16_t pagePosOfFirstElement, uint64_t vectorPosOfFirstUnselElement,
    uint64_t vectorPosOfLastUnselElement) {
    auto selectedState = vector->state;
    auto [fileHandleToPin, pageIdxToPin] =
        getFileHandleAndPhysicalPageIdxToPin(transaction, physicalPageIdx);
    auto frame = bufferManager.pin(*fileHandleToPin, pageIdxToPin);
    while (selectedState->selectedPositions[nextSelectedPos] < vectorPosOfLastUnselElement &&
           nextSelectedPos < selectedState->selectedSize) {
        auto vectorPos = selectedState->selectedPositions[nextSelectedPos];
        auto vectorBytesOffset = vectorPos * elementSize;
        auto pagePos = pagePosOfFirstElement + vectorPos - vectorPosOfFirstUnselElement;
        auto frameBytesOffset = pagePos * elementSize;
        memcpy(vector->values + vectorBytesOffset, frame + frameBytesOffset, elementSize);
        readSingleNullBit(vector, frame, pagePos, vectorPos);
        nextSelectedPos++;
    }
    bufferManager.unpin(*fileHandleToPin, pageIdxToPin);
}

// see BaseColumnOrList::readAPageBySequentialCopyWithSelState for descriptions
void BaseColumnOrList::readNodeIDsFromAPageBySequentialCopyWithSelState(
    const shared_ptr<ValueVector>& vector, uint64_t& nextSelectedPos, page_idx_t physicalPageIdx,
    uint16_t pagePosOfFirstElement, uint64_t vectorPosOfFirstUnselElement,
    uint64_t vectorPosOfLastUnselElement, NodeIDCompressionScheme& compressionScheme) {
    auto selectedState = vector->state;
    auto nodeValues = (nodeID_t*)vector->values;
    auto labelSize = compressionScheme.getNumBytesForLabel();
    auto offsetSize = compressionScheme.getNumBytesForOffset();
    auto frame = bufferManager.pin(fileHandle, physicalPageIdx);
    while (selectedState->selectedPositions[nextSelectedPos] < vectorPosOfLastUnselElement &&
           nextSelectedPos < selectedState->selectedSize) {
        auto vectorPos = selectedState->selectedPositions[nextSelectedPos];
        auto framePos = pagePosOfFirstElement + vectorPos - vectorPosOfFirstUnselElement;
        auto frameBytesOffset = framePos * elementSize;
        nodeID_t nodeID{0, 0};
        if (labelSize == 0) {
            nodeID.label = compressionScheme.getCommonLabel();
        } else {
            memcpy(&nodeID.label, frame + frameBytesOffset, labelSize);
            frameBytesOffset += labelSize;
        }
        memcpy(&nodeID.offset, frame + frameBytesOffset, offsetSize);
        nodeValues[vectorPos] = nodeID;
        readSingleNullBit(vector, frame, framePos, vectorPos);
        nextSelectedPos++;
    }
    bufferManager.unpin(fileHandle, physicalPageIdx);
}

void BaseColumnOrList::readNullBitsFromAPage(const shared_ptr<ValueVector>& valueVector,
    const uint8_t* frame, uint64_t posInPage, uint64_t posInVector, uint64_t numBitsToRead) const {
    auto nullEntryPosInPage = posInPage >> NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2;
    auto nullBitPosInPageEntry =
        posInPage - (nullEntryPosInPage << NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2);
    auto nullEntryPosInVector = posInVector >> NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2;
    auto nullBitPosInVectorEntry =
        posInVector - (nullEntryPosInVector << NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2);
    auto nullEntriesInPage = (uint64_t*)(frame + (numElementsPerPage * elementSize));
    uint64_t bitPos = 0;
    while (bitPos < numBitsToRead) {
        auto currentNullEntryPosInVector = nullEntryPosInVector;
        auto currentNullBitPosInVectorEntry = nullBitPosInVectorEntry;
        uint64_t numBitsToReadInCurrentEntry = 0;
        uint64_t nullMaskEntry = nullEntriesInPage[nullEntryPosInPage];
        if (nullBitPosInVectorEntry < nullBitPosInPageEntry) {
            numBitsToReadInCurrentEntry =
                min(NullMask::NUM_BITS_PER_NULL_ENTRY - nullBitPosInPageEntry, numBitsToRead);
            // Shift right to align the bit in the page entry and vector entry.
            auto numBitsToShift = nullBitPosInPageEntry - nullBitPosInVectorEntry;
            nullMaskEntry = nullMaskEntry >> numBitsToShift;
            // Mask lower bits out of current read range to 0.
            nullMaskEntry &= ~NULL_LOWER_MASKS[nullBitPosInVectorEntry];
            // Mask higher bits out of current read range to 0.
            nullMaskEntry &=
                ~NULL_HIGH_MASKS[NullMask::NUM_BITS_PER_NULL_ENTRY -
                                 (nullBitPosInPageEntry + numBitsToReadInCurrentEntry)];
            // Move to the next null entry in page.
            nullEntryPosInPage++;
            nullBitPosInPageEntry = 0;
            nullBitPosInVectorEntry += numBitsToReadInCurrentEntry;
        } else if (nullBitPosInVectorEntry > nullBitPosInPageEntry) {
            numBitsToReadInCurrentEntry =
                min(NullMask::NUM_BITS_PER_NULL_ENTRY - nullBitPosInVectorEntry, numBitsToRead);
            auto numBitsToShift = nullBitPosInVectorEntry - nullBitPosInPageEntry;
            // Mask lower bits out of current read range to 0.
            nullMaskEntry &= ~NULL_LOWER_MASKS[nullBitPosInPageEntry];
            // Shift left to align the bit in the page entry and vector entry.
            nullMaskEntry = nullMaskEntry << numBitsToShift;
            // Mask higher bits out of current read range to 0.
            nullMaskEntry &=
                ~NULL_HIGH_MASKS[NullMask::NUM_BITS_PER_NULL_ENTRY -
                                 (nullBitPosInVectorEntry + numBitsToReadInCurrentEntry)];
            // Move to the next null entry in vector.
            nullEntryPosInVector++;
            nullBitPosInVectorEntry = 0;
            nullBitPosInPageEntry += numBitsToReadInCurrentEntry;
        } else {
            numBitsToReadInCurrentEntry =
                min(NullMask::NUM_BITS_PER_NULL_ENTRY - nullBitPosInVectorEntry, numBitsToRead);
            // Mask lower bits out of current read range to 0.
            nullMaskEntry &= ~NULL_LOWER_MASKS[nullBitPosInPageEntry];
            // Mask higher bits out of current read range to 0.
            nullMaskEntry &=
                ~NULL_HIGH_MASKS[NullMask::NUM_BITS_PER_NULL_ENTRY -
                                 (nullBitPosInVectorEntry + numBitsToReadInCurrentEntry)];
            // The input entry and the result entry are already aligned.
            nullEntryPosInVector++;
            nullEntryPosInPage++;
            nullBitPosInVectorEntry = nullBitPosInPageEntry = 0;
        }
        bitPos += numBitsToReadInCurrentEntry;
        // Mask all bits to set in vector to 0.
        auto resultNullEntries = valueVector->getNullMaskData();
        resultNullEntries[currentNullEntryPosInVector] &=
            ~(NULL_LOWER_MASKS[numBitsToReadInCurrentEntry] << currentNullBitPosInVectorEntry);
        if (nullMaskEntry != 0) {
            resultNullEntries[currentNullEntryPosInVector] |= nullMaskEntry;
            valueVector->setMayContainNulls();
        }
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
} // namespace graphflow

#include "storage/storage_structure/storage_structure.h"

#include "common/utils.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

// check if val is in range [start, end)
static inline bool isInRange(uint64_t val, uint64_t start, uint64_t end) {
    return val >= start && val < end;
}

void StorageStructure::addNewPageToFileHandle() {
    auto pageIdxInOriginalFile = fileHandle->addNewPage();
    auto pageIdxInWAL = wal->logPageInsertRecord(storageStructureID, pageIdxInOriginalFile);
    bufferManager.pin(
        *wal->fileHandle, pageIdxInWAL, BufferManager::PageReadPolicy::DONT_READ_PAGE);
    fileHandle->addWALPageIdxGroupIfNecessary(pageIdxInOriginalFile);
    fileHandle->setWALPageIdx(pageIdxInOriginalFile, pageIdxInWAL);
    wal->fileHandle->setLockedPageDirty(pageIdxInWAL);
    bufferManager.unpin(*wal->fileHandle, pageIdxInWAL);
}

WALPageIdxPosInPageAndFrame StorageStructure::createWALVersionOfPageIfNecessaryForElement(
    uint64_t elementOffset, uint64_t numElementsPerPage) {
    auto originalPageCursor =
        PageUtils::getPageElementCursorForPos(elementOffset, numElementsPerPage);
    bool insertingNewPage = false;
    if (originalPageCursor.pageIdx >= fileHandle->getNumPages()) {
        assert(originalPageCursor.pageIdx == fileHandle->getNumPages());
        addNewPageToFileHandle();
        insertingNewPage = true;
    }
    auto walPageIdxAndFrame =
        StorageStructureUtils::createWALVersionIfNecessaryAndPinPage(originalPageCursor.pageIdx,
            insertingNewPage, *fileHandle, storageStructureID, bufferManager, *wal);
    return {walPageIdxAndFrame, originalPageCursor.elemPosInPage};
}

BaseColumnOrList::BaseColumnOrList(const StorageStructureIDAndFName& storageStructureIDAndFName,
    DataType dataType, const size_t& elementSize, BufferManager& bufferManager, bool hasNULLBytes,
    WAL* wal)
    : StorageStructure(storageStructureIDAndFName, bufferManager, wal),
      dataType{std::move(dataType)}, elementSize{elementSize} {
    numElementsPerPage = PageUtils::getNumElementsInAPage(elementSize, hasNULLBytes);
}

void BaseColumnOrList::readBySequentialCopy(Transaction* transaction, common::ValueVector* vector,
    PageElementCursor& cursor,
    const std::function<page_idx_t(page_idx_t)>& logicalToPhysicalPageMapper) {
    uint64_t numValuesToRead = vector->state->originalSize;
    uint64_t vectorPos = 0;
    while (vectorPos != numValuesToRead) {
        uint64_t numValuesInPage = numElementsPerPage - cursor.elemPosInPage;
        uint64_t numValuesToReadInPage = std::min(numValuesInPage, numValuesToRead - vectorPos);
        auto physicalPageIdx = logicalToPhysicalPageMapper(cursor.pageIdx);
        readAPageBySequentialCopy(transaction, vector, vectorPos, physicalPageIdx,
            cursor.elemPosInPage, numValuesToReadInPage);
        vectorPos += numValuesToReadInPage;
        cursor.nextPage();
    }
}

void BaseColumnOrList::readInternalIDsBySequentialCopy(Transaction* transaction,
    ValueVector* vector, PageElementCursor& cursor,
    const std::function<page_idx_t(page_idx_t)>& logicalToPhysicalPageMapper,
    table_id_t commonTableID, bool hasNoNullGuarantee) {
    uint64_t numValuesToRead = vector->state->originalSize;
    uint64_t vectorPos = 0;
    while (vectorPos != numValuesToRead) {
        uint64_t numValuesInPage = numElementsPerPage - cursor.elemPosInPage;
        uint64_t numValuesToReadInPage = std::min(numValuesInPage, numValuesToRead - vectorPos);
        auto physicalPageIdx = logicalToPhysicalPageMapper(cursor.pageIdx);
        readInternalIDsFromAPageBySequentialCopy(transaction, vector, vectorPos, physicalPageIdx,
            cursor.elemPosInPage, numValuesToReadInPage, commonTableID, hasNoNullGuarantee);
        vectorPos += numValuesToReadInPage;
        cursor.nextPage();
    }
}

void BaseColumnOrList::readInternalIDsFromAPageBySequentialCopy(Transaction* transaction,
    ValueVector* vector, uint64_t vectorStartPos, page_idx_t physicalPageIdx,
    uint16_t pagePosOfFirstElement, uint64_t numValuesToRead, table_id_t commonTableID,
    bool hasNoNullGuarantee) {
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            *fileHandle, physicalPageIdx, *wal, transaction->getType());
    bufferManager.optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) {
        if (hasNoNullGuarantee) {
            vector->setRangeNonNull(vectorStartPos, numValuesToRead);
        } else {
            readNullBitsFromAPage(
                vector, frame, pagePosOfFirstElement, vectorStartPos, numValuesToRead);
        }
        auto currentFrameHead = frame + getElemByteOffset(pagePosOfFirstElement);
        for (auto i = 0u; i < numValuesToRead; i++) {
            internalID_t internalID{0, commonTableID};
            internalID.offset = *(offset_t*)currentFrameHead;
            currentFrameHead += sizeof(offset_t);
            vector->setValue(vectorStartPos + i, internalID);
        }
    });
}

void BaseColumnOrList::readInternalIDsBySequentialCopyWithSelState(Transaction* transaction,
    ValueVector* vector, PageElementCursor& cursor,
    const std::function<page_idx_t(page_idx_t)>& logicalToPhysicalPageMapper,
    table_id_t commonTableID) {
    auto selectedState = vector->state;
    uint64_t numValuesToRead = vector->state->originalSize;
    uint64_t selectedStatePos = 0;
    uint64_t vectorPos = 0;
    while (true) {
        uint64_t numValuesInPage = numElementsPerPage - cursor.elemPosInPage;
        uint64_t numValuesToReadInPage = std::min(numValuesInPage, numValuesToRead - vectorPos);
        if (isInRange(selectedState->selVector->selectedPositions[selectedStatePos], vectorPos,
                vectorPos + numValuesToReadInPage)) {
            auto physicalPageIdx = logicalToPhysicalPageMapper(cursor.pageIdx);
            readInternalIDsFromAPageBySequentialCopy(transaction, vector, vectorPos,
                physicalPageIdx, cursor.elemPosInPage, numValuesToReadInPage, commonTableID,
                false /* hasNoNullGuarantee */);
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

void BaseColumnOrList::readBySequentialCopyWithSelState(Transaction* transaction,
    ValueVector* vector, PageElementCursor& cursor,
    const std::function<page_idx_t(page_idx_t)>& logicalToPhysicalPageMapper) {
    auto selectedState = vector->state;
    auto numValuesToRead = vector->state->originalSize;
    uint64_t selectedStatePos = 0;
    uint64_t vectorPos = 0;
    while (true) {
        uint64_t numValuesInPage = numElementsPerPage - cursor.elemPosInPage;
        uint64_t numValuesToReadInPage = std::min(numValuesInPage, numValuesToRead - vectorPos);
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

void BaseColumnOrList::readSingleNullBit(ValueVector* valueVector, const uint8_t* frame,
    uint64_t elementPos, uint64_t offsetInVector) const {
    auto inputNullEntries = (uint64_t*)getNullBufferInPage(frame);
    bool isNull = NullMask::isNull(inputNullEntries, elementPos);
    valueVector->setNull(offsetInVector, isNull);
}

void BaseColumnOrList::readAPageBySequentialCopy(Transaction* transaction, ValueVector* vector,
    uint64_t vectorStartPos, page_idx_t physicalPageIdx, uint16_t pagePosOfFirstElement,
    uint64_t numValuesToRead) {
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            *fileHandle, physicalPageIdx, *wal, transaction->getType());
    auto vectorBytesOffset = getElemByteOffset(vectorStartPos);
    auto frameBytesOffset = getElemByteOffset(pagePosOfFirstElement);
    bufferManager.optimisticRead(*fileHandleToPin, pageIdxToPin, [&](uint8_t* frame) {
        memcpy(vector->getData() + vectorBytesOffset, frame + frameBytesOffset,
            numValuesToRead * elementSize);
        readNullBitsFromAPage(
            vector, frame, pagePosOfFirstElement, vectorStartPos, numValuesToRead);
    });
}

void BaseColumnOrList::readNullBitsFromAPage(ValueVector* valueVector, const uint8_t* frame,
    uint64_t posInPage, uint64_t posInVector, uint64_t numBitsToRead) const {
    auto hasNullInSrcNullMask = NullMask::copyNullMask((uint64_t*)getNullBufferInPage(frame),
        posInPage, valueVector->getNullMaskData(), posInVector, numBitsToRead);
    if (hasNullInSrcNullMask) {
        valueVector->setMayContainNulls();
    }
}

void BaseColumnOrList::setNullBitOfAPosInFrame(
    const uint8_t* frame, uint16_t elementPosInPage, bool isNull) const {
    auto nullMask = (uint64_t*)getNullBufferInPage(frame);
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

#include "storage/storage_structure/column.h"

#include "common/in_mem_overflow_buffer_utils.h"
#include "storage/storage_structure/storage_structure_utils.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

void Column::batchLookup(const common::offset_t* nodeOffsets, size_t size, uint8_t* result) {
    for (auto i = 0u; i < size; ++i) {
        auto nodeOffset = nodeOffsets[i];
        auto cursor = PageUtils::getPageElementCursorForPos(nodeOffset, numElementsPerPage);
        auto dummyReadOnlyTransaction = Transaction::getDummyReadOnlyTrx();
        readFromPage(dummyReadOnlyTransaction.get(), cursor.pageIdx, [&](uint8_t* frame) -> void {
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
        scan(transaction, nodeIDVector, resultVector);
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
        frame = bufferManager->pin(
            *wal->fileHandle, pageIdxInWAL, BufferManager::PageReadPolicy::READ_PAGE);
    } else {
        frame = bufferManager->pin(
            *fileHandle, originalPageIdx, BufferManager::PageReadPolicy::READ_PAGE);
    }
    auto nullEntries = (uint64_t*)(frame + (elementSize * numElementsPerPage));
    auto isNull = NullMask::isNull(nullEntries, cursor.elemPosInPage);
    if (readFromWALVersionPage) {
        bufferManager->unpin(*wal->fileHandle, pageIdxInWAL);
        fileHandle->releaseWALPageIdxLock(originalPageIdx);
    } else {
        bufferManager->unpin(*fileHandle, originalPageIdx);
    }
    return isNull;
}

void Column::setNull(common::offset_t nodeOffset) {
    auto updatedPageInfoAndWALPageFrame =
        beginUpdatingPageAndWriteOnlyNullBit(nodeOffset, true /* isNull */);
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, *fileHandle, *bufferManager, *wal);
}

Value Column::readValueForTestingOnly(offset_t offset) {
    auto cursor = PageUtils::getPageElementCursorForPos(offset, numElementsPerPage);
    Value retVal = Value::createDefaultValue(dataType);
    auto dummyReadOnlyTransaction = Transaction::getDummyReadOnlyTrx();
    readFromPage(dummyReadOnlyTransaction.get(), cursor.pageIdx, [&](uint8_t* frame) {
        retVal.copyValueFrom(frame + mapElementPosToByteOffset(cursor.elemPosInPage));
    });
    return retVal;
}

void Column::lookup(Transaction* transaction, common::ValueVector* nodeIDVector,
    common::ValueVector* resultVector, uint32_t vectorPos) {
    if (nodeIDVector->isNull(vectorPos)) {
        resultVector->setNull(vectorPos, true);
        return;
    }
    auto nodeOffset = nodeIDVector->readNodeOffset(vectorPos);
    lookup(transaction, nodeOffset, resultVector, vectorPos);
}

void Column::lookup(Transaction* transaction, common::offset_t nodeOffset,
    common::ValueVector* resultVector, uint32_t vectorPos) {
    auto pageCursor = PageUtils::getPageElementCursorForPos(nodeOffset, numElementsPerPage);
    readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) {
        lookupDataFunc(transaction, frame, pageCursor, resultVector, vectorPos, numElementsPerPage,
            tableID, diskOverflowFile.get());
    });
}

void Column::scan(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
    common::ValueVector* resultVector) {
    // In sequential read, we fetch start offset regardless of selected position.
    auto startOffset = nodeIDVector->readNodeOffset(0);
    uint64_t numValuesToRead = nodeIDVector->state->originalSize;
    auto pageCursor = PageUtils::getPageElementCursorForPos(startOffset, numElementsPerPage);
    auto numValuesRead = 0u;
    auto posInSelVector = 0u;
    if (nodeIDVector->state->selVector->isUnfiltered()) {
        while (numValuesRead < numValuesToRead) {
            uint64_t numValuesToReadInPage =
                std::min((uint64_t)numElementsPerPage - pageCursor.elemPosInPage,
                    numValuesToRead - numValuesRead);
            readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
                scanDataFunc(transaction, frame, pageCursor, resultVector, numValuesRead,
                    numElementsPerPage, numValuesToReadInPage, tableID, diskOverflowFile.get());
            });
            numValuesRead += numValuesToReadInPage;
            pageCursor.nextPage();
        }
    } else {
        while (numValuesRead < numValuesToRead) {
            uint64_t numValuesToReadInPage =
                std::min((uint64_t)numElementsPerPage - pageCursor.elemPosInPage,
                    numValuesToRead - numValuesRead);
            if (isInRange(nodeIDVector->state->selVector->selectedPositions[posInSelVector],
                    numValuesRead, numValuesRead + numValuesToReadInPage)) {
                readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
                    scanDataFunc(transaction, frame, pageCursor, resultVector, numValuesRead,
                        numElementsPerPage, numValuesToReadInPage, tableID, diskOverflowFile.get());
                });
            }
            numValuesRead += numValuesToReadInPage;
            pageCursor.nextPage();
            while (
                posInSelVector < nodeIDVector->state->selVector->selectedSize &&
                nodeIDVector->state->selVector->selectedPositions[posInSelVector] < numValuesRead) {
                posInSelVector++;
            }
        }
    }
}
void Column::writeValueForSingleNodeIDPosition(
    offset_t nodeOffset, common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto updatedPageInfoAndWALPageFrame =
        beginUpdatingPage(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, *fileHandle, *bufferManager, *wal);
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

void Column::readFromPage(transaction::Transaction* transaction, common::page_idx_t pageIdx,
    const std::function<void(uint8_t*)>& func) {
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            *fileHandle, pageIdx, *wal, transaction->getType());
    bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, func);
}

WALPageIdxPosInPageAndFrame Column::beginUpdatingPageAndWriteOnlyNullBit(
    offset_t nodeOffset, bool isNull) {
    auto walPageInfo = createWALVersionOfPageIfNecessaryForElement(nodeOffset, numElementsPerPage);
    setNullBitOfAPosInFrame(walPageInfo.frame, walPageInfo.posInPage, isNull);
    return walPageInfo;
}

void Column::scanValuesFromPage(transaction::Transaction* transaction, uint8_t* frame,
    PageElementCursor& pageCursor, common::ValueVector* resultVector, uint32_t posInVector,
    uint32_t numElementsPerPage, uint32_t numValuesToRead, common::table_id_t commonTableID,
    DiskOverflowFile* diskOverflowFile) {
    auto numBytesPerValue = resultVector->getNumBytesPerValue();
    memcpy(resultVector->getData() + posInVector * numBytesPerValue,
        frame + pageCursor.elemPosInPage * numBytesPerValue, numValuesToRead * numBytesPerValue);
    auto hasNull = NullMask::copyNullMask(
        (uint64_t*)(frame + (numElementsPerPage * numBytesPerValue)), pageCursor.elemPosInPage,
        resultVector->getNullMaskData(), posInVector, numValuesToRead);
    if (hasNull) {
        resultVector->setMayContainNulls();
    }
}

void Column::lookupValueFromPage(transaction::Transaction* transaction, uint8_t* frame,
    kuzu::storage::PageElementCursor& pageCursor, common::ValueVector* resultVector,
    uint32_t posInVector, uint32_t numElementsPerPage, common::table_id_t commonTableID,
    DiskOverflowFile* diskOverflowFile) {
    auto numBytesPerValue = resultVector->getNumBytesPerValue();
    auto frameBytesOffset = pageCursor.elemPosInPage * numBytesPerValue;
    memcpy(resultVector->getData() + posInVector * numBytesPerValue, frame + frameBytesOffset,
        numBytesPerValue);
    auto isNull = NullMask::isNull(
        (uint64_t*)(frame + (numElementsPerPage * numBytesPerValue)), pageCursor.elemPosInPage);
    resultVector->setNull(posInVector, isNull);
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
                diskOverflowFile->writeStringOverflowAndUpdateOverflowPtr(
                    stringToWriteFrom, *stringToWriteTo);
            } catch (RuntimeException& e) {
                // Note: The try catch block is to make sure that we correctly unpin the WAL page
                // and release the page lock of the original page, which was acquired inside
                // `beginUpdatingPage`. Otherwise, we may cause a deadlock in other threads or tasks
                // which requires the same page lock.
                StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
                    updatedPageInfoAndWALPageFrame, *fileHandle, *bufferManager, *wal);
                throw e;
            }
        }
    }
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, *fileHandle, *bufferManager, *wal);
}

Value StringPropertyColumn::readValueForTestingOnly(offset_t offset) {
    ku_string_t kuString;
    auto cursor = PageUtils::getPageElementCursorForPos(offset, numElementsPerPage);
    auto dummyReadOnlyTransaction = Transaction::getDummyReadOnlyTrx();
    readFromPage(dummyReadOnlyTransaction.get(), cursor.pageIdx, [&](uint8_t* frame) -> void {
        memcpy(&kuString, frame + mapElementPosToByteOffset(cursor.elemPosInPage),
            sizeof(ku_string_t));
    });
    return Value(diskOverflowFile->readString(TransactionType::READ_ONLY, kuString));
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
            diskOverflowFile->writeListOverflowAndUpdateOverflowPtr(
                kuListToWriteFrom, *kuListToWriteTo, vectorToWriteFrom->dataType);
        } catch (RuntimeException& e) {
            // Note: The try catch block is to make sure that we correctly unpin the WAL page
            // and release the page lock of the original page, which was acquired inside
            // `beginUpdatingPage`. Otherwise, we may cause a deadlock in other threads or tasks
            // which requires the same page lock.
            StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
                updatedPageInfoAndWALPageFrame, *fileHandle, *bufferManager, *wal);
            throw e;
        }
    }
    StorageStructureUtils::unpinWALPageAndReleaseOriginalPageLock(
        updatedPageInfoAndWALPageFrame, *fileHandle, *bufferManager, *wal);
}

Value ListPropertyColumn::readValueForTestingOnly(offset_t offset) {
    ku_list_t kuList;
    auto cursor = PageUtils::getPageElementCursorForPos(offset, numElementsPerPage);
    auto dummyReadOnlyTransaction = Transaction::getDummyReadOnlyTrx();
    readFromPage(dummyReadOnlyTransaction.get(), cursor.pageIdx, [&](uint8_t* frame) -> void {
        memcpy(&kuList, frame + mapElementPosToByteOffset(cursor.elemPosInPage), sizeof(ku_list_t));
    });
    return Value(
        dataType, diskOverflowFile->readList(TransactionType::READ_ONLY, kuList, dataType));
}

void ListPropertyColumn::scanListsFromPage(transaction::Transaction* transaction, uint8_t* frame,
    kuzu::storage::PageElementCursor& pageCursor, common::ValueVector* resultVector,
    uint32_t posInVector, uint32_t numElementsPerPage, uint32_t numValuesToRead,
    common::table_id_t commonTableID, DiskOverflowFile* diskOverflowFile) {
    auto frameBytesOffset = pageCursor.elemPosInPage * sizeof(ku_list_t);
    auto kuListsToRead = reinterpret_cast<common::ku_list_t*>(frame + frameBytesOffset);
    auto hasNull = NullMask::copyNullMask(
        (uint64_t*)(frame + (numElementsPerPage * sizeof(ku_list_t))), pageCursor.elemPosInPage,
        resultVector->getNullMaskData(), posInVector, numValuesToRead);
    if (hasNull) {
        resultVector->setMayContainNulls();
    }
    for (auto i = 0u; i < numValuesToRead; i++) {
        if (!resultVector->isNull(posInVector + i)) {
            diskOverflowFile->readListToVector(
                transaction->getType(), kuListsToRead[i], resultVector, posInVector + i);
        }
    }
}

void ListPropertyColumn::lookupListFromPage(transaction::Transaction* transaction, uint8_t* frame,
    kuzu::storage::PageElementCursor& pageCursor, common::ValueVector* resultVector,
    uint32_t posInVector, uint32_t numElementsPerPage, common::table_id_t commonTableID,
    DiskOverflowFile* diskOverflowFile) {
    auto isNull = NullMask::isNull(
        (uint64_t*)(frame + (numElementsPerPage * sizeof(ku_list_t))), pageCursor.elemPosInPage);
    resultVector->setNull(posInVector, isNull);
    if (!resultVector->isNull(posInVector)) {
        auto frameBytesOffset = pageCursor.elemPosInPage * sizeof(ku_list_t);
        diskOverflowFile->readListToVector(transaction->getType(),
            *(common::ku_list_t*)(frame + frameBytesOffset), resultVector, posInVector);
    }
}

StructPropertyColumn::StructPropertyColumn(const StorageStructureIDAndFName& structureIDAndFName,
    const common::DataType& dataType, BufferManager* bufferManager, WAL* wal)
    : Column{dataType} {
    auto structFields =
        reinterpret_cast<StructTypeInfo*>(dataType.getExtraTypeInfo())->getStructFields();
    for (auto structField : structFields) {
        auto fieldStructureIDAndFName = structureIDAndFName;
        fieldStructureIDAndFName.fName = StorageUtils::appendStructFieldName(
            fieldStructureIDAndFName.fName, structField->getName());
        structFieldColumns.push_back(ColumnFactory::getColumn(
            fieldStructureIDAndFName, *structField->getType(), bufferManager, wal));
    }
}

void StructPropertyColumn::read(Transaction* transaction, common::ValueVector* nodeIDVector,
    common::ValueVector* resultVector) {
    // TODO(Guodong/Ziyi): We currently do not support null struct value.
    resultVector->setAllNonNull();
    for (auto i = 0u; i < structFieldColumns.size(); i++) {
        structFieldColumns[i]->read(
            transaction, nodeIDVector, common::StructVector::getChildVector(resultVector, i).get());
    }
}

void InternalIDColumn::scanInternalIDsFromPage(transaction::Transaction* transaction,
    uint8_t* frame, PageElementCursor& pageCursor, common::ValueVector* resultVector,
    uint32_t posInVector, uint32_t numElementsPerPage, uint32_t numValuesToRead,
    table_id_t commonTableID, DiskOverflowFile* diskOverflowFile) {
    auto numBytesPerValue = sizeof(offset_t);
    auto resultData = (internalID_t*)resultVector->getData();
    for (auto i = 0u; i < numValuesToRead; i++) {
        auto posInFrame = pageCursor.elemPosInPage + i;
        resultData[posInVector + i].offset = *(offset_t*)(frame + (posInFrame * numBytesPerValue));
        resultData[posInVector + i].tableID = commonTableID;
    }
    auto hasNull = NullMask::copyNullMask(
        (uint64_t*)(frame + (numElementsPerPage * numBytesPerValue)), pageCursor.elemPosInPage,
        resultVector->getNullMaskData(), posInVector, numValuesToRead);
    if (hasNull) {
        resultVector->setMayContainNulls();
    }
}

void InternalIDColumn::lookupInternalIDFromPage(transaction::Transaction* transaction,
    uint8_t* frame, kuzu::storage::PageElementCursor& pageCursor, common::ValueVector* resultVector,
    uint32_t posInVector, uint32_t numElementsPerPage, common::table_id_t commonTableID,
    DiskOverflowFile* diskOverflowFile) {
    auto numBytesPerValue = sizeof(offset_t);
    auto resultData = (internalID_t*)resultVector->getData();
    resultData[posInVector].offset =
        *(offset_t*)(frame + (pageCursor.elemPosInPage * numBytesPerValue));
    resultData[posInVector].tableID = commonTableID;
    auto isNull = NullMask::isNull(
        (uint64_t*)(frame + (numElementsPerPage * numBytesPerValue)), pageCursor.elemPosInPage);
    resultVector->setNull(posInVector, isNull);
}

void SerialColumn::read(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
    common::ValueVector* resultVector) {
    // Serial column cannot contain null values.
    for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        auto offset = nodeIDVector->readNodeOffset(pos);
        resultVector->setValue<offset_t>(pos, offset);
    }
}

} // namespace storage
} // namespace kuzu

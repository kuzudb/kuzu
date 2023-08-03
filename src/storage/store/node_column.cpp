#include "storage/store/node_column.h"

#include "storage/storage_structure/storage_structure.h"
#include "storage/store/string_node_column.h"
#include "storage/store/struct_node_column.h"
#include "storage/store/var_list_node_column.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

void FixedSizedNodeColumnFunc::readValuesFromPage(uint8_t* frame, PageElementCursor& pageCursor,
    ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead) {
    auto numBytesPerValue = resultVector->getNumBytesPerValue();
    memcpy(resultVector->getData() + posInVector * numBytesPerValue,
        frame + pageCursor.elemPosInPage * numBytesPerValue, numValuesToRead * numBytesPerValue);
}

void FixedSizedNodeColumnFunc::writeValueToPage(
    uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector, uint32_t posInVector) {
    auto numBytesPerValue = vector->getNumBytesPerValue();
    memcpy(frame + posInFrame * numBytesPerValue,
        vector->getData() + posInVector * numBytesPerValue, numBytesPerValue);
}

void FixedSizedNodeColumnFunc::readInternalIDValuesFromPage(uint8_t* frame,
    PageElementCursor& pageCursor, ValueVector* resultVector, uint32_t posInVector,
    uint32_t numValuesToRead) {
    auto resultData = (internalID_t*)resultVector->getData();
    for (auto i = 0u; i < numValuesToRead; i++) {
        auto posInFrame = pageCursor.elemPosInPage + i;
        resultData[posInVector + i].offset = *(offset_t*)(frame + (posInFrame * sizeof(offset_t)));
    }
}

void FixedSizedNodeColumnFunc::writeInternalIDValueToPage(
    uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector, uint32_t posInVector) {
    auto relID = vector->getValue<relID_t>(posInVector);
    memcpy(frame + posInFrame * sizeof(offset_t), &relID.offset, sizeof(offset_t));
}

void NullNodeColumnFunc::readValuesFromPage(uint8_t* frame, PageElementCursor& pageCursor,
    ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead) {
    for (auto i = 0u; i < numValuesToRead; i++) {
        bool isNull = *(frame + pageCursor.elemPosInPage + i);
        resultVector->setNull(posInVector + i, isNull);
    }
}

void NullNodeColumnFunc::writeValueToPage(
    uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector, uint32_t posInVector) {
    *(frame + posInFrame) = vector->isNull(posInVector);
}

NodeColumn::NodeColumn(const Property& property, BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, bool requireNullColumn)
    : NodeColumn{*property.getDataType(), *property.getMetadataDAHInfo(), dataFH, metadataFH,
          bufferManager, wal, requireNullColumn} {}

NodeColumn::NodeColumn(LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
    BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
    bool requireNullColumn)
    : storageStructureID{StorageStructureID::newDataID()}, dataType{std::move(dataType)},
      dataFH{dataFH}, metadataFH{metadataFH}, bufferManager{bufferManager}, wal{wal} {
    metadataDA = std::make_unique<InMemDiskArray<ColumnChunkMetadata>>(*metadataFH,
        StorageStructureID::newMetadataID(), metaDAHeaderInfo.dataDAHPageIdx, bufferManager, wal);
    numBytesPerFixedSizedValue = ColumnChunk::getDataTypeSizeInChunk(this->dataType);
    assert(numBytesPerFixedSizedValue <= BufferPoolConstants::PAGE_4KB_SIZE);
    numValuesPerPage =
        numBytesPerFixedSizedValue == 0 ?
            0 :
            PageUtils::getNumElementsInAPage(numBytesPerFixedSizedValue, false /* hasNull */);
    readNodeColumnFunc = this->dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID ?
                             FixedSizedNodeColumnFunc::readInternalIDValuesFromPage :
                             FixedSizedNodeColumnFunc::readValuesFromPage;
    writeNodeColumnFunc = this->dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID ?
                              FixedSizedNodeColumnFunc::writeInternalIDValueToPage :
                              FixedSizedNodeColumnFunc::writeValueToPage;
    if (requireNullColumn) {
        nullColumn = std::make_unique<NullNodeColumn>(
            metaDAHeaderInfo.nullDAHPageIdx, dataFH, metadataFH, bufferManager, wal);
    }
}

void NodeColumn::batchLookup(const offset_t* nodeOffsets, size_t size, uint8_t* result) {
    auto dummyReadOnlyTransaction = Transaction::getDummyReadOnlyTrx();
    for (auto i = 0u; i < size; ++i) {
        auto nodeOffset = nodeOffsets[i];
        auto nodeGroupIdx = getNodeGroupIdxFromNodeOffset(nodeOffset);
        auto cursor = PageUtils::getPageElementCursorForPos(nodeOffset, numValuesPerPage);
        cursor.pageIdx +=
            metadataDA->get(nodeGroupIdx, dummyReadOnlyTransaction->getType()).pageIdx;
        readFromPage(dummyReadOnlyTransaction.get(), cursor.pageIdx, [&](uint8_t* frame) -> void {
            memcpy(result + i * numBytesPerFixedSizedValue,
                frame + (cursor.elemPosInPage * numBytesPerFixedSizedValue),
                numBytesPerFixedSizedValue);
        });
    }
}

void NodeColumn::scan(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    nullColumn->scan(transaction, nodeIDVector, resultVector);
    scanInternal(transaction, nodeIDVector, resultVector);
}

void NodeColumn::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    assert(startNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
    auto nodeGroupIdx = getNodeGroupIdxFromNodeOffset(startNodeOffset);
    auto offsetInNodeGroup =
        startNodeOffset - StorageUtils::getStartOffsetForNodeGroup(nodeGroupIdx);
    auto pageCursor = PageUtils::getPageElementCursorForPos(offsetInNodeGroup, numValuesPerPage);
    auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
    pageCursor.pageIdx += chunkMeta.pageIdx;
    if (nodeIDVector->state->selVector->isUnfiltered()) {
        scanUnfiltered(
            transaction, pageCursor, nodeIDVector->state->selVector->selectedSize, resultVector);
    } else {
        scanFiltered(transaction, pageCursor, nodeIDVector, resultVector);
    }
}

void NodeColumn::scanUnfiltered(Transaction* transaction, PageElementCursor& pageCursor,
    uint64_t numValuesToScan, ValueVector* resultVector, uint64_t startPosInVector) {
    uint64_t numValuesScanned = 0;
    while (numValuesScanned < numValuesToScan) {
        uint64_t numValuesToScanInPage =
            std::min((uint64_t)numValuesPerPage - pageCursor.elemPosInPage,
                numValuesToScan - numValuesScanned);
        readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
            readNodeColumnFunc(frame, pageCursor, resultVector, numValuesScanned + startPosInVector,
                numValuesToScanInPage);
        });
        numValuesScanned += numValuesToScanInPage;
        pageCursor.nextPage();
    }
}

void NodeColumn::scanFiltered(Transaction* transaction, PageElementCursor& pageCursor,
    ValueVector* nodeIDVector, ValueVector* resultVector) {
    auto numValuesToScan = nodeIDVector->state->originalSize;
    auto numValuesScanned = 0u;
    auto posInSelVector = 0u;
    while (numValuesScanned < numValuesToScan) {
        uint64_t numValuesToScanInPage =
            std::min((uint64_t)numValuesPerPage - pageCursor.elemPosInPage,
                numValuesToScan - numValuesScanned);
        if (StorageStructure::isInRange(
                nodeIDVector->state->selVector->selectedPositions[posInSelVector], numValuesScanned,
                numValuesScanned + numValuesToScanInPage)) {
            readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
                readNodeColumnFunc(
                    frame, pageCursor, resultVector, numValuesScanned, numValuesToScanInPage);
            });
        }
        numValuesScanned += numValuesToScanInPage;
        pageCursor.nextPage();
        while (
            posInSelVector < nodeIDVector->state->selVector->selectedSize &&
            nodeIDVector->state->selVector->selectedPositions[posInSelVector] < numValuesScanned) {
            posInSelVector++;
        }
    }
}

void NodeColumn::lookup(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    nullColumn->lookup(transaction, nodeIDVector, resultVector);
    lookupInternal(transaction, nodeIDVector, resultVector);
}

void NodeColumn::lookupInternal(
    transaction::Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    if (nodeIDVector->state->isFlat()) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[0];
        if (nodeIDVector->isNull(pos)) {
            return;
        }
        auto nodeOffset = nodeIDVector->readNodeOffset(pos);
        lookupValue(transaction, nodeOffset, resultVector, pos);
    } else {
        for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            if (nodeIDVector->isNull(pos)) {
                continue;
            }
            auto nodeOffset = nodeIDVector->readNodeOffset(pos);
            lookupValue(transaction, nodeOffset, resultVector, pos);
        }
    }
}

void NodeColumn::lookupValue(transaction::Transaction* transaction, common::offset_t nodeOffset,
    common::ValueVector* resultVector, uint32_t posInVector) {
    auto nodeGroupIdx = getNodeGroupIdxFromNodeOffset(nodeOffset);
    auto pageCursor = PageUtils::getPageElementCursorForPos(nodeOffset, numValuesPerPage);
    pageCursor.pageIdx += metadataDA->get(nodeGroupIdx, transaction->getType()).pageIdx;
    readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
        readNodeColumnFunc(frame, pageCursor, resultVector, posInVector, 1 /* numValuesToRead */);
    });
}

void NodeColumn::readFromPage(
    Transaction* transaction, page_idx_t pageIdx, const std::function<void(uint8_t*)>& func) {
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            *dataFH, pageIdx, *wal, transaction->getType());
    bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, func);
}

void NodeColumn::write(ValueVector* nodeIDVector, ValueVector* vectorToWriteFrom) {
    if (nodeIDVector->state->isFlat() && vectorToWriteFrom->state->isFlat()) {
        auto nodeOffset =
            nodeIDVector->readNodeOffset(nodeIDVector->state->selVector->selectedPositions[0]);
        writeInternal(nodeOffset, vectorToWriteFrom,
            vectorToWriteFrom->state->selVector->selectedPositions[0]);
    } else if (nodeIDVector->state->isFlat() && !vectorToWriteFrom->state->isFlat()) {
        auto nodeOffset =
            nodeIDVector->readNodeOffset(nodeIDVector->state->selVector->selectedPositions[0]);
        auto lastPos = vectorToWriteFrom->state->selVector->selectedSize - 1;
        writeInternal(nodeOffset, vectorToWriteFrom, lastPos);
    } else if (!nodeIDVector->state->isFlat() && vectorToWriteFrom->state->isFlat()) {
        for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
            auto nodeOffset =
                nodeIDVector->readNodeOffset(nodeIDVector->state->selVector->selectedPositions[i]);
            writeInternal(nodeOffset, vectorToWriteFrom,
                vectorToWriteFrom->state->selVector->selectedPositions[0]);
        }
    } else if (!nodeIDVector->state->isFlat() && !vectorToWriteFrom->state->isFlat()) {
        for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; ++i) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            auto nodeOffset = nodeIDVector->readNodeOffset(pos);
            writeInternal(nodeOffset, vectorToWriteFrom, pos);
        }
    }
}

page_idx_t NodeColumn::append(
    ColumnChunk* columnChunk, common::page_idx_t startPageIdx, uint64_t nodeGroupIdx) {
    // Main column chunk.
    page_idx_t numPagesFlushed = 0;
    auto numPagesForChunk = columnChunk->flushBuffer(dataFH, startPageIdx);
    metadataDA->resize(nodeGroupIdx + 1);
    metadataDA->update(nodeGroupIdx, ColumnChunkMetadata{startPageIdx, numPagesForChunk});
    numPagesFlushed += numPagesForChunk;
    startPageIdx += numPagesForChunk;
    // Null column chunk.
    auto numPagesForNullChunk =
        nullColumn->append(columnChunk->getNullChunk(), startPageIdx, nodeGroupIdx);
    numPagesFlushed += numPagesForNullChunk;
    startPageIdx += numPagesForNullChunk;
    // Children column chunks.
    assert(childrenColumns.size() == columnChunk->getNumChildren());
    for (auto i = 0u; i < childrenColumns.size(); i++) {
        auto numPagesForChild =
            childrenColumns[i]->append(columnChunk->getChild(i), startPageIdx, nodeGroupIdx);
        numPagesFlushed += numPagesForChild;
        startPageIdx += numPagesForChild;
    }
    return numPagesFlushed;
}

void NodeColumn::writeInternal(
    offset_t nodeOffset, ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    nullColumn->writeInternal(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
    bool isNull = vectorToWriteFrom->isNull(posInVectorToWriteFrom);
    if (isNull) {
        return;
    }
    writeValue(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
}

void NodeColumn::writeValue(common::offset_t nodeOffset, common::ValueVector* vectorToWriteFrom,
    uint32_t posInVectorToWriteFrom) {
    auto walPageInfo = createWALVersionOfPageForValue(nodeOffset);
    try {
        writeNodeColumnFunc(
            walPageInfo.frame, walPageInfo.posInPage, vectorToWriteFrom, posInVectorToWriteFrom);
    } catch (Exception& e) {
        bufferManager->unpin(*wal->fileHandle, walPageInfo.pageIdxInWAL);
        dataFH->releaseWALPageIdxLock(walPageInfo.originalPageIdx);
        throw;
    }
    bufferManager->unpin(*wal->fileHandle, walPageInfo.pageIdxInWAL);
    dataFH->releaseWALPageIdxLock(walPageInfo.originalPageIdx);
}

void NodeColumn::addNewPageToDataFH() {
    auto pageIdxInOriginalFile = dataFH->addNewPage();
    auto pageIdxInWAL = wal->logPageInsertRecord(storageStructureID, pageIdxInOriginalFile);
    bufferManager->pin(
        *wal->fileHandle, pageIdxInWAL, BufferManager::PageReadPolicy::DONT_READ_PAGE);
    dataFH->addWALPageIdxGroupIfNecessary(pageIdxInOriginalFile);
    dataFH->setWALPageIdx(pageIdxInOriginalFile, pageIdxInWAL);
    wal->fileHandle->setLockedPageDirty(pageIdxInWAL);
    bufferManager->unpin(*wal->fileHandle, pageIdxInWAL);
}

WALPageIdxPosInPageAndFrame NodeColumn::createWALVersionOfPageForValue(offset_t nodeOffset) {
    auto nodeGroupIdx = getNodeGroupIdxFromNodeOffset(nodeOffset);
    auto originalPageCursor = PageUtils::getPageElementCursorForPos(nodeOffset, numValuesPerPage);
    originalPageCursor.pageIdx += metadataDA->get(nodeGroupIdx, TransactionType::WRITE).pageIdx;
    bool insertingNewPage = false;
    if (originalPageCursor.pageIdx >= dataFH->getNumPages()) {
        assert(originalPageCursor.pageIdx == dataFH->getNumPages());
        addNewPageToDataFH();
        insertingNewPage = true;
    }
    auto walPageIdxAndFrame =
        StorageStructureUtils::createWALVersionIfNecessaryAndPinPage(originalPageCursor.pageIdx,
            insertingNewPage, *dataFH, storageStructureID, *bufferManager, *wal);
    return {walPageIdxAndFrame, originalPageCursor.elemPosInPage};
}

void NodeColumn::setNull(offset_t nodeOffset) {
    if (nullColumn) {
        nullColumn->setNull(nodeOffset);
    }
}

void NodeColumn::checkpointInMemory() {
    metadataDA->checkpointInMemoryIfNecessary();
    for (auto& child : childrenColumns) {
        child->checkpointInMemory();
    }
    if (nullColumn) {
        nullColumn->checkpointInMemory();
    }
}

void NodeColumn::rollbackInMemory() {
    metadataDA->rollbackInMemoryIfNecessary();
    for (auto& child : childrenColumns) {
        child->rollbackInMemory();
    }
    if (nullColumn) {
        nullColumn->rollbackInMemory();
    }
}

void NodeColumn::scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
    common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
    common::ValueVector* resultVector, uint64_t offsetInVector) {
    if (nullColumn) {
        nullColumn->scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup,
            resultVector, offsetInVector);
    }
    auto pageCursor = PageUtils::getPageElementCursorForPos(startOffsetInGroup, numValuesPerPage);
    auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
    pageCursor.pageIdx += chunkMeta.pageIdx;
    auto numValuesToScan = endOffsetInGroup - startOffsetInGroup;
    scanUnfiltered(transaction, pageCursor, numValuesToScan, resultVector, offsetInVector);
}

NullNodeColumn::NullNodeColumn(page_idx_t metaDAHeaderPageIdx, BMFileHandle* nodeGroupsDataFH,
    BMFileHandle* nodeGroupsMetaFH, BufferManager* bufferManager, WAL* wal)
    : NodeColumn{LogicalType(LogicalTypeID::BOOL), MetadataDAHInfo{metaDAHeaderPageIdx},
          nodeGroupsDataFH, nodeGroupsMetaFH, bufferManager, wal, false /* requireNullColumn */} {
    readNodeColumnFunc = NullNodeColumnFunc::readValuesFromPage;
    writeNodeColumnFunc = NullNodeColumnFunc::writeValueToPage;
}

void NullNodeColumn::scan(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    scanInternal(transaction, nodeIDVector, resultVector);
}

void NullNodeColumn::lookup(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    lookupInternal(transaction, nodeIDVector, resultVector);
}

page_idx_t NullNodeColumn::append(
    ColumnChunk* columnChunk, common::page_idx_t startPageIdx, uint64_t nodeGroupIdx) {
    auto numPagesFlushed = columnChunk->flushBuffer(dataFH, startPageIdx);
    metadataDA->resize(nodeGroupIdx + 1);
    metadataDA->update(nodeGroupIdx, ColumnChunkMetadata{startPageIdx, numPagesFlushed});
    return numPagesFlushed;
}

void NullNodeColumn::setNull(common::offset_t nodeOffset) {
    auto walPageInfo = createWALVersionOfPageForValue(nodeOffset);
    try {
        *(walPageInfo.frame + walPageInfo.posInPage) = true;
    } catch (Exception& e) {
        bufferManager->unpin(*wal->fileHandle, walPageInfo.pageIdxInWAL);
        dataFH->releaseWALPageIdxLock(walPageInfo.originalPageIdx);
        throw;
    }
    bufferManager->unpin(*wal->fileHandle, walPageInfo.pageIdxInWAL);
    dataFH->releaseWALPageIdxLock(walPageInfo.originalPageIdx);
}

void NullNodeColumn::writeInternal(
    offset_t nodeOffset, ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    writeValue(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
}

SerialNodeColumn::SerialNodeColumn(const catalog::MetadataDAHInfo& metaDAHeaderInfo,
    BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal)
    : NodeColumn{LogicalType(LogicalTypeID::SERIAL), metaDAHeaderInfo, dataFH, metadataFH,
          bufferManager, wal, false} {}

void SerialNodeColumn::scan(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    // Serial column cannot contain null values.
    for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        auto offset = nodeIDVector->readNodeOffset(pos);
        resultVector->setValue<offset_t>(pos, offset);
    }
}

void SerialNodeColumn::lookup(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    if (nodeIDVector->state->isFlat()) {
        // Serial column cannot contain null values.
        auto pos = nodeIDVector->state->selVector->selectedPositions[0];
        auto offset = nodeIDVector->readNodeOffset(pos);
        resultVector->setValue<offset_t>(pos, offset);
    } else {
        // Serial column cannot contain null values.
        for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            auto offset = nodeIDVector->readNodeOffset(pos);
            resultVector->setValue<offset_t>(pos, offset);
        }
    }
}

page_idx_t SerialNodeColumn::append(
    ColumnChunk* columnChunk, common::page_idx_t startPageIdx, uint64_t nodeGroupIdx) {
    // DO NOTHING.
    return 0;
}

std::unique_ptr<NodeColumn> NodeColumnFactory::createNodeColumn(const LogicalType& dataType,
    const catalog::MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH,
    BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL:
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT16:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DATE:
    case LogicalTypeID::TIMESTAMP:
    case LogicalTypeID::INTERVAL:
    case LogicalTypeID::INTERNAL_ID:
    case LogicalTypeID::FIXED_LIST: {
        return std::make_unique<NodeColumn>(
            dataType, metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, true);
    }
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING:
        return std::make_unique<StringNodeColumn>(
            dataType, metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal);
    case LogicalTypeID::VAR_LIST: {
        return std::make_unique<VarListNodeColumn>(
            dataType, metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal);
    }
    case LogicalTypeID::STRUCT: {
        return std::make_unique<StructNodeColumn>(
            dataType, metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal);
    }
    case LogicalTypeID::SERIAL: {
        return std::make_unique<SerialNodeColumn>(
            metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal);
    }
    default: {
        throw NotImplementedException("NodeColumnFactory::createNodeColumn");
    }
    }
}

} // namespace storage
} // namespace kuzu

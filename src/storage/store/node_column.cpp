#include "storage/store/node_column.h"

#include "storage/storage_structure/storage_structure.h"
#include "storage/store/property_statistics.h"
#include "storage/store/string_node_column.h"
#include "storage/store/struct_node_column.h"
#include "storage/store/table_statistics.h"
#include "storage/store/var_list_node_column.h"
#include "transaction/transaction.h"

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
    uint8_t* frame, uint16_t posInFrame, ValueVector* vector, uint32_t posInVector) {
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
    uint8_t* frame, uint16_t posInFrame, ValueVector* vector, uint32_t posInVector) {
    auto relID = vector->getValue<relID_t>(posInVector);
    memcpy(frame + posInFrame * sizeof(offset_t), &relID.offset, sizeof(offset_t));
}

void NullNodeColumnFunc::readValuesFromPage(uint8_t* frame, PageElementCursor& pageCursor,
    ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead) {
    // Read bit-packed null flags from the frame into the result vector
    // Casting to uint64_t should be safe as long as the page size is a multiple of 8 bytes.
    // Otherwise, it could read off the end of the page.
    resultVector->setNullFromBits(
        (uint64_t*)frame, pageCursor.elemPosInPage, posInVector, numValuesToRead);
}

void NullNodeColumnFunc::writeValueToPage(
    uint8_t* frame, uint16_t posInFrame, ValueVector* vector, uint32_t posInVector) {
    // Casting to uint64_t should be safe as long as the page size is a multiple of 8 bytes.
    // Otherwise, it could read off the end of the page.
    NullMask::setNull(
        (uint64_t*)frame, posInFrame, NullMask::isNull(vector->getNullMaskData(), posInVector));
}

void BoolNodeColumnFunc::readValuesFromPage(uint8_t* frame, PageElementCursor& pageCursor,
    ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead) {
    // Read bit-packed null flags from the frame into the result vector
    // Casting to uint64_t should be safe as long as the page size is a multiple of 8 bytes.
    // Otherwise, it could read off the end of the page.
    //
    // Currently, the frame stores bitpacked bools, but the value_vector does not
    for (auto i = 0; i < numValuesToRead; i++) {
        resultVector->setValue(
            posInVector + i, NullMask::isNull((uint64_t*)frame, pageCursor.elemPosInPage + i));
    }
}

void BoolNodeColumnFunc::writeValueToPage(
    uint8_t* frame, uint16_t posInFrame, ValueVector* vector, uint32_t posInVector) {
    // Casting to uint64_t should be safe as long as the page size is a multiple of 8 bytes.
    // Otherwise, it could read/write off the end of the page.
    NullMask::copyNullMask(
        vector->getValue<bool>(posInVector) ? &NullMask::ALL_NULL_ENTRY : &NullMask::NO_NULL_ENTRY,
        posInVector, (uint64_t*)frame, posInFrame, 1);
}

NodeColumn::NodeColumn(const Property& property, BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, Transaction* transaction,
    RWPropertyStats propertyStatistics, bool requireNullColumn)
    : NodeColumn{*property.getDataType(), *property.getMetadataDAHInfo(), dataFH, metadataFH,
          bufferManager, wal, transaction, propertyStatistics, requireNullColumn} {}

NodeColumn::NodeColumn(LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
    BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
    transaction::Transaction* transaction, RWPropertyStats propertyStatistics,
    bool requireNullColumn)
    : storageStructureID{StorageStructureID::newDataID()}, dataType{std::move(dataType)},
      dataFH{dataFH}, metadataFH{metadataFH}, bufferManager{bufferManager},
      propertyStatistics{propertyStatistics}, wal{wal} {
    metadataDA = std::make_unique<InMemDiskArray<ColumnChunkMetadata>>(*metadataFH,
        StorageStructureID::newMetadataID(), metaDAHeaderInfo.dataDAHPageIdx, bufferManager, wal,
        transaction);
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
        nullColumn = std::make_unique<NullNodeColumn>(metaDAHeaderInfo.nullDAHPageIdx, dataFH,
            metadataFH, bufferManager, wal, transaction, propertyStatistics);
    }
}

void NodeColumn::batchLookup(
    Transaction* transaction, const offset_t* nodeOffsets, size_t size, uint8_t* result) {
    for (auto i = 0u; i < size; ++i) {
        auto nodeOffset = nodeOffsets[i];
        auto cursor = getPageCursorForOffset(transaction->getType(), nodeOffset);
        readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
            memcpy(result + i * numBytesPerFixedSizedValue,
                frame + (cursor.elemPosInPage * numBytesPerFixedSizedValue),
                numBytesPerFixedSizedValue);
        });
    }
}

void BoolNodeColumn::batchLookup(
    Transaction* transaction, const offset_t* nodeOffsets, size_t size, uint8_t* result) {
    for (auto i = 0u; i < size; ++i) {
        auto nodeOffset = nodeOffsets[i];
        auto cursor = getPageCursorForOffset(transaction->getType(), nodeOffset);
        readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
            // De-compress bitpacked bools
            result[i] = NullMask::isNull((uint64_t*)frame, cursor.elemPosInPage);
        });
    }
}

void NodeColumn::scan(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    nullColumn->scan(transaction, nodeIDVector, resultVector);
    scanInternal(transaction, nodeIDVector, resultVector);
}

void NodeColumn::scan(transaction::Transaction* transaction, node_group_idx_t nodeGroupIdx,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
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

void NodeColumn::scan(node_group_idx_t nodeGroupIdx, ColumnChunk* columnChunk) {
    if (nullColumn) {
        nullColumn->scan(nodeGroupIdx, columnChunk->getNullChunk());
    }
    if (nodeGroupIdx >= metadataDA->getNumElements()) {
        columnChunk->setNumValues(0);
    } else {
        auto chunkMetadata = metadataDA->get(nodeGroupIdx, TransactionType::WRITE);
        FileUtils::readFromFile(dataFH->getFileInfo(), columnChunk->getData(),
            columnChunk->getNumBytes(), chunkMetadata.pageIdx * BufferPoolConstants::PAGE_4KB_SIZE);
        columnChunk->setNumValues(chunkMetadata.numValues);
    }
}

void NodeColumn::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    assert(startNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
    auto cursor = getPageCursorForOffset(transaction->getType(), startNodeOffset);
    if (nodeIDVector->state->selVector->isUnfiltered()) {
        scanUnfiltered(
            transaction, cursor, nodeIDVector->state->selVector->selectedSize, resultVector);
    } else {
        scanFiltered(transaction, cursor, nodeIDVector, resultVector);
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
    auto numValuesToScan = nodeIDVector->state->getOriginalSize();
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
    for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        if (nodeIDVector->isNull(pos)) {
            continue;
        }
        auto nodeOffset = nodeIDVector->readNodeOffset(pos);
        lookupValue(transaction, nodeOffset, resultVector, pos);
    }
}

void NodeColumn::lookupValue(transaction::Transaction* transaction, offset_t nodeOffset,
    ValueVector* resultVector, uint32_t posInVector) {
    auto cursor = getPageCursorForOffset(transaction->getType(), nodeOffset);
    readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
        readNodeColumnFunc(frame, cursor, resultVector, posInVector, 1 /* numValuesToRead */);
    });
}

void NodeColumn::readFromPage(
    Transaction* transaction, page_idx_t pageIdx, const std::function<void(uint8_t*)>& func) {
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            *dataFH, pageIdx, *wal, transaction->getType());
    bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, func);
}

page_idx_t NodeColumn::append(
    ColumnChunk* columnChunk, page_idx_t startPageIdx, uint64_t nodeGroupIdx) {
    // Main column chunk.
    page_idx_t numPagesFlushed = 0;
    auto numPagesForChunk = columnChunk->flushBuffer(dataFH, startPageIdx);
    ColumnChunkMetadata metadata{startPageIdx, numPagesForChunk, columnChunk->getNumValues()};
    metadataDA->resize(nodeGroupIdx + 1);
    metadataDA->update(nodeGroupIdx, metadata);
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

void NodeColumn::writeInternal(
    offset_t nodeOffset, ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    nullColumn->writeInternal(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
    bool isNull = vectorToWriteFrom->isNull(posInVectorToWriteFrom);
    if (isNull) {
        return;
    }
    writeValue(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
}

void NodeColumn::writeValue(
    offset_t nodeOffset, ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
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

WALPageIdxPosInPageAndFrame NodeColumn::createWALVersionOfPageForValue(offset_t nodeOffset) {
    auto originalPageCursor = getPageCursorForOffset(TransactionType::WRITE, nodeOffset);
    bool insertingNewPage = false;
    if (originalPageCursor.pageIdx >= dataFH->getNumPages()) {
        assert(originalPageCursor.pageIdx == dataFH->getNumPages());
        StorageStructureUtils::insertNewPage(*dataFH, storageStructureID, *bufferManager, *wal);
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

void NodeColumn::populateWithDefaultVal(const Property& property, NodeColumn* nodeColumn,
    ValueVector* defaultValueVector, uint64_t numNodeGroups) {
    auto columnChunk = ColumnChunkFactory::createColumnChunk(
        *property.getDataType(), nullptr /* copyDescription */);
    columnChunk->populateWithDefaultVal(defaultValueVector);
    for (auto i = 0u; i < numNodeGroups; i++) {
        auto numPages = columnChunk->getNumPages();
        auto startPageIdx = dataFH->addNewPages(numPages);
        nodeColumn->append(columnChunk.get(), startPageIdx, i);
    }
}

PageElementCursor NodeColumn::getPageCursorForOffset(
    TransactionType transactionType, offset_t nodeOffset) {
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto offsetInNodeGroup = nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    auto pageCursor = PageUtils::getPageElementCursorForPos(offsetInNodeGroup, numValuesPerPage);
    pageCursor.pageIdx += metadataDA->get(nodeGroupIdx, transactionType).pageIdx;
    return pageCursor;
}

// Page size must be aligned to 8 byte chunks for the 64-bit NullMask algorithms to work
// without the possibility of memory errors from reading/writing off the end of a page.
static_assert(PageUtils::getNumElementsInAPage(1, false /*requireNullColumn*/) % 8 == 0);

BoolNodeColumn::BoolNodeColumn(const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH,
    BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal, Transaction* transaction,
    RWPropertyStats propertyStatistics, bool requireNullColumn)
    : NodeColumn{LogicalType(LogicalTypeID::BOOL), metaDAHeaderInfo, dataFH, metadataFH,
          bufferManager, wal, transaction, propertyStatistics, requireNullColumn} {
    readNodeColumnFunc = BoolNodeColumnFunc::readValuesFromPage;
    writeNodeColumnFunc = BoolNodeColumnFunc::writeValueToPage;
    // 8 values per byte (on-disk)
    numValuesPerPage = PageUtils::getNumElementsInAPage(1, false /*requireNullColumn*/) * 8;
}

NullNodeColumn::NullNodeColumn(page_idx_t metaDAHPageIdx, BMFileHandle* dataFH,
    BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal, Transaction* transaction,
    RWPropertyStats propertyStatistics)
    : NodeColumn{LogicalType(LogicalTypeID::BOOL), MetadataDAHInfo{metaDAHPageIdx}, dataFH,
          metadataFH, bufferManager, wal, transaction, propertyStatistics,
          false /*requireNullColumn*/} {
    readNodeColumnFunc = NullNodeColumnFunc::readValuesFromPage;
    writeNodeColumnFunc = NullNodeColumnFunc::writeValueToPage;

    // 8 values per byte
    numValuesPerPage = PageUtils::getNumElementsInAPage(1, false /*requireNullColumn*/) * 8;
}

void NullNodeColumn::scan(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    if (propertyStatistics.mayHaveNull(*transaction)) {
        scanInternal(transaction, nodeIDVector, resultVector);
    } else {
        resultVector->setAllNonNull();
    }
}

void NullNodeColumn::scan(transaction::Transaction* transaction, node_group_idx_t nodeGroupIdx,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    if (propertyStatistics.mayHaveNull(*transaction)) {
        NodeColumn::scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup,
            resultVector, offsetInVector);
    } else {
        resultVector->setRangeNonNull(offsetInVector, endOffsetInGroup - startOffsetInGroup);
    }
}

void NullNodeColumn::scan(node_group_idx_t nodeGroupIdx, ColumnChunk* columnChunk) {
    if (propertyStatistics.mayHaveNull(DUMMY_WRITE_TRANSACTION)) {
        NodeColumn::scan(nodeGroupIdx, columnChunk);
    } else {
        static_cast<NullColumnChunk*>(columnChunk)->resetNullBuffer();
    }
}

void NullNodeColumn::lookup(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    if (propertyStatistics.mayHaveNull(*transaction)) {
        lookupInternal(transaction, nodeIDVector, resultVector);
    } else {
        for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            resultVector->setNull(pos, false);
        }
    }
}

page_idx_t NullNodeColumn::append(
    ColumnChunk* columnChunk, page_idx_t startPageIdx, uint64_t nodeGroupIdx) {
    auto numPagesFlushed = columnChunk->flushBuffer(dataFH, startPageIdx);
    metadataDA->resize(nodeGroupIdx + 1);
    metadataDA->update(nodeGroupIdx,
        ColumnChunkMetadata{startPageIdx, numPagesFlushed, columnChunk->getNumValues()});
    if (static_cast<NullColumnChunk*>(columnChunk)->mayHaveNull()) {
        propertyStatistics.setHasNull(DUMMY_WRITE_TRANSACTION);
    }
    return numPagesFlushed;
}

void NullNodeColumn::setNull(offset_t nodeOffset) {
    auto walPageInfo = createWALVersionOfPageForValue(nodeOffset);
    try {
        propertyStatistics.setHasNull(DUMMY_WRITE_TRANSACTION);
        NullMask::setNull((uint64_t*)walPageInfo.frame, walPageInfo.posInPage, true);
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
    if (vectorToWriteFrom->isNull(posInVectorToWriteFrom)) {
        propertyStatistics.setHasNull(DUMMY_WRITE_TRANSACTION);
    }
}

SerialNodeColumn::SerialNodeColumn(const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH,
    BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal, Transaction* transaction)
    : NodeColumn{LogicalType(LogicalTypeID::SERIAL), metaDAHeaderInfo, dataFH, metadataFH,
          // Serials can't be null, so they don't need PropertyStatistics
          bufferManager, wal, transaction, RWPropertyStats::empty(), false} {}

void SerialNodeColumn::scan(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    // Serial column cannot contain null values.
    for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        auto offset = nodeIDVector->readNodeOffset(pos);
        assert(!resultVector->isNull(pos));
        resultVector->setValue<offset_t>(pos, offset);
    }
}

void SerialNodeColumn::lookup(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    // Serial column cannot contain null values.
    for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        auto offset = nodeIDVector->readNodeOffset(pos);
        resultVector->setValue<offset_t>(pos, offset);
    }
}

page_idx_t SerialNodeColumn::append(
    ColumnChunk* columnChunk, page_idx_t startPageIdx, uint64_t nodeGroupIdx) {
    metadataDA->resize(nodeGroupIdx + 1);
    return 0;
}

std::unique_ptr<NodeColumn> NodeColumnFactory::createNodeColumn(const LogicalType& dataType,
    const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, Transaction* transaction, RWPropertyStats stats) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        return std::make_unique<BoolNodeColumn>(metaDAHeaderInfo, dataFH, metadataFH, bufferManager,
            wal, transaction, stats, true /* requireNullColumn */);
    }
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT16:
    case LogicalTypeID::INT8:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DATE:
    case LogicalTypeID::TIMESTAMP:
    case LogicalTypeID::INTERVAL:
    case LogicalTypeID::INTERNAL_ID:
    case LogicalTypeID::FIXED_LIST: {
        return std::make_unique<NodeColumn>(dataType, metaDAHeaderInfo, dataFH, metadataFH,
            bufferManager, wal, transaction, stats, true /* requireNullColumn */);
    }
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING: {
        return std::make_unique<StringNodeColumn>(
            dataType, metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, transaction, stats);
    }
    case LogicalTypeID::MAP:
    case LogicalTypeID::VAR_LIST: {
        return std::make_unique<VarListNodeColumn>(
            dataType, metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, transaction, stats);
    }
    case LogicalTypeID::UNION:
    case LogicalTypeID::STRUCT: {
        return std::make_unique<StructNodeColumn>(
            dataType, metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, transaction, stats);
    }
    case LogicalTypeID::SERIAL: {
        return std::make_unique<SerialNodeColumn>(
            metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, transaction);
    }
    default: {
        throw NotImplementedException("NodeColumnFactory::createNodeColumn");
    }
    }
}

} // namespace storage
} // namespace kuzu

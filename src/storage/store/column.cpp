#include "storage/store/column.h"

#include <memory>

#include "common/assert.h"
#include "common/exception/not_implemented.h"
#include "storage/stats/property_statistics.h"
#include "storage/store/column_chunk.h"
#include "storage/store/string_column.h"
#include "storage/store/struct_column.h"
#include "storage/store/var_list_column.h"
#include "transaction/transaction.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

struct InternalIDColumnFunc {
    static void readValuesFromPageToVector(uint8_t* frame, PageElementCursor& pageCursor,
        ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead,
        const CompressionMetadata& /*metadata*/) {
        KU_ASSERT(resultVector->dataType.getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
        auto resultData = (internalID_t*)resultVector->getData();
        for (auto i = 0u; i < numValuesToRead; i++) {
            auto posInFrame = pageCursor.elemPosInPage + i;
            resultData[posInVector + i].offset =
                *(offset_t*)(frame + (posInFrame * sizeof(offset_t)));
        }
    }

    static void writeValueToPage(uint8_t* frame, uint16_t posInFrame, ValueVector* vector,
        uint32_t posInVector, const CompressionMetadata& /*metadata*/) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
        auto internalID = vector->getValue<internalID_t>(posInVector);
        memcpy(frame + posInFrame * sizeof(offset_t), &internalID.offset, sizeof(offset_t));
    }
};

struct NullColumnFunc {
    static void readValuesFromPageToVector(uint8_t* frame, PageElementCursor& pageCursor,
        ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead,
        const CompressionMetadata& /*metadata*/) {
        // Read bit-packed null flags from the frame into the result vector
        // Casting to uint64_t should be safe as long as the page size is a multiple of 8 bytes.
        // Otherwise, it could read off the end of the page.
        resultVector->setNullFromBits(
            (uint64_t*)frame, pageCursor.elemPosInPage, posInVector, numValuesToRead);
    }

    static void writeValueToPage(uint8_t* frame, uint16_t posInFrame, ValueVector* vector,
        uint32_t posInVector, const CompressionMetadata& /*metadata*/) {
        // Casting to uint64_t should be safe as long as the page size is a multiple of 8 bytes.
        // Otherwise, it could read off the end of the page.
        NullMask::setNull(
            (uint64_t*)frame, posInFrame, NullMask::isNull(vector->getNullMaskData(), posInVector));
    }
};

struct BoolColumnFunc {
    static void readValuesFromPageToVector(uint8_t* frame, PageElementCursor& pageCursor,
        ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead,
        const CompressionMetadata& /*metadata*/) {
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

    static void writeValueToPage(uint8_t* frame, uint16_t posInFrame, ValueVector* vector,
        uint32_t posInVector, const CompressionMetadata& /*metadata*/) {
        // Casting to uint64_t should be safe as long as the page size is a multiple of 8 bytes.
        // Otherwise, it could read/write off the end of the page.
        NullMask::copyNullMask(vector->getValue<bool>(posInVector) ? &NullMask::ALL_NULL_ENTRY :
                                                                     &NullMask::NO_NULL_ENTRY,
            posInVector, (uint64_t*)frame, posInFrame, 1);
    }

    static void copyValuesFromPage(uint8_t* frame, PageElementCursor& pageCursor, uint8_t* result,
        uint32_t startPosInResult, uint64_t numValuesToRead,
        const CompressionMetadata& /*metadata*/) {
        NullMask::copyNullMask((uint64_t*)frame, pageCursor.elemPosInPage, (uint64_t*)result,
            startPosInResult, numValuesToRead);
    }

    static void batchLookupFromPage(uint8_t* frame, PageElementCursor& pageCursor, uint8_t* result,
        uint32_t startPosInResult, uint64_t numValuesToRead,
        const CompressionMetadata& /*metadata*/) {
        for (auto i = 0; i < numValuesToRead; i++) {
            result[startPosInResult + i] =
                NullMask::isNull((uint64_t*)frame, pageCursor.elemPosInPage + i);
        }
    }
};

static read_values_to_vector_func_t getReadValuesToVectorFunc(const LogicalType& logicalType) {
    switch (logicalType.getLogicalTypeID()) {
    case LogicalTypeID::INTERNAL_ID:
        return InternalIDColumnFunc::readValuesFromPageToVector;
    case LogicalTypeID::BOOL:
        return BoolColumnFunc::readValuesFromPageToVector;
    default:
        return ReadCompressedValuesFromPageToVector(logicalType);
    }
}

static read_values_to_page_func_t getWriteValuesToPageFunc(const LogicalType& logicalType) {
    switch (logicalType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL:
        return BoolColumnFunc::copyValuesFromPage;
    default:
        return ReadCompressedValuesFromPage(logicalType);
    }
}

static write_values_from_vector_func_t getWriteValuesFromVectorFunc(
    const LogicalType& logicalType) {
    switch (logicalType.getLogicalTypeID()) {
    case LogicalTypeID::INTERNAL_ID:
        return InternalIDColumnFunc::writeValueToPage;
    case LogicalTypeID::BOOL:
        return BoolColumnFunc::writeValueToPage;
    default:
        return WriteCompressedValueToPage(logicalType);
    }
}

static batch_lookup_func_t getBatchLookupFromPageFunc(const LogicalType& logicalType) {
    switch (logicalType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL:
        return BoolColumnFunc::batchLookupFromPage;
    default: {
        return ReadCompressedValuesFromPage(logicalType);
    }
    }
}

class NullColumn : public Column {
    friend StructColumn;

public:
    // Page size must be aligned to 8 byte chunks for the 64-bit NullMask algorithms to work
    // without the possibility of memory errors from reading/writing off the end of a page.
    static_assert(PageUtils::getNumElementsInAPage(1, false /*requireNullColumn*/) % 8 == 0);

    NullColumn(page_idx_t metaDAHPageIdx, BMFileHandle* dataFH, BMFileHandle* metadataFH,
        BufferManager* bufferManager, WAL* wal, Transaction* transaction,
        RWPropertyStats propertyStatistics, bool enableCompression)
        : Column{LogicalType(LogicalTypeID::BOOL), MetadataDAHInfo{metaDAHPageIdx}, dataFH,
              metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression,
              false /*requireNullColumn*/} {
        readToVectorFunc = NullColumnFunc::readValuesFromPageToVector;
        writeFromVectorFunc = NullColumnFunc::writeValueToPage;
    }

    void scan(
        Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) final {
        if (propertyStatistics.mayHaveNull(*transaction)) {
            scanInternal(transaction, nodeIDVector, resultVector);
        } else {
            resultVector->setAllNonNull();
        }
    }

    void scan(transaction::Transaction* transaction, node_group_idx_t nodeGroupIdx,
        offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
        uint64_t offsetInVector) final {
        if (propertyStatistics.mayHaveNull(*transaction)) {
            Column::scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup,
                resultVector, offsetInVector);
        } else {
            resultVector->setRangeNonNull(offsetInVector, endOffsetInGroup - startOffsetInGroup);
        }
    }

    void scan(node_group_idx_t nodeGroupIdx, ColumnChunk* columnChunk) final {
        if (propertyStatistics.mayHaveNull(DUMMY_WRITE_TRANSACTION)) {
            Column::scan(nodeGroupIdx, columnChunk);
        } else {
            static_cast<NullColumnChunk*>(columnChunk)->resetToNoNull();
        }
    }

    void lookup(
        Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) final {
        if (propertyStatistics.mayHaveNull(*transaction)) {
            lookupInternal(transaction, nodeIDVector, resultVector);
        } else {
            for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
                auto pos = nodeIDVector->state->selVector->selectedPositions[i];
                resultVector->setNull(pos, false);
            }
        }
    }

    void append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) final {
        auto preScanMetadata = columnChunk->getMetadataToFlush();
        auto startPageIdx = dataFH->addNewPages(preScanMetadata.numPages);
        auto metadata = columnChunk->flushBuffer(dataFH, startPageIdx, preScanMetadata);
        metadataDA->resize(nodeGroupIdx + 1);
        metadataDA->update(nodeGroupIdx, metadata);
        if (static_cast<NullColumnChunk*>(columnChunk)->mayHaveNull()) {
            propertyStatistics.setHasNull(DUMMY_WRITE_TRANSACTION);
        }
    }

    void setNull(offset_t nodeOffset) final {
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

    void write(offset_t nodeOffset, ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom) final {
        writeValue(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
        if (vectorToWriteFrom->isNull(posInVectorToWriteFrom)) {
            propertyStatistics.setHasNull(DUMMY_WRITE_TRANSACTION);
        }
    }
};

class SerialColumn : public Column {
public:
    SerialColumn(const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH,
        BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal, Transaction* transaction)
        : Column{LogicalType(LogicalTypeID::SERIAL), metaDAHeaderInfo, dataFH, metadataFH,
              // Serials can't be null, so they don't need PropertyStatistics
              bufferManager, wal, transaction, RWPropertyStats::empty(),
              false /* enableCompression */, false /*requireNullColumn*/} {}

    void scan(
        Transaction* /*transaction*/, ValueVector* nodeIDVector, ValueVector* resultVector) final {
        // Serial column cannot contain null values.
        for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            auto offset = nodeIDVector->readNodeOffset(pos);
            KU_ASSERT(!resultVector->isNull(pos));
            resultVector->setValue<offset_t>(pos, offset);
        }
    }

    void lookup(
        Transaction* /*transaction*/, ValueVector* nodeIDVector, ValueVector* resultVector) final {
        // Serial column cannot contain null values.
        for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            auto offset = nodeIDVector->readNodeOffset(pos);
            resultVector->setValue<offset_t>(pos, offset);
        }
    }

    void append(ColumnChunk* /*columnChunk*/, uint64_t nodeGroupIdx) final {
        metadataDA->resize(nodeGroupIdx + 1);
    }
};

InternalIDColumn::InternalIDColumn(const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH,
    BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
    transaction::Transaction* transaction, RWPropertyStats stats)
    : Column{LogicalType{LogicalTypeID::INTERNAL_ID}, metaDAHeaderInfo, dataFH, metadataFH,
          bufferManager, wal, transaction, stats, false /* enableCompression */},
      commonTableID{INVALID_TABLE_ID} {}

void InternalIDColumn::populateCommonTableID(ValueVector* resultVector) const {
    auto nodeIDs = ((internalID_t*)resultVector->getData());
    for (auto i = 0u; i < resultVector->state->selVector->selectedSize; i++) {
        auto pos = resultVector->state->selVector->selectedPositions[i];
        nodeIDs[pos].tableID = commonTableID;
    }
}

Column::Column(LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH,
    BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
    transaction::Transaction* transaction, RWPropertyStats propertyStatistics,
    bool enableCompression, bool requireNullColumn)
    : dbFileID{DBFileID::newDataFileID()}, dataType{std::move(dataType)}, dataFH{dataFH},
      metadataFH{metadataFH}, bufferManager{bufferManager},
      propertyStatistics{propertyStatistics}, wal{wal}, enableCompression{enableCompression} {
    metadataDA = std::make_unique<InMemDiskArray<ColumnChunkMetadata>>(*metadataFH,
        DBFileID::newMetadataFileID(), metaDAHeaderInfo.dataDAHPageIdx, bufferManager, wal,
        transaction);
    numBytesPerFixedSizedValue = getDataTypeSizeInChunk(this->dataType);
    readToVectorFunc = getReadValuesToVectorFunc(this->dataType);
    readToPageFunc = getWriteValuesToPageFunc(this->dataType);
    batchLookupFunc = getBatchLookupFromPageFunc(this->dataType);
    writeFromVectorFunc = getWriteValuesFromVectorFunc(this->dataType);
    KU_ASSERT(numBytesPerFixedSizedValue <= BufferPoolConstants::PAGE_4KB_SIZE);
    if (requireNullColumn) {
        nullColumn = std::make_unique<NullColumn>(metaDAHeaderInfo.nullDAHPageIdx, dataFH,
            metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression);
    }
}

void Column::batchLookup(
    Transaction* transaction, const offset_t* nodeOffsets, size_t size, uint8_t* result) {
    for (auto i = 0u; i < size; ++i) {
        auto nodeOffset = nodeOffsets[i];
        auto cursor = getPageCursorForOffset(transaction->getType(), nodeOffset);
        auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
        auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
        KU_ASSERT(cursor.pageIdx < chunkMeta.pageIdx + chunkMeta.numPages);
        readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
            batchLookupFunc(frame, cursor, result, i, 1, chunkMeta.compMeta);
        });
    }
}

void Column::scan(Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    if (nullColumn) {
        nullColumn->scan(transaction, nodeIDVector, resultVector);
    }
    scanInternal(transaction, nodeIDVector, resultVector);
}

void Column::scan(transaction::Transaction* transaction, node_group_idx_t nodeGroupIdx,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    if (nullColumn) {
        nullColumn->scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup,
            resultVector, offsetInVector);
    }
    auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
    auto pageCursor = PageUtils::getPageElementCursorForPos(startOffsetInGroup,
        chunkMeta.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType));
    pageCursor.pageIdx += chunkMeta.pageIdx;
    auto numValuesToScan = endOffsetInGroup - startOffsetInGroup;
    scanUnfiltered(
        transaction, pageCursor, numValuesToScan, resultVector, chunkMeta, offsetInVector);
}

void Column::scan(node_group_idx_t nodeGroupIdx, ColumnChunk* columnChunk) {
    if (nullColumn) {
        nullColumn->scan(nodeGroupIdx, columnChunk->getNullChunk());
    }
    if (nodeGroupIdx >= metadataDA->getNumElements()) {
        columnChunk->setNumValues(0);
    } else {
        auto chunkMetadata = metadataDA->get(nodeGroupIdx, TransactionType::WRITE);
        auto cursor = PageElementCursor(chunkMetadata.pageIdx, 0);
        uint64_t numValuesPerPage =
            chunkMetadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
        uint64_t numValuesScanned = 0u;
        while (numValuesScanned < columnChunk->getCapacity()) {
            auto numValuesToReadInPage =
                std::min(numValuesPerPage, columnChunk->getCapacity() - numValuesScanned);
            KU_ASSERT(cursor.pageIdx < chunkMetadata.pageIdx + chunkMetadata.numPages);
            readFromPage(&DUMMY_READ_TRANSACTION, cursor.pageIdx, [&](uint8_t* frame) -> void {
                readToPageFunc(frame, cursor, columnChunk->getData(), numValuesScanned,
                    numValuesToReadInPage, chunkMetadata.compMeta);
            });
            numValuesScanned += numValuesToReadInPage;
            cursor.nextPage();
        }
        columnChunk->setNumValues(chunkMetadata.numValues);
    }
}

void Column::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    KU_ASSERT(startNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
    auto cursor = getPageCursorForOffset(transaction->getType(), startNodeOffset);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(startNodeOffset);
    auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
    if (nodeIDVector->state->selVector->isUnfiltered()) {
        scanUnfiltered(transaction, cursor, nodeIDVector->state->selVector->selectedSize,
            resultVector, chunkMeta);
    } else {
        scanFiltered(transaction, cursor, nodeIDVector, resultVector, chunkMeta);
    }
}

void Column::scanUnfiltered(Transaction* transaction, PageElementCursor& pageCursor,
    uint64_t numValuesToScan, ValueVector* resultVector, const ColumnChunkMetadata& chunkMeta,
    uint64_t startPosInVector) {
    uint64_t numValuesScanned = 0;
    auto numValuesPerPage =
        chunkMeta.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
    while (numValuesScanned < numValuesToScan) {
        uint64_t numValuesToScanInPage =
            std::min((uint64_t)numValuesPerPage - pageCursor.elemPosInPage,
                numValuesToScan - numValuesScanned);
        KU_ASSERT(pageCursor.pageIdx < chunkMeta.pageIdx + chunkMeta.numPages);
        readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
            readToVectorFunc(frame, pageCursor, resultVector, numValuesScanned + startPosInVector,
                numValuesToScanInPage, chunkMeta.compMeta);
        });
        numValuesScanned += numValuesToScanInPage;
        pageCursor.nextPage();
    }
}

void Column::scanFiltered(Transaction* transaction, PageElementCursor& pageCursor,
    ValueVector* nodeIDVector, ValueVector* resultVector, const ColumnChunkMetadata& chunkMeta) {
    auto numValuesToScan = nodeIDVector->state->getOriginalSize();
    auto numValuesScanned = 0u;
    auto posInSelVector = 0u;
    auto numValuesPerPage =
        chunkMeta.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
    while (numValuesScanned < numValuesToScan) {
        uint64_t numValuesToScanInPage =
            std::min((uint64_t)numValuesPerPage - pageCursor.elemPosInPage,
                numValuesToScan - numValuesScanned);
        if (isInRange(nodeIDVector->state->selVector->selectedPositions[posInSelVector],
                numValuesScanned, numValuesScanned + numValuesToScanInPage)) {
            KU_ASSERT(pageCursor.pageIdx < chunkMeta.pageIdx + chunkMeta.numPages);
            readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
                readToVectorFunc(frame, pageCursor, resultVector, numValuesScanned,
                    numValuesToScanInPage, chunkMeta.compMeta);
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

void Column::lookup(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    if (nullColumn) {
        nullColumn->lookup(transaction, nodeIDVector, resultVector);
    }
    lookupInternal(transaction, nodeIDVector, resultVector);
}

void Column::lookupInternal(
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

void Column::lookupValue(transaction::Transaction* transaction, offset_t nodeOffset,
    ValueVector* resultVector, uint32_t posInVector) {
    auto cursor = getPageCursorForOffset(transaction->getType(), nodeOffset);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
    KU_ASSERT(cursor.pageIdx < chunkMeta.pageIdx + chunkMeta.numPages);
    readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
        readToVectorFunc(
            frame, cursor, resultVector, posInVector, 1 /* numValuesToRead */, chunkMeta.compMeta);
    });
}

void Column::readFromPage(
    Transaction* transaction, page_idx_t pageIdx, const std::function<void(uint8_t*)>& func) {
    auto [fileHandleToPin, pageIdxToPin] = DBFileUtils::getFileHandleAndPhysicalPageIdxToPin(
        *dataFH, pageIdx, *wal, transaction->getType());
    bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, func);
}

void Column::append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) {
    // Main column chunk.
    auto preScanMetadata = columnChunk->getMetadataToFlush();
    auto startPageIdx = dataFH->addNewPages(preScanMetadata.numPages);
    auto metadata = columnChunk->flushBuffer(dataFH, startPageIdx, preScanMetadata);
    metadataDA->resize(nodeGroupIdx + 1);
    metadataDA->update(nodeGroupIdx, metadata);
    if (nullColumn) {
        // Null column chunk.
        nullColumn->append(columnChunk->getNullChunk(), nodeGroupIdx);
    }
}

void Column::write(
    offset_t nodeOffset, ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    if (nullColumn) {
        nullColumn->write(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
    }
    bool isNull = vectorToWriteFrom->isNull(posInVectorToWriteFrom);
    if (isNull) {
        return;
    }
    writeValue(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
}

void Column::writeValue(
    offset_t nodeOffset, ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto walPageInfo = createWALVersionOfPageForValue(nodeOffset);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto chunkMeta = metadataDA->get(nodeGroupIdx, TransactionType::WRITE);
    KU_ASSERT(
        chunkMeta.pageIdx <= walPageInfo.originalPageIdx < chunkMeta.pageIdx + chunkMeta.numPages);
    try {
        writeFromVectorFunc(walPageInfo.frame, walPageInfo.posInPage, vectorToWriteFrom,
            posInVectorToWriteFrom, chunkMeta.compMeta);
    } catch (Exception& e) {
        bufferManager->unpin(*wal->fileHandle, walPageInfo.pageIdxInWAL);
        dataFH->releaseWALPageIdxLock(walPageInfo.originalPageIdx);
        throw;
    }
    bufferManager->unpin(*wal->fileHandle, walPageInfo.pageIdxInWAL);
    dataFH->releaseWALPageIdxLock(walPageInfo.originalPageIdx);
}

WALPageIdxPosInPageAndFrame Column::createWALVersionOfPageForValue(offset_t nodeOffset) {
    auto originalPageCursor = getPageCursorForOffset(TransactionType::WRITE, nodeOffset);
    bool insertingNewPage = false;
    if (originalPageCursor.pageIdx >= dataFH->getNumPages()) {
        KU_ASSERT(originalPageCursor.pageIdx == dataFH->getNumPages());
        DBFileUtils::insertNewPage(*dataFH, dbFileID, *bufferManager, *wal);
        insertingNewPage = true;
    }
    auto walPageIdxAndFrame = DBFileUtils::createWALVersionIfNecessaryAndPinPage(
        originalPageCursor.pageIdx, insertingNewPage, *dataFH, dbFileID, *bufferManager, *wal);
    return {walPageIdxAndFrame, originalPageCursor.elemPosInPage};
}

void Column::setNull(offset_t nodeOffset) {
    if (nullColumn) {
        nullColumn->setNull(nodeOffset);
    }
}

void Column::checkpointInMemory() {
    metadataDA->checkpointInMemoryIfNecessary();
    if (nullColumn) {
        nullColumn->checkpointInMemory();
    }
}

void Column::rollbackInMemory() {
    metadataDA->rollbackInMemoryIfNecessary();
    if (nullColumn) {
        nullColumn->rollbackInMemory();
    }
}

void Column::populateWithDefaultVal(const Property& property, Column* column,
    InMemDiskArray<ColumnChunkMetadata>* metadataDA_, ValueVector* defaultValueVector,
    uint64_t numNodeGroups) const {
    auto columnChunk =
        ColumnChunkFactory::createColumnChunk(*property.getDataType(), enableCompression);
    columnChunk->populateWithDefaultVal(defaultValueVector);
    for (auto i = 0u; i < numNodeGroups; i++) {
        auto chunkMeta = metadataDA_->get(i, TransactionType::WRITE);
        auto capacity = columnChunk->getCapacity();
        while (capacity < chunkMeta.numValues) {
            capacity *= VAR_LIST_RESIZE_RATIO;
        }
        if (capacity > columnChunk->getCapacity()) {
            auto newColumnChunk =
                ColumnChunkFactory::createColumnChunk(*property.getDataType(), enableCompression);
            newColumnChunk->populateWithDefaultVal(defaultValueVector);
            newColumnChunk->setNumValues(chunkMeta.numValues);
            column->append(newColumnChunk.get(), i);
        } else {
            columnChunk->setNumValues(chunkMeta.numValues);
            column->append(columnChunk.get(), i);
        }
    }
}

PageElementCursor Column::getPageCursorForOffset(
    TransactionType transactionType, offset_t nodeOffset) {
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto offsetInNodeGroup = nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    auto chunkMeta = metadataDA->get(nodeGroupIdx, transactionType);
    auto pageCursor = PageUtils::getPageElementCursorForPos(offsetInNodeGroup,
        chunkMeta.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType));
    pageCursor.pageIdx += chunkMeta.pageIdx;
    return pageCursor;
}

std::unique_ptr<Column> ColumnFactory::createColumn(const common::LogicalType& dataType,
    const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, transaction::Transaction* transaction,
    RWPropertyStats propertyStatistics, bool enableCompression) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL:
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT16:
    case LogicalTypeID::INT8:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::UINT32:
    case LogicalTypeID::UINT16:
    case LogicalTypeID::UINT8:
    case LogicalTypeID::INT128:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DATE:
    case LogicalTypeID::TIMESTAMP:
    case LogicalTypeID::INTERVAL:
    case LogicalTypeID::FIXED_LIST: {
        return std::make_unique<Column>(dataType, metaDAHeaderInfo, dataFH, metadataFH,
            bufferManager, wal, transaction, propertyStatistics, enableCompression);
    }
    case LogicalTypeID::INTERNAL_ID: {
        return std::make_unique<InternalIDColumn>(metaDAHeaderInfo, dataFH, metadataFH,
            bufferManager, wal, transaction, propertyStatistics);
    }
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING: {
        return std::make_unique<StringColumn>(dataType, metaDAHeaderInfo, dataFH, metadataFH,
            bufferManager, wal, transaction, propertyStatistics);
    }
    case LogicalTypeID::MAP:
    case LogicalTypeID::VAR_LIST: {
        return std::make_unique<VarListColumn>(dataType, metaDAHeaderInfo, dataFH, metadataFH,
            bufferManager, wal, transaction, propertyStatistics, enableCompression);
    }
    case LogicalTypeID::UNION:
    case LogicalTypeID::STRUCT:
    case LogicalTypeID::RDF_VARIANT: {
        return std::make_unique<StructColumn>(dataType, metaDAHeaderInfo, dataFH, metadataFH,
            bufferManager, wal, transaction, propertyStatistics, enableCompression);
    }
    case LogicalTypeID::SERIAL: {
        return std::make_unique<SerialColumn>(
            metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, transaction);
    }
        // LCOV_EXCL_START
    default: {
        throw NotImplementedException("ColumnFactory::createColumn");
    }
        // LCOV_EXCL_STOP
    }
}

} // namespace storage
} // namespace kuzu

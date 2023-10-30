#include "storage/store/node_column.h"

#include <memory>

#include "storage/stats/property_statistics.h"
#include "storage/storage_structure/storage_structure.h"
#include "storage/store/column_chunk.h"
#include "storage/store/compression.h"
#include "storage/store/string_node_column.h"
#include "storage/store/struct_node_column.h"
#include "storage/store/var_list_node_column.h"
#include "transaction/transaction.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

struct InternalIDNodeColumnFunc {
    static void readValuesFromPageToVector(uint8_t* frame, PageElementCursor& pageCursor,
        ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead,
        const CompressionMetadata& /*metadata*/) {
        auto resultData = (internalID_t*)resultVector->getData();
        for (auto i = 0u; i < numValuesToRead; i++) {
            auto posInFrame = pageCursor.elemPosInPage + i;
            resultData[posInVector + i].offset =
                *(offset_t*)(frame + (posInFrame * sizeof(offset_t)));
        }
    }

    static void writeValueToPage(uint8_t* frame, uint16_t posInFrame, ValueVector* vector,
        uint32_t posInVector, const CompressionMetadata& /*metadata*/) {
        auto relID = vector->getValue<relID_t>(posInVector);
        memcpy(frame + posInFrame * sizeof(offset_t), &relID.offset, sizeof(offset_t));
    }
};

struct NullNodeColumnFunc {
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

struct BoolNodeColumnFunc {
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
        return InternalIDNodeColumnFunc::readValuesFromPageToVector;
    case LogicalTypeID::BOOL:
        return BoolNodeColumnFunc::readValuesFromPageToVector;
    default:
        return ReadCompressedValuesFromPageToVector(logicalType);
    }
}

static read_values_to_page_func_t getWriteValuesToPageFunc(const LogicalType& logicalType) {
    switch (logicalType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL:
        return BoolNodeColumnFunc::copyValuesFromPage;
    default:
        return ReadCompressedValuesFromPage(logicalType);
    }
}

static write_values_from_vector_func_t getWriteValuesFromVectorFunc(
    const LogicalType& logicalType) {
    switch (logicalType.getLogicalTypeID()) {
    case LogicalTypeID::INTERNAL_ID:
        return InternalIDNodeColumnFunc::writeValueToPage;
    case LogicalTypeID::BOOL:
        return BoolNodeColumnFunc::writeValueToPage;
    default:
        return WriteCompressedValueToPage(logicalType);
    }
}

static batch_lookup_func_t getBatchLookupFromPageFunc(const LogicalType& logicalType) {
    switch (logicalType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL:
        return BoolNodeColumnFunc::batchLookupFromPage;
    default: {
        return ReadCompressedValuesFromPage(logicalType);
    }
    }
}

class NullNodeColumn : public NodeColumn {
    friend StructNodeColumn;

public:
    // Page size must be aligned to 8 byte chunks for the 64-bit NullMask algorithms to work
    // without the possibility of memory errors from reading/writing off the end of a page.
    static_assert(PageUtils::getNumElementsInAPage(1, false /*requireNullColumn*/) % 8 == 0);

    NullNodeColumn(page_idx_t metaDAHPageIdx, BMFileHandle* dataFH, BMFileHandle* metadataFH,
        BufferManager* bufferManager, WAL* wal, Transaction* transaction,
        RWPropertyStats propertyStatistics, bool enableCompression)
        : NodeColumn{LogicalType(LogicalTypeID::BOOL), MetadataDAHInfo{metaDAHPageIdx}, dataFH,
              metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression,
              false /*requireNullColumn*/} {
        readToVectorFunc = NullNodeColumnFunc::readValuesFromPageToVector;
        writeFromVectorFunc = NullNodeColumnFunc::writeValueToPage;
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
            NodeColumn::scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup,
                resultVector, offsetInVector);
        } else {
            resultVector->setRangeNonNull(offsetInVector, endOffsetInGroup - startOffsetInGroup);
        }
    }

    void scan(node_group_idx_t nodeGroupIdx, ColumnChunk* columnChunk) final {
        if (propertyStatistics.mayHaveNull(DUMMY_WRITE_TRANSACTION)) {
            NodeColumn::scan(nodeGroupIdx, columnChunk);
        } else {
            static_cast<NullColumnChunk*>(columnChunk)->resetNullBuffer();
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

class SerialNodeColumn : public NodeColumn {
public:
    SerialNodeColumn(const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH,
        BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal, Transaction* transaction)
        : NodeColumn{LogicalType(LogicalTypeID::SERIAL), metaDAHeaderInfo, dataFH, metadataFH,
              // Serials can't be null, so they don't need PropertyStatistics
              bufferManager, wal, transaction, RWPropertyStats::empty(),
              false /* enableCompression */, false /*requireNullColumn*/} {}

    void scan(
        Transaction* /*transaction*/, ValueVector* nodeIDVector, ValueVector* resultVector) final {
        // Serial column cannot contain null values.
        for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            auto offset = nodeIDVector->readNodeOffset(pos);
            assert(!resultVector->isNull(pos));
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

NodeColumn::NodeColumn(LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
    BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
    transaction::Transaction* transaction, RWPropertyStats propertyStatistics,
    bool enableCompression, bool requireNullColumn)
    : storageStructureID{StorageStructureID::newDataID()}, dataType{std::move(dataType)},
      dataFH{dataFH}, metadataFH{metadataFH}, bufferManager{bufferManager},
      propertyStatistics{propertyStatistics}, wal{wal}, enableCompression{enableCompression} {
    metadataDA = std::make_unique<InMemDiskArray<ColumnChunkMetadata>>(*metadataFH,
        StorageStructureID::newMetadataID(), metaDAHeaderInfo.dataDAHPageIdx, bufferManager, wal,
        transaction);
    numBytesPerFixedSizedValue = getDataTypeSizeInChunk(this->dataType);
    readToVectorFunc = getReadValuesToVectorFunc(this->dataType);
    readToPageFunc = getWriteValuesToPageFunc(this->dataType);
    batchLookupFunc = getBatchLookupFromPageFunc(this->dataType);
    writeFromVectorFunc = getWriteValuesFromVectorFunc(this->dataType);
    assert(numBytesPerFixedSizedValue <= BufferPoolConstants::PAGE_4KB_SIZE);
    if (requireNullColumn) {
        nullColumn = std::make_unique<NullNodeColumn>(metaDAHeaderInfo.nullDAHPageIdx, dataFH,
            metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression);
    }
}

void NodeColumn::batchLookup(
    Transaction* transaction, const offset_t* nodeOffsets, size_t size, uint8_t* result) {
    for (auto i = 0u; i < size; ++i) {
        auto nodeOffset = nodeOffsets[i];
        auto cursor = getPageCursorForOffset(transaction->getType(), nodeOffset);
        auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
        auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
        readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
            batchLookupFunc(frame, cursor, result, i, 1, chunkMeta.compMeta);
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
    auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
    auto pageCursor = PageUtils::getPageElementCursorForPos(startOffsetInGroup,
        chunkMeta.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType));
    pageCursor.pageIdx += chunkMeta.pageIdx;
    auto numValuesToScan = endOffsetInGroup - startOffsetInGroup;
    scanUnfiltered(
        transaction, pageCursor, numValuesToScan, resultVector, chunkMeta.compMeta, offsetInVector);
}

void NodeColumn::scan(node_group_idx_t nodeGroupIdx, ColumnChunk* columnChunk) {
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

void NodeColumn::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    assert(startNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
    auto cursor = getPageCursorForOffset(transaction->getType(), startNodeOffset);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(startNodeOffset);
    auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
    if (nodeIDVector->state->selVector->isUnfiltered()) {
        scanUnfiltered(transaction, cursor, nodeIDVector->state->selVector->selectedSize,
            resultVector, chunkMeta.compMeta);
    } else {
        scanFiltered(transaction, cursor, nodeIDVector, resultVector, chunkMeta.compMeta);
    }
}

void NodeColumn::scanUnfiltered(Transaction* transaction, PageElementCursor& pageCursor,
    uint64_t numValuesToScan, ValueVector* resultVector, const CompressionMetadata& compMeta,
    uint64_t startPosInVector) {
    uint64_t numValuesScanned = 0;
    auto numValuesPerPage = compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
    while (numValuesScanned < numValuesToScan) {
        uint64_t numValuesToScanInPage =
            std::min((uint64_t)numValuesPerPage - pageCursor.elemPosInPage,
                numValuesToScan - numValuesScanned);
        readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
            readToVectorFunc(frame, pageCursor, resultVector, numValuesScanned + startPosInVector,
                numValuesToScanInPage, compMeta);
        });
        numValuesScanned += numValuesToScanInPage;
        pageCursor.nextPage();
    }
}

void NodeColumn::scanFiltered(Transaction* transaction, PageElementCursor& pageCursor,
    ValueVector* nodeIDVector, ValueVector* resultVector, const CompressionMetadata& compMeta) {
    auto numValuesToScan = nodeIDVector->state->getOriginalSize();
    auto numValuesScanned = 0u;
    auto posInSelVector = 0u;
    auto numValuesPerPage = compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
    while (numValuesScanned < numValuesToScan) {
        uint64_t numValuesToScanInPage =
            std::min((uint64_t)numValuesPerPage - pageCursor.elemPosInPage,
                numValuesToScan - numValuesScanned);
        if (StorageStructure::isInRange(
                nodeIDVector->state->selVector->selectedPositions[posInSelVector], numValuesScanned,
                numValuesScanned + numValuesToScanInPage)) {
            readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
                readToVectorFunc(frame, pageCursor, resultVector, numValuesScanned,
                    numValuesToScanInPage, compMeta);
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
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
    readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
        readToVectorFunc(
            frame, cursor, resultVector, posInVector, 1 /* numValuesToRead */, chunkMeta.compMeta);
    });
}

void NodeColumn::readFromPage(
    Transaction* transaction, page_idx_t pageIdx, const std::function<void(uint8_t*)>& func) {
    auto [fileHandleToPin, pageIdxToPin] =
        StorageStructureUtils::getFileHandleAndPhysicalPageIdxToPin(
            *dataFH, pageIdx, *wal, transaction->getType());
    bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, func);
}

void NodeColumn::append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) {
    // Main column chunk.
    auto preScanMetadata = columnChunk->getMetadataToFlush();
    auto startPageIdx = dataFH->addNewPages(preScanMetadata.numPages);
    auto metadata = columnChunk->flushBuffer(dataFH, startPageIdx, preScanMetadata);
    metadataDA->resize(nodeGroupIdx + 1);
    metadataDA->update(nodeGroupIdx, metadata);
    // Null column chunk.
    nullColumn->append(columnChunk->getNullChunk(), nodeGroupIdx);
}

void NodeColumn::write(
    offset_t nodeOffset, ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    nullColumn->write(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
    bool isNull = vectorToWriteFrom->isNull(posInVectorToWriteFrom);
    if (isNull) {
        return;
    }
    writeValue(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
}

void NodeColumn::writeValue(
    offset_t nodeOffset, ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto walPageInfo = createWALVersionOfPageForValue(nodeOffset);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto chunkMeta = metadataDA->get(nodeGroupIdx, TransactionType::WRITE);
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
    if (nullColumn) {
        nullColumn->checkpointInMemory();
    }
}

void NodeColumn::rollbackInMemory() {
    metadataDA->rollbackInMemoryIfNecessary();
    if (nullColumn) {
        nullColumn->rollbackInMemory();
    }
}

void NodeColumn::populateWithDefaultVal(const Property& property, NodeColumn* nodeColumn,
    ValueVector* defaultValueVector, uint64_t numNodeGroups) const {
    auto columnChunk =
        ColumnChunkFactory::createColumnChunk(*property.getDataType(), enableCompression);
    columnChunk->populateWithDefaultVal(defaultValueVector);
    for (auto i = 0u; i < numNodeGroups; i++) {
        nodeColumn->append(columnChunk.get(), i);
    }
}

PageElementCursor NodeColumn::getPageCursorForOffset(
    TransactionType transactionType, offset_t nodeOffset) {
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    auto offsetInNodeGroup = nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    auto chunkMeta = metadataDA->get(nodeGroupIdx, transactionType);
    auto pageCursor = PageUtils::getPageElementCursorForPos(offsetInNodeGroup,
        chunkMeta.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType));
    pageCursor.pageIdx += chunkMeta.pageIdx;
    return pageCursor;
}

std::unique_ptr<NodeColumn> NodeColumnFactory::createNodeColumn(const LogicalType& dataType,
    const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, Transaction* transaction, RWPropertyStats stats,
    bool enableCompression) {
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
    case LogicalTypeID::INTERNAL_ID:
    case LogicalTypeID::FIXED_LIST: {
        return std::make_unique<NodeColumn>(dataType, metaDAHeaderInfo, dataFH, metadataFH,
            bufferManager, wal, transaction, stats, enableCompression);
    }
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING: {
        return std::make_unique<StringNodeColumn>(
            dataType, metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, transaction, stats);
    }
    case LogicalTypeID::MAP:
    case LogicalTypeID::VAR_LIST: {
        return std::make_unique<VarListNodeColumn>(dataType, metaDAHeaderInfo, dataFH, metadataFH,
            bufferManager, wal, transaction, stats, enableCompression);
    }
    case LogicalTypeID::UNION:
    case LogicalTypeID::STRUCT: {
        return std::make_unique<StructNodeColumn>(dataType, metaDAHeaderInfo, dataFH, metadataFH,
            bufferManager, wal, transaction, stats, enableCompression);
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

#include "storage/store/column.h"

#include <memory>

#include "common/assert.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "storage/stats/property_statistics.h"
#include "storage/storage_utils.h"
#include "storage/store/column_chunk.h"
#include "storage/store/list_column.h"
#include "storage/store/null_column.h"
#include "storage/store/string_column.h"
#include "storage/store/struct_column.h"
#include "transaction/transaction.h"
#include <bit>

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

static inline bool isPageIdxValid(page_idx_t pageIdx, const ColumnChunkMetadata& metadata) {
    return (metadata.pageIdx <= pageIdx && pageIdx < metadata.pageIdx + metadata.numPages) ||
           (pageIdx == INVALID_PAGE_IDX && metadata.compMeta.isConstant());
}

struct ReadInternalIDValuesToVector {
    ReadInternalIDValuesToVector() : compressedReader{LogicalType(LogicalTypeID::INTERNAL_ID)} {}
    void operator()(const uint8_t* frame, PageCursor& pageCursor, ValueVector* resultVector,
        uint32_t posInVector, uint32_t numValuesToRead, const CompressionMetadata& metadata) {
        KU_ASSERT(resultVector->dataType.getPhysicalType() == PhysicalTypeID::INTERNAL_ID);

        std::unique_ptr<offset_t[]> buffer = std::make_unique<offset_t[]>(numValuesToRead);
        compressedReader(frame, pageCursor, (uint8_t*)buffer.get(), 0, numValuesToRead, metadata);
        auto resultData = (internalID_t*)resultVector->getData();
        for (auto i = 0u; i < numValuesToRead; i++) {
            resultData[posInVector + i].offset = buffer[i];
        }
    }

private:
    ReadCompressedValuesFromPage compressedReader;
};

struct WriteInternalIDValuesToPage {
    WriteInternalIDValuesToPage() : compressedWriter{LogicalType(LogicalTypeID::INTERNAL_ID)} {}
    void operator()(uint8_t* frame, uint16_t posInFrame, const uint8_t* data, uint32_t dataOffset,
        offset_t numValues, const CompressionMetadata& metadata) {
        compressedWriter(frame, posInFrame, data, dataOffset, numValues, metadata);
    }
    void operator()(uint8_t* frame, uint16_t posInFrame, ValueVector* vector,
        uint32_t offsetInVector, const CompressionMetadata& metadata) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
        compressedWriter(frame, posInFrame,
            reinterpret_cast<const uint8_t*>(
                &vector->getValue<internalID_t>(offsetInVector).offset),
            0 /*dataOffset*/, 1 /*numValues*/, metadata);
    }

private:
    WriteCompressedValuesToPage compressedWriter;
};

static read_values_to_vector_func_t getReadValuesToVectorFunc(const LogicalType& logicalType) {
    switch (logicalType.getLogicalTypeID()) {
    case LogicalTypeID::INTERNAL_ID:
        return ReadInternalIDValuesToVector();
    default:
        return ReadCompressedValuesFromPageToVector(logicalType);
    }
}

static write_values_from_vector_func_t getWriteValueFromVectorFunc(const LogicalType& logicalType) {
    switch (logicalType.getLogicalTypeID()) {
    case LogicalTypeID::INTERNAL_ID:
        return WriteInternalIDValuesToPage();
    default:
        return WriteCompressedValuesToPage(logicalType);
    }
}

static write_values_func_t getWriteValuesFunc(const LogicalType& logicalType) {
    switch (logicalType.getLogicalTypeID()) {
    case LogicalTypeID::INTERNAL_ID:
        return WriteInternalIDValuesToPage();
    default:
        return WriteCompressedValuesToPage(logicalType);
    }
}

class SerialColumn final : public Column {
public:
    SerialColumn(std::string name, const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH,
        BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal, Transaction* transaction)
        : Column{name, *LogicalType::SERIAL(), metaDAHeaderInfo, dataFH, metadataFH,
              // Serials can't be null, so they don't need PropertyStatistics
              bufferManager, wal, transaction, RWPropertyStats::empty(),
              false /* enableCompression */, false /*requireNullColumn*/} {}

    void scan(Transaction* /*transaction*/, ValueVector* nodeIDVector,
        ValueVector* resultVector) override {
        // Serial column cannot contain null values.
        for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            auto offset = nodeIDVector->readNodeOffset(pos);
            resultVector->setNull(pos, false);
            resultVector->setValue<offset_t>(pos, offset);
        }
    }

    void lookup(Transaction* /*transaction*/, ValueVector* nodeIDVector,
        ValueVector* resultVector) override {
        // Serial column cannot contain null values.
        for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            auto offset = nodeIDVector->readNodeOffset(pos);
            resultVector->setNull(pos, false);
            resultVector->setValue<offset_t>(pos, offset);
        }
    }

    bool canCommitInPlace(Transaction*, node_group_idx_t, const ChunkCollection&,
        const offset_to_row_idx_t&, const ChunkCollection&,
        const offset_to_row_idx_t& updateInfo) override {
        (void)updateInfo; // Avoid unused parameter warnings during release build.
        KU_ASSERT(updateInfo.empty());
        return true;
    }

    bool canCommitInPlace(transaction::Transaction* /*transaction*/,
        common::node_group_idx_t /*nodeGroupIdx*/,
        const std::vector<common::offset_t>& /*dstOffset*/, ColumnChunk* /*chunk*/,
        common::offset_t /*startOffset*/) override {
        // Note: only updates to rel table can trigger this code path. SERIAL is not supported in
        // rel table yet.
        KU_UNREACHABLE;
    }

    void commitLocalChunkInPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
        const ChunkCollection&, const offset_to_row_idx_t& insertInfo, const ChunkCollection&,
        const offset_to_row_idx_t& updateInfo, const offset_set_t& deleteInfo) override {
        // Avoid unused parameter warnings during release build.
        (void)updateInfo;
        (void)deleteInfo;
        KU_ASSERT(updateInfo.empty() && deleteInfo.empty());
        auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
        auto numValues = chunkMeta.numValues;
        for (auto& [offsetInChunk, _] : insertInfo) {
            if (offsetInChunk >= numValues) {
                numValues = offsetInChunk + 1;
            }
        }
        if (numValues > chunkMeta.numValues) {
            chunkMeta.numValues = numValues;
            metadataDA->update(nodeGroupIdx, chunkMeta);
        }
    }

    void commitLocalChunkOutOfPlace(Transaction* /*transaction*/, node_group_idx_t nodeGroupIdx,
        bool isNewNodeGroup, const ChunkCollection&, const offset_to_row_idx_t& insertInfo,
        const ChunkCollection&, const offset_to_row_idx_t& updateInfo,
        const offset_set_t& deleteInfo) override {
        // Avoid unused parameter warnings during release build.
        (void)isNewNodeGroup;
        (void)updateInfo;
        (void)deleteInfo;
        KU_ASSERT(isNewNodeGroup && updateInfo.empty() && deleteInfo.empty());
        // Only when a new node group is created, we need to commit out of place.
        auto numValues = 0u;
        for (auto& [offsetInChunk, _] : insertInfo) {
            numValues = offsetInChunk >= numValues ? offsetInChunk + 1 : numValues;
        }
        ColumnChunkMetadata chunkMeta;
        chunkMeta.numValues = numValues;
        metadataDA->resize(nodeGroupIdx + 1);
        metadataDA->update(nodeGroupIdx, chunkMeta);
    }

    void commitColumnChunkInPlace(common::node_group_idx_t /*nodeGroupIdx*/,
        const std::vector<common::offset_t>& /*dstOffset*/, ColumnChunk* /*chunk*/,
        common::offset_t /*srcOffsetInChunk*/) override {
        // Note: only updates to rel table can trigger this code path. SERIAL is not supported in
        // rel table yet.
        KU_UNREACHABLE;
    }

    void commitColumnChunkOutOfPlace(Transaction* /*transaction*/,
        common::node_group_idx_t /*nodeGroupIdx*/, bool /*isNewNodeGroup*/,
        const std::vector<common::offset_t>& /*dstOffset*/, ColumnChunk* /*chunk*/,
        common::offset_t /*srcOffsetInChunk*/) override {
        // Note: only updates to rel table can trigger this code path. SERIAL is not supported in
        // rel table yet.
        KU_UNREACHABLE;
    }

    void append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) override {
        ColumnChunkMetadata metadata;
        metadata.numValues = columnChunk->getNumValues();
        metadataDA->resize(nodeGroupIdx + 1);
        metadataDA->update(nodeGroupIdx, metadata);
    }
};

InternalIDColumn::InternalIDColumn(std::string name, const MetadataDAHInfo& metaDAHeaderInfo,
    BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
    transaction::Transaction* transaction, RWPropertyStats stats, bool enableCompression)
    : Column{name, *LogicalType::INTERNAL_ID(), metaDAHeaderInfo, dataFH, metadataFH, bufferManager,
          wal, transaction, stats, enableCompression, false /*requireNullColumn*/},
      commonTableID{INVALID_TABLE_ID} {}

void InternalIDColumn::populateCommonTableID(ValueVector* resultVector) const {
    auto nodeIDs = ((internalID_t*)resultVector->getData());
    for (auto i = 0u; i < resultVector->state->selVector->selectedSize; i++) {
        auto pos = resultVector->state->selVector->selectedPositions[i];
        nodeIDs[pos].tableID = commonTableID;
    }
}

Column::Column(std::string name, LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
    BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
    transaction::Transaction* transaction, RWPropertyStats propertyStatistics,
    bool enableCompression, bool requireNullColumn)
    : name{std::move(name)}, dbFileID{DBFileID::newDataFileID()}, dataType{std::move(dataType)},
      dataFH{dataFH}, metadataFH{metadataFH}, bufferManager{bufferManager}, wal{wal},
      propertyStatistics{propertyStatistics}, enableCompression{enableCompression} {
    metadataDA = std::make_unique<InMemDiskArray<ColumnChunkMetadata>>(*metadataFH,
        DBFileID::newMetadataFileID(), metaDAHeaderInfo.dataDAHPageIdx, bufferManager, wal,
        transaction);
    numBytesPerFixedSizedValue = getDataTypeSizeInChunk(this->dataType);
    readToVectorFunc = getReadValuesToVectorFunc(this->dataType);
    readToPageFunc = ReadCompressedValuesFromPage(this->dataType);
    batchLookupFunc = ReadCompressedValuesFromPage(this->dataType);
    writeFromVectorFunc = getWriteValueFromVectorFunc(this->dataType);
    writeFunc = getWriteValuesFunc(this->dataType);
    KU_ASSERT(numBytesPerFixedSizedValue <= BufferPoolConstants::PAGE_4KB_SIZE);
    if (requireNullColumn) {
        auto columnName =
            StorageUtils::getColumnName(this->name, StorageUtils::ColumnType::NULL_MASK, "");
        nullColumn =
            std::make_unique<NullColumn>(columnName, metaDAHeaderInfo.nullDAHPageIdx, dataFH,
                metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression);
    }
}

Column::~Column() = default;

Column* Column::getNullColumn() {
    return nullColumn.get();
}

// NOTE: This function should only be called on node tables.
void Column::batchLookup(Transaction* transaction, const offset_t* nodeOffsets, size_t size,
    uint8_t* result) {
    for (auto i = 0u; i < size; ++i) {
        auto nodeOffset = nodeOffsets[i];
        auto [nodeGroupIdx, offsetInChunk] =
            StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
        auto cursor = getPageCursorForOffset(transaction->getType(), nodeGroupIdx, offsetInChunk);
        auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
        KU_ASSERT(isPageIdxValid(cursor.pageIdx, chunkMeta));
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
    auto state = getReadState(transaction->getType(), nodeGroupIdx);
    auto pageCursor = getPageCursorForOffsetInGroup(startOffsetInGroup, state);
    auto numValuesToScan = endOffsetInGroup - startOffsetInGroup;
    scanUnfiltered(transaction, pageCursor, numValuesToScan, resultVector, state.metadata,
        offsetInVector);
}

void Column::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx, ColumnChunk* columnChunk,
    offset_t startOffset, offset_t endOffset) {
    if (nullColumn) {
        nullColumn->scan(transaction, nodeGroupIdx, columnChunk->getNullChunk(), startOffset,
            endOffset);
    }
    if (nodeGroupIdx >= metadataDA->getNumElements(transaction->getType())) {
        columnChunk->setNumValues(0);
    } else {
        auto chunkMetadata = metadataDA->get(nodeGroupIdx, transaction->getType());
        if (chunkMetadata.numValues == 0) {
            columnChunk->setNumValues(0);
            return;
        }
        uint64_t numValuesPerPage =
            chunkMetadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
        auto cursor = PageUtils::getPageCursorForPos(startOffset, numValuesPerPage);
        cursor.pageIdx += chunkMetadata.pageIdx;
        uint64_t numValuesScanned = 0u;
        endOffset = std::min(endOffset, chunkMetadata.numValues);
        KU_ASSERT(endOffset >= startOffset);
        auto numValuesToScan = endOffset - startOffset;
        if (numValuesToScan > columnChunk->getCapacity()) {
            columnChunk->resize(std::bit_ceil(numValuesToScan));
        }
        KU_ASSERT((numValuesToScan + startOffset) <= chunkMetadata.numValues);
        while (numValuesScanned < numValuesToScan) {
            auto numValuesToReadInPage = std::min(numValuesPerPage - cursor.elemPosInPage,
                numValuesToScan - numValuesScanned);
            KU_ASSERT(isPageIdxValid(cursor.pageIdx, chunkMetadata));
            readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
                readToPageFunc(frame, cursor, columnChunk->getData(), numValuesScanned,
                    numValuesToReadInPage, chunkMetadata.compMeta);
            });
            numValuesScanned += numValuesToReadInPage;
            cursor.nextPage();
        }
        KU_ASSERT(numValuesScanned == numValuesToScan);
        columnChunk->setNumValues(numValuesScanned);
    }
}

void Column::scan(Transaction* transaction, const ReadState& state, offset_t startOffsetInGroup,
    offset_t endOffsetInGroup, uint8_t* result) {
    auto cursor = getPageCursorForOffsetInGroup(startOffsetInGroup, state);
    auto numValuesToScan = endOffsetInGroup - startOffsetInGroup;
    uint64_t numValuesScanned = 0;
    while (numValuesScanned < numValuesToScan) {
        uint64_t numValuesToScanInPage =
            std::min((uint64_t)state.numValuesPerPage - cursor.elemPosInPage,
                numValuesToScan - numValuesScanned);
        readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
            readToPageFunc(frame, cursor, result, numValuesScanned, numValuesToScanInPage,
                state.metadata.compMeta);
        });
        numValuesScanned += numValuesToScanInPage;
        cursor.nextPage();
    }
}

void Column::scanInternal(Transaction* transaction, ValueVector* nodeIDVector,
    ValueVector* resultVector) {
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    KU_ASSERT(startNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
    // TODO: replace with state
    auto [nodeGroupIdx, offsetInChunk] =
        StorageUtils::getNodeGroupIdxAndOffsetInChunk(startNodeOffset);
    auto cursor = getPageCursorForOffset(transaction->getType(), nodeGroupIdx, offsetInChunk);
    auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
    if (nodeIDVector->state->selVector->isUnfiltered()) {
        scanUnfiltered(transaction, cursor, nodeIDVector->state->selVector->selectedSize,
            resultVector, chunkMeta);
    } else {
        scanFiltered(transaction, cursor, nodeIDVector, resultVector, chunkMeta);
    }
}

void Column::scanUnfiltered(Transaction* transaction, PageCursor& pageCursor,
    uint64_t numValuesToScan, ValueVector* resultVector, const ColumnChunkMetadata& chunkMeta,
    uint64_t startPosInVector) {
    uint64_t numValuesScanned = 0;
    auto numValuesPerPage =
        chunkMeta.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
    while (numValuesScanned < numValuesToScan) {
        uint64_t numValuesToScanInPage =
            std::min((uint64_t)numValuesPerPage - pageCursor.elemPosInPage,
                numValuesToScan - numValuesScanned);
        KU_ASSERT(isPageIdxValid(pageCursor.pageIdx, chunkMeta));
        readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
            readToVectorFunc(frame, pageCursor, resultVector, numValuesScanned + startPosInVector,
                numValuesToScanInPage, chunkMeta.compMeta);
        });
        numValuesScanned += numValuesToScanInPage;
        pageCursor.nextPage();
    }
}

void Column::scanFiltered(Transaction* transaction, PageCursor& pageCursor,
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
            KU_ASSERT(isPageIdxValid(pageCursor.pageIdx, chunkMeta));
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

void Column::lookup(Transaction* transaction, ValueVector* nodeIDVector,
    ValueVector* resultVector) {
    if (nullColumn) {
        nullColumn->lookup(transaction, nodeIDVector, resultVector);
    }
    lookupInternal(transaction, nodeIDVector, resultVector);
}

void Column::lookupInternal(transaction::Transaction* transaction, ValueVector* nodeIDVector,
    ValueVector* resultVector) {
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
    auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
    auto cursor = getPageCursorForOffset(transaction->getType(), nodeGroupIdx, offsetInChunk);
    auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
    KU_ASSERT(isPageIdxValid(cursor.pageIdx, chunkMeta));
    readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
        readToVectorFunc(frame, cursor, resultVector, posInVector, 1 /* numValuesToRead */,
            chunkMeta.compMeta);
    });
}

void Column::readFromPage(Transaction* transaction, page_idx_t pageIdx,
    const std::function<void(uint8_t*)>& func) {
    // For constant compression, call read on a nullptr since there is no data on disk and
    // decompression only requires metadata
    if (pageIdx == INVALID_PAGE_IDX) {
        return func(nullptr);
    }
    auto [fileHandleToPin, pageIdxToPin] = DBFileUtils::getFileHandleAndPhysicalPageIdxToPin(
        *dataFH, pageIdx, *wal, transaction->getType());
    bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, func);
}

static bool sanityCheckForWrites(const ColumnChunkMetadata& metadata, const LogicalType& dataType) {
    if (metadata.compMeta.compression == CompressionType::CONSTANT) {
        return metadata.numPages == 0;
    } else {
        auto numValuesPerPage =
            metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
        if (numValuesPerPage == UINT64_MAX) {
            return metadata.numPages == 0;
        } else {
            return std::ceil((double)metadata.numValues / (double)numValuesPerPage) <=
                   metadata.numPages;
        }
    }
}

void Column::append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) {
    KU_ASSERT(enableCompression == columnChunk->isCompressionEnabled());
    KU_ASSERT(columnChunk->sanityCheck());
    // Main column chunk.
    auto preScanMetadata = columnChunk->getMetadataToFlush();
    auto startPageIdx = dataFH->addNewPages(preScanMetadata.numPages);
    auto metadata = columnChunk->flushBuffer(dataFH, startPageIdx, preScanMetadata);
    KU_ASSERT(sanityCheckForWrites(metadata, dataType));
    metadataDA->resize(nodeGroupIdx + 1);
    metadataDA->update(nodeGroupIdx, metadata);
    if (nullColumn) {
        // Null column chunk.
        nullColumn->append(columnChunk->getNullChunk(), nodeGroupIdx);
    }
}

void Column::write(node_group_idx_t nodeGroupIdx, offset_t offsetInChunk,
    ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    bool isNull = vectorToWriteFrom->isNull(posInVectorToWriteFrom);
    auto chunkMeta = metadataDA->get(nodeGroupIdx, TransactionType::WRITE);
    if (!isNull) {
        writeValue(chunkMeta, nodeGroupIdx, offsetInChunk, vectorToWriteFrom,
            posInVectorToWriteFrom);
    }
    if (offsetInChunk >= chunkMeta.numValues) {
        chunkMeta.numValues = offsetInChunk + 1;
        KU_ASSERT(sanityCheckForWrites(chunkMeta, dataType));
        metadataDA->update(nodeGroupIdx, chunkMeta);
    }
}

void Column::writeValue(const ColumnChunkMetadata& chunkMeta, node_group_idx_t nodeGroupIdx,
    offset_t offsetInChunk, ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    updatePageWithCursor(
        getPageCursorForOffset(TransactionType::WRITE, nodeGroupIdx, offsetInChunk),
        [&](auto frame, auto posInPage) {
            writeFromVectorFunc(frame, posInPage, vectorToWriteFrom, posInVectorToWriteFrom,
                chunkMeta.compMeta);
        });
}

void Column::write(node_group_idx_t nodeGroupIdx, offset_t offsetInChunk, ColumnChunk* data,
    offset_t dataOffset, length_t numValues) {
    auto state = getReadState(TransactionType::WRITE, nodeGroupIdx);
    writeValues(state, offsetInChunk, data->getData(), dataOffset, numValues);
    if (offsetInChunk + numValues > state.metadata.numValues) {
        state.metadata.numValues = offsetInChunk + numValues;
        KU_ASSERT(sanityCheckForWrites(state.metadata, dataType));
        metadataDA->update(nodeGroupIdx, state.metadata);
    }
}

void Column::writeValues(ReadState& state, common::offset_t dstOffset, const uint8_t* data,
    common::offset_t srcOffset, common::offset_t numValues) {
    auto numValuesWritten = 0u;
    auto cursor = getPageCursorForOffsetInGroup(dstOffset, state);
    while (numValuesWritten < numValues) {
        auto numValuesToWriteInPage =
            std::min(numValues - numValuesWritten, state.numValuesPerPage - cursor.elemPosInPage);
        updatePageWithCursor(cursor, [&](auto frame, auto offsetInPage) {
            writeFunc(frame, offsetInPage, data, srcOffset + numValuesWritten,
                numValuesToWriteInPage, state.metadata.compMeta);
        });

        numValuesWritten += numValuesToWriteInPage;
        cursor.nextPage();
    }
}

offset_t Column::appendValues(node_group_idx_t nodeGroupIdx, const uint8_t* data,
    offset_t numValues) {
    auto state = getReadState(TransactionType::WRITE, nodeGroupIdx);
    auto startOffset = state.metadata.numValues;
    auto numPages = dataFH->getNumPages();
    writeValues(state, state.metadata.numValues, data, 0 /*dataOffset*/, numValues);
    auto newNumPages = dataFH->getNumPages();
    state.metadata.numValues += numValues;
    state.metadata.numPages += (newNumPages - numPages);
    metadataDA->update(nodeGroupIdx, state.metadata);
    return startOffset;
}

PageCursor Column::getPageCursorForOffsetInGroup(offset_t offsetInChunk, const ReadState& state) {
    auto pageCursor = PageUtils::getPageCursorForPos(offsetInChunk, state.numValuesPerPage);
    pageCursor.pageIdx += state.metadata.pageIdx;
    return pageCursor;
}

void Column::updatePageWithCursor(PageCursor cursor,
    const std::function<void(uint8_t*, offset_t)>& writeOp) {
    bool insertingNewPage = false;
    if (cursor.pageIdx == INVALID_PAGE_IDX) {
        return writeOp(nullptr, cursor.elemPosInPage);
    } else if (cursor.pageIdx >= dataFH->getNumPages()) {
        KU_ASSERT(cursor.pageIdx == dataFH->getNumPages());
        DBFileUtils::insertNewPage(*dataFH, dbFileID, *bufferManager, *wal);
        insertingNewPage = true;
    }
    DBFileUtils::updatePage(*dataFH, dbFileID, cursor.pageIdx, insertingNewPage, *bufferManager,
        *wal, [&](auto frame) { writeOp(frame, cursor.elemPosInPage); });
}

Column::ReadState Column::getReadState(TransactionType transactionType,
    node_group_idx_t nodeGroupIdx) const {
    auto metadata = metadataDA->get(nodeGroupIdx, transactionType);
    return {metadata, metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType)};
}

void Column::prepareCommit() {
    metadataDA->prepareCommit();
    if (nullColumn) {
        nullColumn->prepareCommit();
    }
}

void Column::prepareCommitForChunk(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    const ChunkCollection& localInsertChunks, const offset_to_row_idx_t& insertInfo,
    const ChunkCollection& localUpdateChunks, const offset_to_row_idx_t& updateInfo,
    const offset_set_t& deleteInfo) {
    auto currentNumNodeGroups = metadataDA->getNumElements(transaction->getType());
    auto isNewNodeGroup = nodeGroupIdx >= currentNumNodeGroups;
    if (isNewNodeGroup) {
        // If this is a new node group, updateInfo should be empty. We should perform out-of-place
        // commit with a new column chunk.
        commitLocalChunkOutOfPlace(transaction, nodeGroupIdx, isNewNodeGroup, localInsertChunks,
            insertInfo, localUpdateChunks, updateInfo, deleteInfo);
    } else {
        bool didInPlaceCommit = false;
        // If this is not a new node group, we should first check if we can perform in-place commit.
        if (canCommitInPlace(transaction, nodeGroupIdx, localInsertChunks, insertInfo,
                localUpdateChunks, updateInfo)) {
            commitLocalChunkInPlace(transaction, nodeGroupIdx, localInsertChunks, insertInfo,
                localUpdateChunks, updateInfo, deleteInfo);
            didInPlaceCommit = true;
        } else {
            commitLocalChunkOutOfPlace(transaction, nodeGroupIdx, isNewNodeGroup, localInsertChunks,
                insertInfo, localUpdateChunks, updateInfo, deleteInfo);
        }
        // TODO(Guodong/Ben): The logic here on NullColumn is confusing as out-of-place commits and
        // in-place commits handle it differently. See if we can unify them.
        if (nullColumn) {
            // Uses functions written for the null chunk which only access the localColumnChunk's
            // null information
            if (nullColumn->canCommitInPlace(transaction, nodeGroupIdx,
                    getNullChunkCollection(localInsertChunks), insertInfo,
                    getNullChunkCollection(localUpdateChunks), updateInfo)) {
                nullColumn->commitLocalChunkInPlace(transaction, nodeGroupIdx,
                    getNullChunkCollection(localInsertChunks), insertInfo,
                    getNullChunkCollection(localUpdateChunks), updateInfo, deleteInfo);
            } else if (didInPlaceCommit) {
                // Out-of-place commits also commit the null chunk out of place,
                // so we only need to do a separate out of place commit for the null chunk if the
                // main chunk did an in-place commit.
                nullColumn->commitLocalChunkOutOfPlace(transaction, nodeGroupIdx, isNewNodeGroup,
                    getNullChunkCollection(localInsertChunks), insertInfo,
                    getNullChunkCollection(localUpdateChunks), updateInfo, deleteInfo);
            }
        }
    }
}

void Column::prepareCommitForChunk(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    const std::vector<common::offset_t>& dstOffsets, ColumnChunk* chunk, offset_t startSrcOffset) {
    metadataDA->prepareCommit();
    auto currentNumNodeGroups = metadataDA->getNumElements(transaction->getType());
    auto isNewNodeGroup = nodeGroupIdx >= currentNumNodeGroups;
    if (isNewNodeGroup) {
        commitColumnChunkOutOfPlace(transaction, nodeGroupIdx, isNewNodeGroup, dstOffsets, chunk,
            startSrcOffset);
    } else {
        bool didInPlaceCommit = false;
        // If this is not a new node group, we should first check if we can perform in-place commit.
        if (canCommitInPlace(transaction, nodeGroupIdx, dstOffsets, chunk, startSrcOffset)) {
            commitColumnChunkInPlace(nodeGroupIdx, dstOffsets, chunk, startSrcOffset);
            didInPlaceCommit = true;
        } else {
            commitColumnChunkOutOfPlace(transaction, nodeGroupIdx, isNewNodeGroup, dstOffsets,
                chunk, startSrcOffset);
        }
        if (nullColumn) {
            if (nullColumn->canCommitInPlace(transaction, nodeGroupIdx, dstOffsets,
                    chunk->getNullChunk(), startSrcOffset)) {
                nullColumn->commitColumnChunkInPlace(nodeGroupIdx, dstOffsets,
                    chunk->getNullChunk(), startSrcOffset);
            } else if (didInPlaceCommit) {
                nullColumn->commitColumnChunkOutOfPlace(transaction, nodeGroupIdx, isNewNodeGroup,
                    dstOffsets, chunk->getNullChunk(), startSrcOffset);
            }
        }
    }
}

bool Column::isInsertionsOutOfPagesCapacity(const ColumnChunkMetadata& metadata,
    const offset_to_row_idx_t& insertInfo) {
    auto maxOffset = 0u;
    for (auto& [offset, rowIdx] : insertInfo) {
        if (offset > maxOffset) {
            maxOffset = offset;
        }
    }
    return isMaxOffsetOutOfPagesCapacity(metadata, maxOffset);
}

bool Column::isMaxOffsetOutOfPagesCapacity(const ColumnChunkMetadata& metadata,
    offset_t maxOffset) {
    if (metadata.compMeta.compression != CompressionType::CONSTANT &&
        (metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType) *
            metadata.numPages) <= (maxOffset + 1)) {
        // Note that for constant compression, `metadata.numPages` will be equal to 0. Thus, this
        // function will always return true.
        return true;
    }
    return false;
}

bool Column::checkUpdateInPlace(const ColumnChunkMetadata& metadata,
    const ChunkCollection& localChunks, const offset_to_row_idx_t& writeInfo) {
    std::vector<row_idx_t> rowIdxesToRead;
    for (auto& [_, rowIdx] : writeInfo) {
        rowIdxesToRead.push_back(rowIdx);
    }
    std::sort(rowIdxesToRead.begin(), rowIdxesToRead.end());
    for (auto rowIdx : rowIdxesToRead) {
        auto [chunkIdx, offsetInLocalChunk] =
            LocalChunkedGroupCollection::getChunkIdxAndOffsetInChunk(rowIdx);
        if (localChunks[chunkIdx]->getNullChunk() != nullptr &&
            localChunks[chunkIdx]->getNullChunk()->isNull(offsetInLocalChunk)) {
            continue;
        }
        if (!metadata.compMeta.canUpdateInPlace(localChunks[chunkIdx]->getData(),
                offsetInLocalChunk, dataType.getPhysicalType())) {
            return false;
        }
    }
    return true;
}

bool Column::canCommitInPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    const ChunkCollection& localInsertChunks, const offset_to_row_idx_t& insertInfo,
    const ChunkCollection& localUpdateChunks, const offset_to_row_idx_t& updateInfo) {
    auto metadata = getMetadata(nodeGroupIdx, transaction->getType());
    if (isInsertionsOutOfPagesCapacity(metadata, insertInfo)) {
        return false;
    }
    if (metadata.compMeta.canAlwaysUpdateInPlace()) {
        return true;
    }
    return checkUpdateInPlace(metadata, localInsertChunks, insertInfo) &&
           checkUpdateInPlace(metadata, localUpdateChunks, updateInfo);
}

bool Column::canCommitInPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    const std::vector<common::offset_t>& dstOffsets, ColumnChunk* chunk,
    common::offset_t srcOffset) {
    auto maxDstOffset = getMaxOffset(dstOffsets);
    auto metadata = getMetadata(nodeGroupIdx, transaction->getType());
    if (isMaxOffsetOutOfPagesCapacity(metadata, maxDstOffset)) {
        return false;
    }
    if (metadata.compMeta.canAlwaysUpdateInPlace()) {
        return true;
    }
    for (auto i = 0u; i < dstOffsets.size(); i++) {
        if (!metadata.compMeta.canUpdateInPlace(chunk->getData(), srcOffset + i,
                dataType.getPhysicalType())) {
            return false;
        }
    }
    return true;
}

void Column::commitLocalChunkInPlace(Transaction* /*transaction*/, node_group_idx_t nodeGroupIdx,
    const ChunkCollection& localInsertChunks, const offset_to_row_idx_t& insertInfo,
    const ChunkCollection& localUpdateChunks, const offset_to_row_idx_t& updateInfo,
    const offset_set_t& /*deleteInfo*/) {
    applyLocalChunkToColumn(nodeGroupIdx, localUpdateChunks, updateInfo);
    applyLocalChunkToColumn(nodeGroupIdx, localInsertChunks, insertInfo);
}

std::unique_ptr<ColumnChunk> Column::getEmptyChunkForCommit(uint64_t capacity) {
    return ColumnChunkFactory::createColumnChunk(*dataType.copy(), enableCompression, capacity);
}

void Column::commitLocalChunkOutOfPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    bool isNewNodeGroup, const ChunkCollection& localInsertChunks,
    const offset_to_row_idx_t& insertInfo, const ChunkCollection& localUpdateChunks,
    const offset_to_row_idx_t& updateInfo, const offset_set_t& deleteInfo) {
    auto columnChunk = getEmptyChunkForCommit(common::StorageConstants::NODE_GROUP_SIZE);
    if (isNewNodeGroup) {
        KU_ASSERT(updateInfo.empty() && deleteInfo.empty());
        // Apply inserts from the local chunk.
        applyLocalChunkToColumnChunk(localInsertChunks, columnChunk.get(), insertInfo);
    } else {
        // First, scan the whole column chunk from persistent storage.
        scan(transaction, nodeGroupIdx, columnChunk.get());
        // Then, apply updates from the local chunk.
        applyLocalChunkToColumnChunk(localUpdateChunks, columnChunk.get(), updateInfo);
        // Lastly, apply inserts from the local chunk.
        applyLocalChunkToColumnChunk(localInsertChunks, columnChunk.get(), insertInfo);
        if (columnChunk->getNullChunk()) {
            // Set nulls based on deleteInfo.
            for (auto offsetInChunk : deleteInfo) {
                columnChunk->getNullChunk()->setNull(offsetInChunk, true /* isNull */);
            }
        }
    }
    columnChunk->finalize();
    KU_ASSERT(columnChunk->sanityCheck());
    append(columnChunk.get(), nodeGroupIdx);
}

void Column::commitColumnChunkInPlace(node_group_idx_t nodeGroupIdx,
    const std::vector<offset_t>& dstOffsets, ColumnChunk* chunk, offset_t srcOffset) {
    for (auto i = 0u; i < dstOffsets.size(); i++) {
        write(nodeGroupIdx, dstOffsets[i], chunk, srcOffset + i, 1 /* numValues */);
    }
}

void Column::commitColumnChunkOutOfPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    bool isNewNodeGroup, const std::vector<offset_t>& dstOffsets, ColumnChunk* chunk,
    offset_t srcOffset) {
    if (isNewNodeGroup) {
        chunk->finalize();
        append(chunk, nodeGroupIdx);
    } else {
        auto chunkMeta = getMetadata(nodeGroupIdx, transaction->getType());
        // TODO(Guodong): Should consider caching the scanned column chunk to avoid redundant
        // scans in the same transaction.
        auto columnChunk =
            getEmptyChunkForCommit(1.5 * std::bit_ceil(chunkMeta.numValues + dstOffsets.size()));
        scan(transaction, nodeGroupIdx, columnChunk.get());
        for (auto i = 0u; i < dstOffsets.size(); i++) {
            columnChunk->write(chunk, srcOffset + i, dstOffsets[i], 1 /* numValues */);
        }
        columnChunk->finalize();
        append(columnChunk.get(), nodeGroupIdx);
    }
}

void Column::applyLocalChunkToColumnChunk(const ChunkCollection& localChunks,
    ColumnChunk* columnChunk, const offset_to_row_idx_t& updateInfo) {
    for (auto& [offsetInDstChunk, rowIdx] : updateInfo) {
        auto [chunkIdx, offsetInLocalChunk] =
            LocalChunkedGroupCollection::getChunkIdxAndOffsetInChunk(rowIdx);
        columnChunk->write(localChunks[chunkIdx], offsetInLocalChunk, offsetInDstChunk,
            1 /* numValues */);
    }
}

void Column::applyLocalChunkToColumn(node_group_idx_t nodeGroupIdx,
    const ChunkCollection& localChunks, const offset_to_row_idx_t& updateInfo) {
    for (auto& [offsetInDstChunk, rowIdx] : updateInfo) {
        auto [chunkIdx, offsetInLocalChunk] =
            LocalChunkedGroupCollection::getChunkIdxAndOffsetInChunk(rowIdx);
        if (!localChunks[chunkIdx]->getNullChunk()->isNull(offsetInLocalChunk)) {
            write(nodeGroupIdx, offsetInDstChunk, localChunks[chunkIdx], offsetInLocalChunk,
                1 /*numValues*/);
        } else {
            auto chunkMeta = metadataDA->get(nodeGroupIdx, TransactionType::WRITE);
            if (offsetInDstChunk >= chunkMeta.numValues) {
                chunkMeta.numValues = offsetInDstChunk + 1;
                KU_ASSERT(sanityCheckForWrites(chunkMeta, dataType));
                metadataDA->update(nodeGroupIdx, chunkMeta);
            }
        }
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

void Column::populateWithDefaultVal(Transaction* transaction,
    InMemDiskArray<ColumnChunkMetadata>* metadataDA_, ValueVector* defaultValueVector) {
    KU_ASSERT(metadataDA_ != nullptr);
    auto numNodeGroups = metadataDA_->getNumElements(transaction->getType());
    auto columnChunk = ColumnChunkFactory::createColumnChunk(*dataType.copy(), enableCompression);
    columnChunk->populateWithDefaultVal(defaultValueVector);
    for (auto i = 0u; i < numNodeGroups; i++) {
        auto chunkMeta = metadataDA_->get(i, transaction->getType());
        auto capacity = columnChunk->getCapacity();
        while (capacity < chunkMeta.numValues) {
            capacity *= CHUNK_RESIZE_RATIO;
        }
        if (capacity > columnChunk->getCapacity()) {
            auto newColumnChunk = ColumnChunkFactory::createColumnChunk(*dataType.copy(),
                enableCompression, capacity);
            newColumnChunk->populateWithDefaultVal(defaultValueVector);
            newColumnChunk->setNumValues(chunkMeta.numValues);
            append(newColumnChunk.get(), i);
        } else {
            columnChunk->setNumValues(chunkMeta.numValues);
            append(columnChunk.get(), i);
        }
    }
}

PageCursor Column::getPageCursorForOffset(TransactionType transactionType,
    node_group_idx_t nodeGroupIdx, offset_t offsetInChunk) {
    auto state = getReadState(transactionType, nodeGroupIdx);
    return getPageCursorForOffsetInGroup(offsetInChunk, state);
}

ChunkCollection Column::getNullChunkCollection(const ChunkCollection& chunkCollection) {
    ChunkCollection nullChunkCollection;
    for (const auto& chunk : chunkCollection) {
        nullChunkCollection.push_back(chunk->getNullChunk());
    }
    return nullChunkCollection;
}

std::unique_ptr<Column> ColumnFactory::createColumn(std::string name, LogicalType dataType,
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
    case LogicalTypeID::UUID:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DATE:
    case LogicalTypeID::TIMESTAMP:
    case LogicalTypeID::TIMESTAMP_MS:
    case LogicalTypeID::TIMESTAMP_NS:
    case LogicalTypeID::TIMESTAMP_SEC:
    case LogicalTypeID::TIMESTAMP_TZ:
    case LogicalTypeID::INTERVAL: {
        return std::make_unique<Column>(name, std::move(dataType), metaDAHeaderInfo, dataFH,
            metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression);
    }
    case LogicalTypeID::INTERNAL_ID: {
        return std::make_unique<InternalIDColumn>(name, metaDAHeaderInfo, dataFH, metadataFH,
            bufferManager, wal, transaction, propertyStatistics, enableCompression);
    }
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING: {
        return std::make_unique<StringColumn>(name, std::move(dataType), metaDAHeaderInfo, dataFH,
            metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression);
    }
    case LogicalTypeID::ARRAY:
    case LogicalTypeID::MAP:
    case LogicalTypeID::LIST: {
        return std::make_unique<ListColumn>(name, std::move(dataType), metaDAHeaderInfo, dataFH,
            metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression);
    }
    case LogicalTypeID::UNION:
    case LogicalTypeID::STRUCT:
    case LogicalTypeID::RDF_VARIANT: {
        return std::make_unique<StructColumn>(name, std::move(dataType), metaDAHeaderInfo, dataFH,
            metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression);
    }
    case LogicalTypeID::SERIAL: {
        return std::make_unique<SerialColumn>(name, metaDAHeaderInfo, dataFH, metadataFH,
            bufferManager, wal, transaction);
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

} // namespace storage
} // namespace kuzu

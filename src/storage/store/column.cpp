#include "storage/store/column.h"

#include <memory>

#include "common/assert.h"
#include "common/null_mask.h"
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

static bool isPageIdxValid(page_idx_t pageIdx, const ColumnChunkMetadata& metadata) { // NOLINT
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
        offset_t numValues, const CompressionMetadata& metadata, const NullMask* nullMask) {
        compressedWriter(frame, posInFrame, data, dataOffset, numValues, metadata, nullMask);
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

    void scan(Transaction*, ChunkState&, ValueVector* nodeIDVector,
        ValueVector* resultVector) override {
        // Serial column cannot contain null values.
        auto& selVector = nodeIDVector->state->getSelVector();
        for (auto i = 0ul; i < selVector.getSelSize(); i++) {
            auto pos = selVector[i];
            auto offset = nodeIDVector->readNodeOffset(pos);
            resultVector->setNull(pos, false);
            resultVector->setValue<offset_t>(pos, offset);
        }
    }

    void lookup(Transaction*, ChunkState&, ValueVector* nodeIDVector,
        ValueVector* resultVector) override {
        // Serial column cannot contain null values.
        auto& selVector = nodeIDVector->state->getSelVector();
        for (auto i = 0ul; i < selVector.getSelSize(); i++) {
            auto pos = selVector[i];
            auto offset = nodeIDVector->readNodeOffset(pos);
            resultVector->setNull(pos, false);
            resultVector->setValue<offset_t>(pos, offset);
        }
    }

    bool canCommitInPlace(const ChunkState&, const ChunkCollection&, const offset_to_row_idx_t&,
        const ChunkCollection&, const offset_to_row_idx_t& updateInfo) override {
        (void)updateInfo; // Avoid unused parameter warnings during release build.
        KU_ASSERT(updateInfo.empty());
        return true;
    }

    bool canCommitInPlace(const ChunkState&, const std::vector<common::offset_t>&, ColumnChunk*,
        common::offset_t) override {
        // Note: only updates to rel table can trigger this code path. SERIAL is not supported in
        // rel table yet.
        KU_UNREACHABLE;
    }

    void commitLocalChunkInPlace(ChunkState& state, const ChunkCollection&,
        const offset_to_row_idx_t& insertInfo, const ChunkCollection&,
        const offset_to_row_idx_t& updateInfo, const offset_set_t& deleteInfo) override {
        // Avoid unused parameter warnings during release build.
        (void)updateInfo;
        (void)deleteInfo;
        KU_ASSERT(updateInfo.empty() && deleteInfo.empty());
        auto numValues = state.metadata.numValues;
        for (auto& [offsetInChunk, _] : insertInfo) {
            if (offsetInChunk >= numValues) {
                numValues = offsetInChunk + 1;
            }
        }
        if (numValues > state.metadata.numValues) {
            state.metadata.numValues = numValues;
            // TODO: Avoid updating metadataDA.
            metadataDA->update(state.nodeGroupIdx, state.metadata);
        }
    }

    void commitLocalChunkOutOfPlace(Transaction*, node_group_idx_t nodeGroupIdx,
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

    void commitColumnChunkInPlace(ChunkState&, const std::vector<common::offset_t>&, ColumnChunk*,
        common::offset_t) override {
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
    auto& selVector = resultVector->state->getSelVector();
    for (auto i = 0u; i < selVector.getSelSize(); i++) {
        auto pos = selVector[i];
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
    metadataDA =
        std::make_unique<DiskArray<ColumnChunkMetadata>>(*metadataFH, DBFileID::newMetadataFileID(),
            metaDAHeaderInfo.dataDAHPageIdx, bufferManager, wal, transaction);
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
        ChunkState state;
        initChunkState(transaction, nodeGroupIdx, state);
        auto cursor = getPageCursorForOffsetInGroup(offsetInChunk, state);
        auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
        KU_ASSERT(isPageIdxValid(cursor.pageIdx, chunkMeta));
        readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
            batchLookupFunc(frame, cursor, result, i, 1, chunkMeta.compMeta);
        });
    }
}

void Column::initChunkState(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    ChunkState& readState) {
    if (nullColumn) {
        if (!readState.nullState) {
            readState.nullState = std::make_unique<ChunkState>();
        }
        nullColumn->initChunkState(transaction, nodeGroupIdx, *readState.nullState);
    }
    KU_ASSERT(nodeGroupIdx < metadataDA->getNumElements(transaction->getType()));
    if (nodeGroupIdx != readState.nodeGroupIdx) {
        // Only update metadata and numValuesPerPage if we're reading a different node group.
        // This is an optimization to reduce accesses to metadataDA, which can lead to contention
        // due to lock acquiring in DiskArray.
        readState.nodeGroupIdx = nodeGroupIdx;
        readState.metadata = metadataDA->get(nodeGroupIdx, transaction->getType());
        readState.numValuesPerPage =
            readState.metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
    }
}

void Column::scan(Transaction* transaction, ChunkState& readState, ValueVector* nodeIDVector,
    ValueVector* resultVector) {
    if (nullColumn) {
        KU_ASSERT(readState.nullState);
        nullColumn->scan(transaction, *readState.nullState, nodeIDVector, resultVector);
    }
    scanInternal(transaction, readState, nodeIDVector, resultVector);
}

void Column::scan(Transaction* transaction, ChunkState& readState, offset_t startOffsetInGroup,
    offset_t endOffsetInGroup, ValueVector* resultVector, uint64_t offsetInVector) {
    if (nullColumn) {
        KU_ASSERT(readState.nullState);
        nullColumn->scan(transaction, *readState.nullState, startOffsetInGroup, endOffsetInGroup,
            resultVector, offsetInVector);
    }
    auto pageCursor = getPageCursorForOffsetInGroup(startOffsetInGroup, readState);
    auto numValuesToScan = endOffsetInGroup - startOffsetInGroup;
    scanUnfiltered(transaction, pageCursor, numValuesToScan, resultVector, readState.metadata,
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
        startOffset = std::min(startOffset, chunkMetadata.numValues);
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

void Column::scan(Transaction* transaction, const ChunkState& state, offset_t startOffsetInGroup,
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

void Column::scanInternal(Transaction* transaction, ChunkState& readState,
    ValueVector* nodeIDVector, ValueVector* resultVector) {
    auto [nodeGroupIdx, startOffsetInChunk] =
        StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeIDVector->readNodeOffset(0));
    KU_ASSERT(nodeGroupIdx == readState.nodeGroupIdx);
    auto cursor = getPageCursorForOffsetInGroup(startOffsetInChunk, readState);
    if (nodeIDVector->state->getSelVector().isUnfiltered()) {
        scanUnfiltered(transaction, cursor, nodeIDVector->state->getSelVector().getSelSize(),
            resultVector, readState.metadata);
    } else {
        scanFiltered(transaction, cursor, nodeIDVector, resultVector, readState.metadata);
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
    auto& selVector = nodeIDVector->state->getSelVector();
    while (numValuesScanned < numValuesToScan) {
        uint64_t numValuesToScanInPage =
            std::min((uint64_t)numValuesPerPage - pageCursor.elemPosInPage,
                numValuesToScan - numValuesScanned);
        if (isInRange(selVector[posInSelVector], numValuesScanned,
                numValuesScanned + numValuesToScanInPage)) {
            KU_ASSERT(isPageIdxValid(pageCursor.pageIdx, chunkMeta));
            readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
                readToVectorFunc(frame, pageCursor, resultVector, numValuesScanned,
                    numValuesToScanInPage, chunkMeta.compMeta);
            });
        }
        numValuesScanned += numValuesToScanInPage;
        pageCursor.nextPage();
        while (posInSelVector < selVector.getSelSize() &&
               selVector[posInSelVector] < numValuesScanned) {
            posInSelVector++;
        }
    }
}

void Column::lookup(Transaction* transaction, ChunkState& readState, ValueVector* nodeIDVector,
    ValueVector* resultVector) {
    if (nullColumn) {
        KU_ASSERT(readState.nullState);
        nullColumn->lookup(transaction, *readState.nullState, nodeIDVector, resultVector);
    }
    lookupInternal(transaction, readState, nodeIDVector, resultVector);
}

void Column::lookupInternal(transaction::Transaction* transaction, ChunkState& readState,
    ValueVector* nodeIDVector, ValueVector* resultVector) {
    auto& selVector = nodeIDVector->state->getSelVector();
    for (auto i = 0ul; i < selVector.getSelSize(); i++) {
        auto pos = selVector[i];
        if (nodeIDVector->isNull(pos)) {
            continue;
        }
        auto nodeOffset = nodeIDVector->readNodeOffset(pos);
        lookupValue(transaction, readState, nodeOffset, resultVector, pos);
    }
}

void Column::lookupValue(transaction::Transaction* transaction, ChunkState& readState,
    offset_t nodeOffset, ValueVector* resultVector, uint32_t posInVector) {
    auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
    auto cursor = getPageCursorForOffsetInGroup(offsetInChunk, readState);
    KU_ASSERT(isPageIdxValid(cursor.pageIdx, readState.metadata));
    readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
        readToVectorFunc(frame, cursor, resultVector, posInVector, 1 /* numValuesToRead */,
            readState.metadata.compMeta);
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

static bool sanityCheckForWrites(const ColumnChunkMetadata& metadata,
    const LogicalType& dataType) { // NOLINT
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

void Column::write(ChunkState& state, offset_t offsetInChunk, ValueVector* vectorToWriteFrom,
    uint32_t posInVectorToWriteFrom) {
    bool isNull = vectorToWriteFrom->isNull(posInVectorToWriteFrom);
    if (!isNull) {
        writeValue(state, offsetInChunk, vectorToWriteFrom, posInVectorToWriteFrom);
    }
    if (offsetInChunk >= state.metadata.numValues) {
        state.metadata.numValues = offsetInChunk + 1;
    }
}

void Column::writeValue(ChunkState& state, offset_t offsetInChunk, ValueVector* vectorToWriteFrom,
    uint32_t posInVectorToWriteFrom) {
    updatePageWithCursor(getPageCursorForOffsetInGroup(offsetInChunk, state),
        [&](auto frame, auto posInPage) {
            writeFromVectorFunc(frame, posInPage, vectorToWriteFrom, posInVectorToWriteFrom,
                state.metadata.compMeta);
        });
}

void Column::write(ChunkState& state, offset_t offsetInChunk, ColumnChunk* data,
    offset_t dataOffset, length_t numValues) {
    NullMask nullMask{std::span<uint64_t>()};
    NullMask* nullMaskPtr = nullptr;
    if (data->getNullChunk()) {
        nullMask = data->getNullChunk()->getNullMask();
        nullMaskPtr = &nullMask;
    }
    writeValues(state, offsetInChunk, data->getData(), nullMaskPtr, dataOffset, numValues);
    if (offsetInChunk + numValues > state.metadata.numValues) {
        state.metadata.numValues = offsetInChunk + numValues;
    }
}

void Column::writeValues(ChunkState& state, common::offset_t dstOffset, const uint8_t* data,
    const common::NullMask* nullChunkData, common::offset_t srcOffset, common::offset_t numValues) {
    auto numValuesWritten = 0u;
    auto cursor = getPageCursorForOffsetInGroup(dstOffset, state);
    while (numValuesWritten < numValues) {
        auto numValuesToWriteInPage =
            std::min(numValues - numValuesWritten, state.numValuesPerPage - cursor.elemPosInPage);
        updatePageWithCursor(cursor, [&](auto frame, auto offsetInPage) {
            writeFunc(frame, offsetInPage, data, srcOffset + numValuesWritten,
                numValuesToWriteInPage, state.metadata.compMeta, nullChunkData);
        });
        numValuesWritten += numValuesToWriteInPage;
        cursor.nextPage();
    }
}

// Apend to the end of the chunk.
offset_t Column::appendValues(ChunkState& state, const uint8_t* data,
    const common::NullMask* nullChunkData, offset_t numValues) {
    auto startOffset = state.metadata.numValues;
    auto numPages = dataFH->getNumPages();
    // TODO: writeValues should return new pages appended if any.
    writeValues(state, state.metadata.numValues, data, nullChunkData, 0 /*dataOffset*/, numValues);
    auto newNumPages = dataFH->getNumPages();
    state.metadata.numValues += numValues;
    state.metadata.numPages += (newNumPages - numPages);
    metadataDA->update(state.nodeGroupIdx, state.metadata);
    return startOffset;
}

PageCursor Column::getPageCursorForOffsetInGroup(offset_t offsetInChunk, const ChunkState& state) {
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

void Column::prepareCommit() {
    metadataDA->prepareCommit();
    if (nullColumn) {
        nullColumn->prepareCommit();
    }
}

void Column::prepareCommitForChunk(Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
    bool isNewNodeGroup, const ChunkCollection& localInsertChunks,
    const offset_to_row_idx_t& insertInfo, const ChunkCollection& localUpdateChunks,
    const offset_to_row_idx_t& updateInfo, const offset_set_t& deleteInfo) {
    if (isNewNodeGroup) {
        // If this is a new node group, updateInfo should be empty. We should perform out-of-place
        // commit with a new column chunk.
        commitLocalChunkOutOfPlace(transaction, nodeGroupIdx, isNewNodeGroup, localInsertChunks,
            insertInfo, localUpdateChunks, updateInfo, deleteInfo);
    } else {
        ChunkState state;
        initChunkState(transaction, nodeGroupIdx, state);
        prepareCommitForExistingChunk(transaction, state, localInsertChunks, insertInfo,
            localUpdateChunks, updateInfo, deleteInfo);
    }
}

void Column::prepareCommitForChunk(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    bool isNewNodeGroup, const std::vector<common::offset_t>& dstOffsets, ColumnChunk* chunk,
    offset_t startSrcOffset) {
    metadataDA->prepareCommit();
    if (isNewNodeGroup) {
        commitColumnChunkOutOfPlace(transaction, nodeGroupIdx, isNewNodeGroup, dstOffsets, chunk,
            startSrcOffset);
    } else {
        // If this is not a new node group, we should first check if we can perform in-place commit.
        ChunkState state;
        initChunkState(transaction, nodeGroupIdx, state);
        prepareCommitForExistingChunk(transaction, state, dstOffsets, chunk, startSrcOffset);
    }
}

void Column::prepareCommitForExistingChunk(Transaction* transaction, Column::ChunkState& state,
    const ChunkCollection& localInsertChunks, const offset_to_row_idx_t& insertInfo,
    const ChunkCollection& localUpdateChunks, const offset_to_row_idx_t& updateInfo,
    const offset_set_t& deleteInfo) {
    // If this is not a new node group, we should first check if we can perform in-place commit.
    if (canCommitInPlace(state, localInsertChunks, insertInfo, localUpdateChunks, updateInfo)) {
        commitLocalChunkInPlace(state, localInsertChunks, insertInfo, localUpdateChunks, updateInfo,
            deleteInfo);
        KU_ASSERT(sanityCheckForWrites(state.metadata, dataType));
        metadataDA->update(state.nodeGroupIdx, state.metadata);
        if (nullColumn) {
            KU_ASSERT(state.nullState);
            auto nullInsertChunks = getNullChunkCollection(localInsertChunks);
            auto nullUpdateChunks = getNullChunkCollection(localUpdateChunks);
            nullColumn->prepareCommitForExistingChunk(transaction, *state.nullState,
                nullInsertChunks, insertInfo, nullUpdateChunks, updateInfo, deleteInfo);
        }
    } else {
        commitLocalChunkOutOfPlace(transaction, state.nodeGroupIdx, false /*isNewNodeGroup*/,
            localInsertChunks, insertInfo, localUpdateChunks, updateInfo, deleteInfo);
    }
}

void Column::prepareCommitForExistingChunk(Transaction* transaction, ChunkState& state,
    const std::vector<offset_t>& dstOffsets, ColumnChunk* chunk, offset_t startSrcOffset) {
    if (canCommitInPlace(state, dstOffsets, chunk, startSrcOffset)) {
        commitColumnChunkInPlace(state, dstOffsets, chunk, startSrcOffset);
        KU_ASSERT(sanityCheckForWrites(state.metadata, dataType));
        metadataDA->update(state.nodeGroupIdx, state.metadata);
        if (nullColumn) {
            nullColumn->prepareCommitForExistingChunk(transaction, *state.nullState, dstOffsets,
                chunk->getNullChunk(), startSrcOffset);
        }
    } else {
        commitColumnChunkOutOfPlace(transaction, state.nodeGroupIdx, false /*isNewNodeGroup*/,
            dstOffsets, chunk, startSrcOffset);
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

bool Column::canCommitInPlace(const ChunkState& state, const ChunkCollection& localInsertChunks,
    const offset_to_row_idx_t& insertInfo, const ChunkCollection& localUpdateChunks,
    const offset_to_row_idx_t& updateInfo) {
    if (isInsertionsOutOfPagesCapacity(state.metadata, insertInfo)) {
        return false;
    }
    if (state.metadata.compMeta.canAlwaysUpdateInPlace()) {
        return true;
    }
    return checkUpdateInPlace(state.metadata, localInsertChunks, insertInfo) &&
           checkUpdateInPlace(state.metadata, localUpdateChunks, updateInfo);
}

bool Column::canCommitInPlace(const ChunkState& state,
    const std::vector<common::offset_t>& dstOffsets, ColumnChunk* chunk,
    common::offset_t srcOffset) {
    auto maxDstOffset = getMaxOffset(dstOffsets);
    if (isMaxOffsetOutOfPagesCapacity(state.metadata, maxDstOffset)) {
        return false;
    }
    if (state.metadata.compMeta.canAlwaysUpdateInPlace()) {
        return true;
    }
    for (auto i = 0u; i < dstOffsets.size(); i++) {
        if (!state.metadata.compMeta.canUpdateInPlace(chunk->getData(), srcOffset + i,
                dataType.getPhysicalType())) {
            return false;
        }
    }
    return true;
}

void Column::commitLocalChunkInPlace(ChunkState& state, const ChunkCollection& localInsertChunks,
    const offset_to_row_idx_t& insertInfo, const ChunkCollection& localUpdateChunks,
    const offset_to_row_idx_t& updateInfo, const offset_set_t&) {
    applyLocalChunkToColumn(state, localUpdateChunks, updateInfo);
    applyLocalChunkToColumn(state, localInsertChunks, insertInfo);
}

std::unique_ptr<ColumnChunk> Column::getEmptyChunkForCommit(uint64_t capacity) {
    return ColumnChunkFactory::createColumnChunk(*dataType.copy(), enableCompression, capacity);
}

// TODO: Pass state in to avoid access metadata.
void Column::commitLocalChunkOutOfPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    bool isNewNodeGroup, const ChunkCollection& localInsertChunks,
    const offset_to_row_idx_t& insertInfo, const ChunkCollection& localUpdateChunks,
    const offset_to_row_idx_t& updateInfo, const offset_set_t& deleteInfo) {
    std::unique_ptr<ColumnChunk> columnChunk;
    if (isNewNodeGroup) {
        KU_ASSERT(updateInfo.empty() && deleteInfo.empty());
        columnChunk = getEmptyChunkForCommit(getMaxOffset(insertInfo) + 1);
        // Apply inserts from the local chunk.
        applyLocalChunkToColumnChunk(localInsertChunks, columnChunk.get(), insertInfo);
    } else {
        auto maxNodeOffset = std::max(getMaxOffset(insertInfo), getMaxOffset(updateInfo));
        auto chunkMeta = getMetadata(nodeGroupIdx, transaction->getType());
        maxNodeOffset = std::max(maxNodeOffset, chunkMeta.numValues);
        columnChunk = getEmptyChunkForCommit(maxNodeOffset + 1);
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

void Column::commitColumnChunkInPlace(ChunkState& state, const std::vector<offset_t>& dstOffsets,
    ColumnChunk* chunk, offset_t srcOffset) {
    // TODO: Should always sort dstOffsets, and group writes to the same page.
    for (auto i = 0u; i < dstOffsets.size(); i++) {
        write(state, dstOffsets[i], chunk, srcOffset + i, 1 /* numValues */);
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
        auto columnChunk = getEmptyChunkForCommit(
            std::max(std::bit_ceil(chunkMeta.numValues), getMaxOffset(dstOffsets) + 1));
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

void Column::applyLocalChunkToColumn(ChunkState& state, const ChunkCollection& localChunks,
    const offset_to_row_idx_t& updateInfo) {
    for (auto& [offsetInDstChunk, rowIdx] : updateInfo) {
        auto [chunkIdx, offsetInLocalChunk] =
            LocalChunkedGroupCollection::getChunkIdxAndOffsetInChunk(rowIdx);
        if (!localChunks[chunkIdx]->getNullChunk()->isNull(offsetInLocalChunk)) {
            write(state, offsetInDstChunk, localChunks[chunkIdx], offsetInLocalChunk,
                1 /*numValues*/);
        } else {
            if (offsetInDstChunk >= state.metadata.numValues) {
                state.metadata.numValues = offsetInDstChunk + 1;
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
    DiskArray<ColumnChunkMetadata>* metadataDA_, ValueVector* defaultValueVector) {
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
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::BOOL:
    case PhysicalTypeID::INT64:
    case PhysicalTypeID::INT32:
    case PhysicalTypeID::INT16:
    case PhysicalTypeID::INT8:
    case PhysicalTypeID::UINT64:
    case PhysicalTypeID::UINT32:
    case PhysicalTypeID::UINT16:
    case PhysicalTypeID::UINT8:
    case PhysicalTypeID::INT128:
    case PhysicalTypeID::DOUBLE:
    case PhysicalTypeID::FLOAT:
    case PhysicalTypeID::INTERVAL: {
        if (dataType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
            return std::make_unique<SerialColumn>(name, metaDAHeaderInfo, dataFH, metadataFH,
                bufferManager, wal, transaction);
        } else {
            return std::make_unique<Column>(name, std::move(dataType), metaDAHeaderInfo, dataFH,
                metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression);
        }
    }
    case PhysicalTypeID::INTERNAL_ID: {
        return std::make_unique<InternalIDColumn>(name, metaDAHeaderInfo, dataFH, metadataFH,
            bufferManager, wal, transaction, propertyStatistics, enableCompression);
    }
    case PhysicalTypeID::STRING: {
        return std::make_unique<StringColumn>(name, std::move(dataType), metaDAHeaderInfo, dataFH,
            metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression);
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        return std::make_unique<ListColumn>(name, std::move(dataType), metaDAHeaderInfo, dataFH,
            metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression);
    }
    case PhysicalTypeID::STRUCT: {
        return std::make_unique<StructColumn>(name, std::move(dataType), metaDAHeaderInfo, dataFH,
            metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression);
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

} // namespace storage
} // namespace kuzu

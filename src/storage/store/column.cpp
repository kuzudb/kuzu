#include "storage/store/column.h"

#include <algorithm>
#include <cstdint>

#include "common/assert.h"
#include "common/null_mask.h"
#include "common/types/types.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/compression/compression.h"
#include "storage/storage_structure/db_file_utils.h"
#include "storage/storage_utils.h"
#include "storage/store/column_chunk.h"
#include "storage/store/column_chunk_data.h"
#include "storage/store/list_column.h"
#include "storage/store/null_column.h"
#include "storage/store/string_column.h"
#include "storage/store/struct_column.h"
#include "transaction/transaction.h"
#include <bit>

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;
using namespace kuzu::evaluator;

namespace kuzu {
namespace storage {

static bool isPageIdxValid(page_idx_t pageIdx, const ColumnChunkMetadata& metadata) {
    return (metadata.pageIdx <= pageIdx && pageIdx < metadata.pageIdx + metadata.numPages) ||
           (pageIdx == INVALID_PAGE_IDX && metadata.compMeta.isConstant());
}

struct ReadInternalIDValuesToVector {
    ReadInternalIDValuesToVector() : compressedReader{LogicalType(LogicalTypeID::INTERNAL_ID)} {}
    void operator()(const uint8_t* frame, PageCursor& pageCursor, ValueVector* resultVector,
        uint32_t posInVector, uint32_t numValuesToRead, const CompressionMetadata& metadata) {
        KU_ASSERT(resultVector->dataType.getPhysicalType() == PhysicalTypeID::INTERNAL_ID);

        auto buffer = std::make_unique<offset_t[]>(numValuesToRead);
        compressedReader(frame, pageCursor, reinterpret_cast<uint8_t*>(buffer.get()), 0,
            numValuesToRead, metadata);
        auto resultData = reinterpret_cast<internalID_t*>(resultVector->getData());
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

InternalIDColumn::InternalIDColumn(std::string name, BMFileHandle* dataFH,
    BufferManager* bufferManager, ShadowFile* shadowFile, bool enableCompression)
    : Column{std::move(name), LogicalType::INTERNAL_ID(), dataFH, bufferManager, shadowFile,
          enableCompression, false /*requireNullColumn*/},
      commonTableID{INVALID_TABLE_ID} {}

void InternalIDColumn::populateCommonTableID(const ValueVector* resultVector) const {
    auto nodeIDs = reinterpret_cast<internalID_t*>(resultVector->getData());
    auto& selVector = resultVector->state->getSelVector();
    for (auto i = 0u; i < selVector.getSelSize(); i++) {
        const auto pos = selVector[i];
        nodeIDs[pos].tableID = commonTableID;
    }
}

Column::Column(std::string name, LogicalType dataType, BMFileHandle* dataFH,
    BufferManager* bufferManager, ShadowFile* shadowFile, bool enableCompression,
    bool requireNullColumn)
    : name{std::move(name)}, dbFileID{DBFileID::newDataFileID()}, dataType{std::move(dataType)},
      dataFH{dataFH}, bufferManager{bufferManager}, shadowFile{shadowFile},
      enableCompression{enableCompression} {
    readToVectorFunc = getReadValuesToVectorFunc(this->dataType);
    readToPageFunc = ReadCompressedValuesFromPage(this->dataType);
    batchLookupFunc = ReadCompressedValuesFromPage(this->dataType);
    writeFromVectorFunc = getWriteValueFromVectorFunc(this->dataType);
    writeFunc = getWriteValuesFunc(this->dataType);
    if (requireNullColumn) {
        auto columnName =
            StorageUtils::getColumnName(this->name, StorageUtils::ColumnType::NULL_MASK, "");
        nullColumn = std::make_unique<NullColumn>(columnName, dataFH, bufferManager, shadowFile,
            enableCompression);
    }
}

Column::~Column() = default;

Column* Column::getNullColumn() const {
    return nullColumn.get();
}

std::unique_ptr<ColumnChunkData> Column::flushChunkData(const ColumnChunkData& chunkData,
    BMFileHandle& dataFH) {
    switch (chunkData.getDataType().getPhysicalType()) {
    case PhysicalTypeID::STRUCT: {
        return StructColumn::flushChunkData(chunkData, dataFH);
    }
    case PhysicalTypeID::STRING: {
        return StringColumn::flushChunkData(chunkData, dataFH);
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        return ListColumn::flushChunkData(chunkData, dataFH);
    }
    default: {
        return flushNonNestedChunkData(chunkData, dataFH);
    }
    }
}

std::unique_ptr<ColumnChunkData> Column::flushNonNestedChunkData(const ColumnChunkData& chunkData,
    BMFileHandle& dataFH) {
    auto chunkMeta = flushData(chunkData, dataFH);
    auto flushedChunk = ColumnChunkFactory::createColumnChunkData(chunkData.getDataType().copy(),
        chunkData.isCompressionEnabled(), chunkMeta, chunkData.hasNullData());
    if (chunkData.hasNullData()) {
        auto nullChunkMeta = flushData(chunkData.getNullData(), dataFH);
        auto nullData =
            std::make_unique<NullChunkData>(chunkData.isCompressionEnabled(), nullChunkMeta);
        flushedChunk->setNullData(std::move(nullData));
    }
    return flushedChunk;
}

ColumnChunkMetadata Column::flushData(const ColumnChunkData& chunkData, BMFileHandle& dataFH) {
    KU_ASSERT(chunkData.sanityCheck());
    // TODO(Guodong/Ben): We can optimize the flush to write back to same set of pages if new
    // flushed data are not out of the capacity.
    const auto preScanMetadata = chunkData.getMetadataToFlush();
    const auto startPageIdx = dataFH.addNewPages(preScanMetadata.numPages);
    return chunkData.flushBuffer(&dataFH, startPageIdx, preScanMetadata);
}

void Column::scan(Transaction* transaction, const ChunkState& state, offset_t startOffsetInChunk,
    row_idx_t numValuesToScan, ValueVector* nodeIDVector, ValueVector* resultVector) {
    if (nullColumn) {
        KU_ASSERT(state.nullState);
        nullColumn->scan(transaction, *state.nullState, startOffsetInChunk, numValuesToScan,
            nodeIDVector, resultVector);
    }
    scanInternal(transaction, state, startOffsetInChunk, numValuesToScan, nodeIDVector,
        resultVector);
}

void Column::scan(Transaction* transaction, const ChunkState& state, offset_t startOffsetInGroup,
    offset_t endOffsetInGroup, ValueVector* resultVector, uint64_t offsetInVector) {
    if (nullColumn) {
        KU_ASSERT(state.nullState);
        nullColumn->scan(transaction, *state.nullState, startOffsetInGroup, endOffsetInGroup,
            resultVector, offsetInVector);
    }
    auto pageCursor = getPageCursorForOffsetInGroup(startOffsetInGroup, state);
    const auto numValuesToScan = endOffsetInGroup - startOffsetInGroup;
    scanUnfiltered(transaction, pageCursor, numValuesToScan, resultVector, state.metadata,
        offsetInVector);
}

void Column::scan(Transaction* transaction, const ChunkState& state, ColumnChunkData* columnChunk,
    offset_t startOffset, offset_t endOffset) {
    if (nullColumn) {
        nullColumn->scan(transaction, *state.nullState, columnChunk->getNullData(), startOffset,
            endOffset);
    }

    startOffset = std::min(startOffset, state.metadata.numValues);
    endOffset = std::min(endOffset, state.metadata.numValues);
    KU_ASSERT(endOffset >= startOffset);
    const auto numValuesToScan = endOffset - startOffset;
    if (numValuesToScan > columnChunk->getCapacity()) {
        columnChunk->resize(std::bit_ceil(numValuesToScan));
    }
    if (getDataTypeSizeInChunk(dataType) == 0) {
        columnChunk->setNumValues(numValuesToScan);
        return;
    }

    const uint64_t numValuesPerPage =
        state.metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
    auto cursor = PageUtils::getPageCursorForPos(startOffset, numValuesPerPage);
    cursor.pageIdx += state.metadata.pageIdx;
    uint64_t numValuesScanned = 0u;
    KU_ASSERT((numValuesToScan + startOffset) <= state.metadata.numValues);
    while (numValuesScanned < numValuesToScan) {
        auto numValuesToReadInPage =
            std::min(numValuesPerPage - cursor.elemPosInPage, numValuesToScan - numValuesScanned);
        KU_ASSERT(isPageIdxValid(cursor.pageIdx, state.metadata));
        readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
            readToPageFunc(frame, cursor, columnChunk->getData(), numValuesScanned,
                numValuesToReadInPage, state.metadata.compMeta);
        });
        numValuesScanned += numValuesToReadInPage;
        cursor.nextPage();
    }
    KU_ASSERT(numValuesScanned == numValuesToScan);
    columnChunk->setNumValues(numValuesScanned);
}

void Column::scan(Transaction* transaction, const ChunkState& state, offset_t startOffsetInGroup,
    offset_t endOffsetInGroup, uint8_t* result) {
    auto cursor = getPageCursorForOffsetInGroup(startOffsetInGroup, state);
    const auto numValuesToScan = endOffsetInGroup - startOffsetInGroup;
    uint64_t numValuesScanned = 0;
    while (numValuesScanned < numValuesToScan) {
        uint64_t numValuesToScanInPage =
            std::min(static_cast<uint64_t>(state.numValuesPerPage) - cursor.elemPosInPage,
                numValuesToScan - numValuesScanned);
        readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
            readToPageFunc(frame, cursor, result, numValuesScanned, numValuesToScanInPage,
                state.metadata.compMeta);
        });
        numValuesScanned += numValuesToScanInPage;
        cursor.nextPage();
    }
}

void Column::scanInternal(Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInChunk, row_idx_t numValuesToScan, ValueVector* nodeIDVector,
    ValueVector* resultVector) {
    auto cursor = getPageCursorForOffsetInGroup(startOffsetInChunk, state);
    if (nodeIDVector->state->getSelVector().isUnfiltered()) {
        scanUnfiltered(transaction, cursor, numValuesToScan, resultVector, state.metadata);
    } else {
        scanFiltered(transaction, cursor, numValuesToScan, nodeIDVector->state->getSelVector(),
            resultVector, state.metadata);
    }
}

void Column::scanUnfiltered(Transaction* transaction, PageCursor& pageCursor,
    uint64_t numValuesToScan, ValueVector* resultVector, const ColumnChunkMetadata& chunkMeta,
    uint64_t startPosInVector) const {
    uint64_t numValuesScanned = 0;
    const auto numValuesPerPage =
        chunkMeta.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
    while (numValuesScanned < numValuesToScan) {
        uint64_t numValuesToScanInPage = std::min(numValuesPerPage - pageCursor.elemPosInPage,
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
    uint64_t numValuesToScan, const SelectionVector& selVector, ValueVector* resultVector,
    const ColumnChunkMetadata& chunkMeta) const {
    auto numValuesScanned = 0u;
    auto posInSelVector = 0u;
    const auto numValuesPerPage =
        chunkMeta.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
    while (numValuesScanned < numValuesToScan) {
        uint64_t numValuesToScanInPage = std::min(numValuesPerPage - pageCursor.elemPosInPage,
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

void Column::lookupValue(Transaction* transaction, const ChunkState& state, offset_t nodeOffset,
    ValueVector* resultVector, uint32_t posInVector) {
    if (nullColumn) {
        nullColumn->lookupValue(transaction, *state.nullState, nodeOffset, resultVector,
            posInVector);
    }
    if (resultVector->isNull(posInVector)) {
        return;
    }
    lookupInternal(transaction, state, nodeOffset, resultVector, posInVector);
}

void Column::lookupInternal(Transaction* transaction, const ChunkState& state, offset_t nodeOffset,
    ValueVector* resultVector, uint32_t posInVector) {
    auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
    auto cursor = getPageCursorForOffsetInGroup(offsetInChunk, state);
    KU_ASSERT(isPageIdxValid(cursor.pageIdx, state.metadata));
    readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
        readToVectorFunc(frame, cursor, resultVector, posInVector, 1 /* numValuesToRead */,
            state.metadata.compMeta);
    });
}

void Column::readFromPage(Transaction* transaction, page_idx_t pageIdx,
    const std::function<void(uint8_t*)>& func) const {
    // For constant compression, call read on a nullptr since there is no data on disk and
    // decompression only requires metadata
    if (pageIdx == INVALID_PAGE_IDX) {
        return func(nullptr);
    }
    auto [fileHandleToPin, pageIdxToPin] = DBFileUtils::getFileHandleAndPhysicalPageIdxToPin(
        *dataFH, pageIdx, *shadowFile, transaction->getType());
    fileHandleToPin->optimisticReadPage(pageIdxToPin, func);
}

static bool sanityCheckForWrites(const ColumnChunkMetadata& metadata, const LogicalType& dataType) {
    if (metadata.compMeta.compression == CompressionType::CONSTANT) {
        return metadata.numPages == 0;
    }
    const auto numValuesPerPage =
        metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
    if (numValuesPerPage == UINT64_MAX) {
        return metadata.numPages == 0;
    }
    return std::ceil(static_cast<double>(metadata.numValues) /
                     static_cast<double>(numValuesPerPage)) <= metadata.numPages;
}

void Column::updateStatistics(ColumnChunkMetadata& metadata, offset_t maxIndex,
    const std::optional<StorageValue>& min, const std::optional<StorageValue>& max) const {
    if (maxIndex >= metadata.numValues) {
        metadata.numValues = maxIndex + 1;
        KU_ASSERT(sanityCheckForWrites(metadata, dataType));
    }
    // Either both or neither should be provided
    KU_ASSERT((!min && !max) || (min && max));
    if (min && max) {
        // FIXME(bmwinger): this is causing test failures
        // If new values are outside of the existing min/max, update them
        if (max->gt(metadata.compMeta.max, dataType.getPhysicalType())) {
            metadata.compMeta.max = *max;
        } else if (metadata.compMeta.min.gt(*min, dataType.getPhysicalType())) {
            metadata.compMeta.min = *min;
        }
    }
}

void Column::write(ColumnChunkData& persistentChunk, ChunkState& state, offset_t dstOffset,
    ColumnChunkData* data, offset_t srcOffset, length_t numValues) {
    std::optional<NullMask> nullMask = data->getNullMask();
    NullMask* nullMaskPtr = nullptr;
    if (nullMask) {
        nullMaskPtr = &*nullMask;
    }
    writeValues(persistentChunk, state, dstOffset, data->getData(), nullMaskPtr, srcOffset,
        numValues);

    auto [minWritten, maxWritten] = getMinMaxStorageValue(data->getData(), srcOffset, numValues,
        dataType.getPhysicalType(), nullMaskPtr);
    updateStatistics(persistentChunk.getMetadata(), dstOffset + numValues - 1, minWritten,
        maxWritten);
}

void Column::writeValues(ColumnChunkData&, ChunkState& state, offset_t dstOffset,
    const uint8_t* data, const NullMask* nullChunkData, offset_t srcOffset, offset_t numValues) {
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
offset_t Column::appendValues(ColumnChunkData& persistentChunk, ChunkState& state,
    const uint8_t* data, const NullMask* nullChunkData, offset_t numValues) {
    const auto startOffset = state.metadata.numValues;
    const auto numPages = dataFH->getNumPages();
    // TODO: writeValues should return new pages appended if any.
    writeValues(persistentChunk, state, state.metadata.numValues, data, nullChunkData,
        0 /*dataOffset*/, numValues);
    const auto newNumPages = dataFH->getNumPages();
    auto& metadata = persistentChunk.getMetadata();
    metadata.numPages += (newNumPages - numPages);

    auto [minWritten, maxWritten] = getMinMaxStorageValue(data, 0 /*offset*/, numValues,
        dataType.getPhysicalType(), nullChunkData);
    updateStatistics(metadata, startOffset + numValues - 1, minWritten, maxWritten);
    return startOffset;
}

PageCursor Column::getPageCursorForOffsetInGroup(offset_t offsetInChunk, const ChunkState& state) {
    auto pageCursor = PageUtils::getPageCursorForPos(offsetInChunk, state.numValuesPerPage);
    pageCursor.pageIdx += state.metadata.pageIdx;
    return pageCursor;
}

void Column::updatePageWithCursor(PageCursor cursor,
    const std::function<void(uint8_t*, offset_t)>& writeOp) const {
    bool insertingNewPage = false;
    if (cursor.pageIdx == INVALID_PAGE_IDX) {
        return writeOp(nullptr, cursor.elemPosInPage);
    }
    if (cursor.pageIdx >= dataFH->getNumPages()) {
        KU_ASSERT(cursor.pageIdx == dataFH->getNumPages());
        DBFileUtils::insertNewPage(*dataFH, dbFileID, *shadowFile);
        insertingNewPage = true;
    }
    DBFileUtils::updatePage(*dataFH, dbFileID, cursor.pageIdx, insertingNewPage, *shadowFile,
        [&](auto frame) { writeOp(frame, cursor.elemPosInPage); });
}

bool Column::isMaxOffsetOutOfPagesCapacity(const ColumnChunkMetadata& metadata,
    offset_t maxOffset) const {
    if (metadata.compMeta.compression != CompressionType::CONSTANT &&
        (metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType) *
            metadata.numPages) <= (maxOffset + 1)) {
        // Note that for constant compression, `metadata.numPages` will be equal to 0. Thus, this
        // function will always return true.
        return true;
    }
    return false;
}

void Column::checkpointColumnChunkInPlace(ChunkState& state,
    const ColumnCheckpointState& checkpointState) {
    for (auto& chunkCheckpointState : checkpointState.chunkCheckpointStates) {
        KU_ASSERT(chunkCheckpointState.numRows > 0);
        write(checkpointState.persistentData, state, chunkCheckpointState.startRow,
            chunkCheckpointState.chunkData.get(), 0 /*srcOffset*/, chunkCheckpointState.numRows);
    }
    checkpointState.persistentData.resetNumValuesFromMetadata();
    if (nullColumn) {
        checkpointNullData(checkpointState);
    }
}

void Column::checkpointNullData(const ColumnCheckpointState& checkpointState) const {
    std::vector<ChunkCheckpointState> nullChunkCheckpointStates;
    for (const auto& chunkCheckpointState : checkpointState.chunkCheckpointStates) {
        KU_ASSERT(chunkCheckpointState.chunkData->hasNullData());
        ChunkCheckpointState nullChunkCheckpointState(
            chunkCheckpointState.chunkData->moveNullData(), chunkCheckpointState.startRow,
            chunkCheckpointState.numRows);
        nullChunkCheckpointStates.push_back(std::move(nullChunkCheckpointState));
    }
    KU_ASSERT(checkpointState.persistentData.hasNullData());
    ColumnCheckpointState nullColumnCheckpointState(*checkpointState.persistentData.getNullData(),
        std::move(nullChunkCheckpointStates));
    nullColumn->checkpointColumnChunk(nullColumnCheckpointState);
}

void Column::checkpointColumnChunkOutOfPlace(ChunkState& state,
    const ColumnCheckpointState& checkpointState) {
    const auto numRows = std::max(checkpointState.maxRowIdxToWrite + 1, state.metadata.numValues);
    checkpointState.persistentData.setToInMemory();
    checkpointState.persistentData.resize(numRows);
    scan(&DUMMY_CHECKPOINT_TRANSACTION, state, &checkpointState.persistentData);
    for (auto& chunkCheckpointState : checkpointState.chunkCheckpointStates) {
        checkpointState.persistentData.write(chunkCheckpointState.chunkData.get(), 0 /*srcOffset*/,
            chunkCheckpointState.startRow, chunkCheckpointState.numRows);
    }
    checkpointState.persistentData.finalize();
    checkpointState.persistentData.flush(*dataFH);
}

bool Column::canCheckpointInPlace(const ChunkState& state,
    const ColumnCheckpointState& checkpointState) {
    if (isMaxOffsetOutOfPagesCapacity(checkpointState.persistentData.getMetadata(),
            checkpointState.maxRowIdxToWrite)) {
        return false;
    }
    if (checkpointState.persistentData.getMetadata().compMeta.canAlwaysUpdateInPlace()) {
        return true;
    }
    for (auto& chunkCheckpointState : checkpointState.chunkCheckpointStates) {
        auto& chunkData = chunkCheckpointState.chunkData;
        KU_ASSERT(chunkData->getNumValues() == chunkCheckpointState.numRows);
        if (chunkData->getNumValues() != 0 &&
            !state.metadata.compMeta.canUpdateInPlace(chunkData->getData(), 0,
                chunkData->getNumValues(), dataType.getPhysicalType(), chunkData->getNullMask())) {
            return false;
        }
    }
    return true;
}

void Column::checkpointColumnChunk(ColumnCheckpointState& checkpointState) {
    ChunkState chunkState;
    checkpointState.persistentData.initializeScanState(chunkState);
    if (canCheckpointInPlace(chunkState, checkpointState)) {
        checkpointColumnChunkInPlace(chunkState, checkpointState);
    } else {
        checkpointColumnChunkOutOfPlace(chunkState, checkpointState);
    }
}

std::unique_ptr<Column> ColumnFactory::createColumn(std::string name, LogicalType dataType,
    BMFileHandle* dataFH, BufferManager* bufferManager, ShadowFile* shadowFile,
    bool enableCompression) {
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
        return std::make_unique<Column>(name, std::move(dataType), dataFH, bufferManager,
            shadowFile, enableCompression);
    }
    case PhysicalTypeID::INTERNAL_ID: {
        return std::make_unique<InternalIDColumn>(name, dataFH, bufferManager, shadowFile,
            enableCompression);
    }
    case PhysicalTypeID::STRING: {
        return std::make_unique<StringColumn>(name, std::move(dataType), dataFH, bufferManager,
            shadowFile, enableCompression);
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        return std::make_unique<ListColumn>(name, std::move(dataType), dataFH, bufferManager,
            shadowFile, enableCompression);
    }
    case PhysicalTypeID::STRUCT: {
        return std::make_unique<StructColumn>(name, std::move(dataType), dataFH, bufferManager,
            shadowFile, enableCompression);
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

} // namespace storage
} // namespace kuzu

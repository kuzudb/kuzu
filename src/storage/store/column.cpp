#include "storage/store/column.h"

#include <algorithm>
#include <cstdint>

#include "common/assert.h"
#include "common/null_mask.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/compression/compression.h"
#include "storage/file_handle.h"
#include "storage/page_manager.h"
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

struct ReadInternalIDValuesToVector {
    ReadInternalIDValuesToVector() : compressedReader{LogicalType(LogicalTypeID::INTERNAL_ID)} {}
    void operator()(const uint8_t* frame, PageCursor& pageCursor, ValueVector* resultVector,
        uint32_t posInVector, uint32_t numValuesToRead, const CompressionMetadata& metadata) {
        KU_ASSERT(resultVector->dataType.getPhysicalType() == PhysicalTypeID::INTERNAL_ID);

        KU_ASSERT(numValuesToRead <= DEFAULT_VECTOR_CAPACITY);
        offset_t offsetBuffer[DEFAULT_VECTOR_CAPACITY];

        compressedReader(frame, pageCursor, reinterpret_cast<uint8_t*>(offsetBuffer), 0,
            numValuesToRead, metadata);
        auto resultData = reinterpret_cast<internalID_t*>(resultVector->getData());
        for (auto i = 0u; i < numValuesToRead; i++) {
            resultData[posInVector + i].offset = offsetBuffer[i];
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
        uint32_t offsetInVector, offset_t numValues, const CompressionMetadata& metadata) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
        compressedWriter(frame, posInFrame,
            reinterpret_cast<const uint8_t*>(
                &vector->getValue<internalID_t>(offsetInVector).offset),
            0 /*dataOffset*/, numValues, metadata);
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

static write_values_func_t getWriteValuesFunc(const LogicalType& logicalType) {
    switch (logicalType.getLogicalTypeID()) {
    case LogicalTypeID::INTERNAL_ID:
        return WriteInternalIDValuesToPage();
    default:
        return WriteCompressedValuesToPage(logicalType);
    }
}

InternalIDColumn::InternalIDColumn(std::string name, FileHandle* dataFH, MemoryManager* mm,
    ShadowFile* shadowFile, bool enableCompression)
    : Column{std::move(name), LogicalType::INTERNAL_ID(), dataFH, mm, shadowFile, enableCompression,
          false /*requireNullColumn*/},
      commonTableID{INVALID_TABLE_ID} {}

void InternalIDColumn::populateCommonTableID(const ValueVector* resultVector) const {
    auto nodeIDs = reinterpret_cast<internalID_t*>(resultVector->getData());
    auto& selVector = resultVector->state->getSelVector();
    for (auto i = 0u; i < selVector.getSelSize(); i++) {
        const auto pos = selVector[i];
        nodeIDs[pos].tableID = commonTableID;
    }
}

Column::Column(std::string name, common::LogicalType dataType, FileHandle* dataFH,
    MemoryManager* mm, ShadowFile* shadowFile, bool enableCompression, bool requireNullColumn)
    : name{std::move(name)}, dbFileID{DBFileID::newDataFileID()}, dataType{std::move(dataType)},
      dataFH{dataFH}, mm{mm}, shadowFile(shadowFile), enableCompression{enableCompression},
      columnReadWriter(ColumnReadWriterFactory::createColumnReadWriter(
          this->dataType.getPhysicalType(), dbFileID, dataFH, shadowFile)) {
    readToVectorFunc = getReadValuesToVectorFunc(this->dataType);
    readToPageFunc = ReadCompressedValuesFromPage(this->dataType);
    writeFunc = getWriteValuesFunc(this->dataType);
    if (requireNullColumn) {
        auto columnName =
            StorageUtils::getColumnName(this->name, StorageUtils::ColumnType::NULL_MASK, "");
        nullColumn =
            std::make_unique<NullColumn>(columnName, dataFH, mm, shadowFile, enableCompression);
    }
}

Column::Column(std::string name, PhysicalTypeID physicalType, FileHandle* dataFH, MemoryManager* mm,
    ShadowFile* shadowFile, bool enableCompression, bool requireNullColumn)
    : Column(name, LogicalType::ANY(physicalType), dataFH, mm, shadowFile, enableCompression,
          requireNullColumn) {}

Column::~Column() = default;

Column* Column::getNullColumn() const {
    return nullColumn.get();
}

void Column::populateExtraChunkState(SegmentState& state) const {
    if (state.metadata.compMeta.compression == CompressionType::ALP) {
        Transaction& transaction = DUMMY_CHECKPOINT_TRANSACTION;
        if (dataType.getPhysicalType() == common::PhysicalTypeID::DOUBLE) {
            state.alpExceptionChunk = std::make_unique<InMemoryExceptionChunk<double>>(&transaction,
                state, dataFH, mm, shadowFile);
        } else if (dataType.getPhysicalType() == common::PhysicalTypeID::FLOAT) {
            state.alpExceptionChunk = std::make_unique<InMemoryExceptionChunk<float>>(&transaction,
                state, dataFH, mm, shadowFile);
        }
    }
}

std::unique_ptr<ColumnChunkData> Column::flushChunkData(const ColumnChunkData& chunkData,
    FileHandle& dataFH) {
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
    FileHandle& dataFH) {
    auto chunkMeta = flushData(chunkData, dataFH);
    auto flushedChunk = ColumnChunkFactory::createColumnChunkData(chunkData.getMemoryManager(),
        chunkData.getDataType().copy(), chunkData.isCompressionEnabled(), chunkMeta,
        chunkData.hasNullData(), true);
    if (chunkData.hasNullData()) {
        auto nullChunkMeta = flushData(*chunkData.getNullData(), dataFH);
        auto nullData = std::make_unique<NullChunkData>(chunkData.getMemoryManager(),
            chunkData.isCompressionEnabled(), nullChunkMeta);
        flushedChunk->setNullData(std::move(nullData));
    }
    return flushedChunk;
}

ColumnChunkMetadata Column::flushData(const ColumnChunkData& chunkData, FileHandle& dataFH) {
    KU_ASSERT(chunkData.sanityCheck());
    const auto preScanMetadata = chunkData.getMetadataToFlush();
    auto allocatedBlock = dataFH.getPageManager()->allocatePageRange(preScanMetadata.getNumPages());
    return chunkData.flushBuffer(&dataFH, allocatedBlock, preScanMetadata);
}

void Column::scan(const Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInChunk, offset_t length, ValueVector* resultVector,
    uint64_t offsetInVector) const {
    state.rangeSegments(startOffsetInChunk, length,
        [&](auto& segmentState, auto startOffsetInSegment, auto lengthInSegment, auto dstOffset) {
            scanSegment(transaction, segmentState, startOffsetInSegment, lengthInSegment,
                resultVector, offsetInVector + dstOffset);
        });
}

void Column::scanSegment(const Transaction* transaction, const SegmentState& state,
    offset_t startOffsetInSegment, row_idx_t numValuesToScan, ValueVector* resultVector,
    offset_t offsetInVector) const {
    if (nullColumn) {
        KU_ASSERT(state.nullState);
        nullColumn->scanInternal(transaction, *state.nullState, startOffsetInSegment,
            numValuesToScan, resultVector, offsetInVector);
    }
    scanInternal(transaction, state, startOffsetInSegment, numValuesToScan, resultVector,
        offsetInVector);
}

void Column::scanSegment(const Transaction* transaction, const SegmentState& state,
    ColumnChunkData* outputChunk, offset_t offsetInSegment, offset_t numValues) const {
    auto startLength = outputChunk->getNumValues();
    if (nullColumn) {
        nullColumn->scanSegment(transaction, *state.nullState, outputChunk->getNullData(),
            offsetInSegment, numValues);
    }

    if (startLength + numValues > outputChunk->getCapacity()) {
        outputChunk->resize(std::bit_ceil(startLength + numValues));
    }

    // FIXME(bmwinger): failing for certain null columns
    // KU_ASSERT((offsetInSegment + numValues) <= state.metadata.numValues);
    scanInternal(transaction, state, offsetInSegment, numValues, outputChunk);
    outputChunk->setNumValues(startLength + numValues);
}

void Column::scan(const Transaction* transaction, const ChunkState& state,
    ColumnChunkData* outputChunk, offset_t offsetInChunk, offset_t numValues) const {
    [[maybe_unused]] auto startLength = outputChunk->getNumValues();
    [[maybe_unused]] uint64_t numValuesScanned = state.rangeSegments(offsetInChunk, numValues,
        [&](auto& segmentState, auto startOffsetInSegment, auto lengthInSegment, auto) {
            scanSegment(transaction, segmentState, outputChunk, startOffsetInSegment,
                lengthInSegment);
        });
    KU_ASSERT(outputChunk->getNumValues() == startLength + numValuesScanned);
}

// FIXME: scanInternal functions can probably be removed in favour of scanSegment. Any chunk which
// skips this scan in its specialization should have a data size of 0, so they can override
// scanSegment instead and just call Column::scanSegment. That will reduce the chance of confusing
// the two when used.
void Column::scanInternal(const Transaction* transaction, const SegmentState& state,
    offset_t startOffsetInSegment, row_idx_t numValuesToScan, ColumnChunkData* resultChunk) const {
    if (getDataTypeSizeInChunk(dataType) > 0) {
        columnReadWriter->readCompressedValuesToPage(transaction, state, resultChunk->getData(),
            resultChunk->getNumValues(), startOffsetInSegment, numValuesToScan, readToPageFunc);
    }
}

void Column::scan(const Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInGroup, offset_t length, uint8_t* result) {
    state.rangeSegments(startOffsetInGroup, length,
        [&](auto& segmentState, auto startOffsetInSegment, auto lengthInSegment, auto dstOffset) {
            columnReadWriter->readCompressedValuesToPage(transaction, segmentState, result,
                dstOffset, startOffsetInSegment, lengthInSegment, readToPageFunc);
        });
}

void Column::scanSegment(const Transaction* transaction, const SegmentState& state,
    offset_t startOffsetInSegment, offset_t length, uint8_t* result) {
    columnReadWriter->readCompressedValuesToPage(transaction, state, result, 0,
        startOffsetInSegment, length, readToPageFunc);
}

void Column::scanInternal(const Transaction* transaction, const SegmentState& state,
    offset_t startOffsetInSegment, row_idx_t numValuesToScan, ValueVector* resultVector,
    offset_t offsetInVector) const {
    if (!resultVector->state || resultVector->state->getSelVector().isUnfiltered()) {
        columnReadWriter->readCompressedValuesToVector(transaction, state, resultVector,
            offsetInVector, startOffsetInSegment, numValuesToScan, readToVectorFunc);
    } else {
        struct Filterer {
            explicit Filterer(const SelectionVector& selVector)
                : selVector(selVector), posInSelVector(0) {}
            bool operator()(offset_t startIdx, offset_t endIdx) {
                while (posInSelVector < selVector.getSelSize() &&
                       selVector[posInSelVector] < startIdx) {
                    posInSelVector++;
                }
                return (posInSelVector < selVector.getSelSize() &&
                        isInRange(selVector[posInSelVector], startIdx, endIdx));
            }

            const SelectionVector& selVector;
            offset_t posInSelVector;
        };

        columnReadWriter->readCompressedValuesToVector(transaction, state, resultVector,
            offsetInVector, startOffsetInSegment, numValuesToScan, readToVectorFunc,
            Filterer{resultVector->state->getSelVector()});
    }
}

void Column::lookupValue(const Transaction* transaction, const ChunkState& state,
    offset_t nodeOffset, ValueVector* resultVector, uint32_t posInVector) const {
    offset_t offsetInSegment = 0;
    auto segmentState = state.findSegment(nodeOffset, offsetInSegment);
    if (nullColumn) {
        nullColumn->lookupInternal(transaction, *segmentState->nullState, nodeOffset, resultVector,
            posInVector);
    }
    if (!resultVector->isNull(posInVector)) {
        lookupInternal(transaction, *segmentState, nodeOffset, resultVector, posInVector);
    }
}

void Column::lookupInternal(const Transaction* transaction, const SegmentState& state,
    offset_t offsetInSegment, ValueVector* resultVector, uint32_t posInVector) const {
    columnReadWriter->readCompressedValueToVector(transaction, state, offsetInSegment, resultVector,
        posInVector, readToVectorFunc);
}

[[maybe_unused]] static bool sanityCheckForWrites(const ColumnChunkMetadata& metadata,
    const LogicalType& dataType) {
    if (metadata.compMeta.compression == CompressionType::ALP) {
        return metadata.compMeta.children.size() != 0;
    }
    if (metadata.compMeta.compression == CompressionType::CONSTANT) {
        return metadata.getNumDataPages(dataType.getPhysicalType()) == 0;
    }
    const auto numValuesPerPage = metadata.compMeta.numValues(KUZU_PAGE_SIZE, dataType);
    if (numValuesPerPage == UINT64_MAX) {
        return metadata.getNumDataPages(dataType.getPhysicalType()) == 0;
    }
    return std::ceil(
               static_cast<double>(metadata.numValues) / static_cast<double>(numValuesPerPage)) <=
           metadata.getNumDataPages(dataType.getPhysicalType());
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

// TODO(bmwinger): maybe this should be moved into ColumnChunkData; the only thing it seems to be
// using from Column is the ColumnReaderWriter And since ColumnChunk needs to call it for multiple
// segments, it might make more sense for it to be there instead of passing the Column in
// ColumnChunk::write
void Column::write(ColumnChunkData& persistentChunk, ChunkState& state, offset_t initialDstOffset,
    const ColumnChunkData& data, offset_t srcOffset, length_t numValues) const {
    state.rangeSegments(srcOffset, numValues,
        [&](auto& segmentState, auto offsetInSegment, auto lengthInSegment, auto dstOffset) {
            writeInternal(persistentChunk, segmentState, initialDstOffset + dstOffset, data,
                offsetInSegment, lengthInSegment);
        });
}

void Column::writeInternal(ColumnChunkData& persistentChunk, SegmentState& state,
    offset_t dstOffsetInSegment, const ColumnChunkData& data, offset_t srcOffset,
    offset_t numValues) const {
    auto nullMask = data.getNullMask();
    columnReadWriter->writeValuesToPageFromBuffer(state, dstOffsetInSegment, data.getData(),
        nullMask ? &*nullMask : nullptr, srcOffset, numValues, writeFunc);

    // TODO: this needs to be propagated to other WriteInternal functions
    // Or maybe split this function into writeSegment and writeSegmentInternal
    if (dataType.getPhysicalType() != common::PhysicalTypeID::ALP_EXCEPTION_DOUBLE &&
        dataType.getPhysicalType() != common::PhysicalTypeID::ALP_EXCEPTION_FLOAT) {
        auto nullMask = data.getNullMask();
        auto [minWritten, maxWritten] = getMinMaxStorageValue(data.getData(), srcOffset, numValues,
            dataType.getPhysicalType(), nullMask ? &*nullMask : nullptr);
        updateStatistics(persistentChunk.getMetadata(), dstOffsetInSegment + numValues - 1,
            minWritten, maxWritten);
    }
}

// TODO: Do we need to adapt the offsets to this current node group?
void Column::writeValues(ChunkState& state, offset_t initialDstOffset, const uint8_t* data,
    const NullMask* nullChunkData, offset_t srcOffset, offset_t numValues) const {
    state.rangeSegments(srcOffset, numValues,
        [&](auto& segmentState, auto offsetInSegment, auto lengthInSegment, auto dstOffset) {
            writeValuesInternal(segmentState, initialDstOffset + dstOffset, data, nullChunkData,
                offsetInSegment, lengthInSegment);
        });
}

void Column::writeValuesInternal(SegmentState& state, common::offset_t dstOffsetInSegment,
    const uint8_t* data, const common::NullMask* nullChunkData, common::offset_t srcOffset,
    common::offset_t numValues) const {
    columnReadWriter->writeValuesToPageFromBuffer(state, dstOffsetInSegment, data, nullChunkData,
        srcOffset, numValues, writeFunc);
}

// Append to the end of the chunk.
offset_t Column::appendValues(ColumnChunkData& persistentChunk, SegmentState& state,
    const uint8_t* data, const NullMask* nullChunkData, offset_t numValues) const {
    auto& metadata = persistentChunk.getMetadata();
    const auto startOffset = metadata.numValues;
    writeValuesInternal(state, metadata.numValues, data, nullChunkData, 0 /*dataOffset*/,
        numValues);

    auto [minWritten, maxWritten] = getMinMaxStorageValue(data, 0 /*offset*/, numValues,
        dataType.getPhysicalType(), nullChunkData);
    updateStatistics(metadata, startOffset + numValues - 1, minWritten, maxWritten);
    return startOffset;
}

bool Column::isEndOffsetOutOfPagesCapacity(const ColumnChunkMetadata& metadata,
    offset_t endOffset) const {
    if (metadata.compMeta.compression != CompressionType::CONSTANT &&
        (metadata.compMeta.numValues(KUZU_PAGE_SIZE, dataType) *
            metadata.getNumDataPages(dataType.getPhysicalType())) <= endOffset) {
        // Note that for constant compression, `metadata.numPages` will be equal to 0.
        // Thus, this function will always return true.
        return true;
    }
    return false;
}

void Column::checkpointColumnChunkInPlace(SegmentState& state,
    const ColumnCheckpointState& checkpointState) const {
    for (auto& segmentCheckpointState : checkpointState.segmentCheckpointStates) {
        KU_ASSERT(segmentCheckpointState.numRows > 0);
        state.column->writeInternal(checkpointState.persistentData, state,
            segmentCheckpointState.offsetInSegment, segmentCheckpointState.chunkData,
            segmentCheckpointState.startRowInData, segmentCheckpointState.numRows);
    }
    // FIXME(bmwinger): Why?
    checkpointState.persistentData.resetNumValuesFromMetadata();
    if (nullColumn) {
        checkpointNullData(checkpointState);
    }
}

void Column::checkpointNullData(const ColumnCheckpointState& checkpointState) const {
    std::vector<SegmentCheckpointState> nullSegmentCheckpointStates;
    for (const auto& segmentCheckpointState : checkpointState.segmentCheckpointStates) {
        KU_ASSERT(segmentCheckpointState.chunkData.hasNullData());
        nullSegmentCheckpointStates.emplace_back(*segmentCheckpointState.chunkData.getNullData(),
            segmentCheckpointState.startRowInData, segmentCheckpointState.offsetInSegment,
            segmentCheckpointState.numRows);
    }
    KU_ASSERT(checkpointState.persistentData.hasNullData());
    nullColumn->checkpointSegment(ColumnCheckpointState(
        *checkpointState.persistentData.getNullData(), std::move(nullSegmentCheckpointStates)));
}

void Column::checkpointColumnChunkOutOfPlace(const SegmentState& state,
    const ColumnCheckpointState& checkpointState) const {
    const auto numRows = std::max(checkpointState.endRowIdxToWrite, state.metadata.numValues);
    checkpointState.persistentData.setToInMemory();
    checkpointState.persistentData.resize(numRows);
    scanSegment(&DUMMY_CHECKPOINT_TRANSACTION, state, &checkpointState.persistentData, 0,
        state.metadata.numValues);
    state.reclaimAllocatedPages(*dataFH);
    for (auto& segmentCheckpointState : checkpointState.segmentCheckpointStates) {
        checkpointState.persistentData.write(&segmentCheckpointState.chunkData,
            segmentCheckpointState.startRowInData, segmentCheckpointState.offsetInSegment,
            segmentCheckpointState.numRows);
    }
    checkpointState.persistentData.finalize();
    checkpointState.persistentData.flush(*dataFH);
}

bool Column::canCheckpointInPlace(const SegmentState& state,
    const ColumnCheckpointState& checkpointState) const {
    if (isEndOffsetOutOfPagesCapacity(checkpointState.persistentData.getMetadata(),
            checkpointState.endRowIdxToWrite)) {
        return false;
    }
    if (checkpointState.persistentData.getMetadata().compMeta.canAlwaysUpdateInPlace()) {
        return true;
    }

    InPlaceUpdateLocalState localUpdateState{};
    for (auto& chunkCheckpointState : checkpointState.segmentCheckpointStates) {
        auto& chunkData = chunkCheckpointState.chunkData;
        if (chunkData.getNumValues() != 0 &&
            !state.metadata.compMeta.canUpdateInPlace(chunkData.getData(),
                chunkCheckpointState.startRowInData, chunkCheckpointState.numRows,
                dataType.getPhysicalType(), localUpdateState, chunkData.getNullMask())) {
            return false;
        }
    }
    return true;
}

void Column::checkpointSegment(ColumnCheckpointState&& checkpointState) const {
    SegmentState chunkState;
    checkpointState.persistentData.initializeScanState(chunkState, this);
    if (canCheckpointInPlace(chunkState, checkpointState)) {
        checkpointColumnChunkInPlace(chunkState, checkpointState);

        if (chunkState.metadata.compMeta.compression == CompressionType::ALP) {
            if (dataType.getPhysicalType() == common::PhysicalTypeID::DOUBLE) {
                chunkState.getExceptionChunk<double>()->finalizeAndFlushToDisk(chunkState);
            } else if (dataType.getPhysicalType() == common::PhysicalTypeID::FLOAT) {
                chunkState.getExceptionChunk<float>()->finalizeAndFlushToDisk(chunkState);
            } else {
                KU_UNREACHABLE;
            }
            checkpointState.persistentData.getMetadata().compMeta.floatMetadata()->exceptionCount =
                chunkState.metadata.compMeta.floatMetadata()->exceptionCount;
        }
    } else {
        checkpointColumnChunkOutOfPlace(chunkState, checkpointState);
    }
}

std::unique_ptr<Column> ColumnFactory::createColumn(std::string name, PhysicalTypeID physicalType,
    FileHandle* dataFH, MemoryManager* mm, ShadowFile* shadowFile, bool enableCompression) {
    return std::make_unique<Column>(name, LogicalType::ANY(physicalType), dataFH, mm, shadowFile,
        enableCompression);
}

std::unique_ptr<Column> ColumnFactory::createColumn(std::string name, LogicalType dataType,
    FileHandle* dataFH, MemoryManager* mm, ShadowFile* shadowFile, bool enableCompression) {
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
        return std::make_unique<Column>(name, std::move(dataType), dataFH, mm, shadowFile,
            enableCompression);
    }
    case PhysicalTypeID::INTERNAL_ID: {
        return std::make_unique<InternalIDColumn>(name, dataFH, mm, shadowFile, enableCompression);
    }
    case PhysicalTypeID::STRING: {
        return std::make_unique<StringColumn>(name, std::move(dataType), dataFH, mm, shadowFile,
            enableCompression);
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        return std::make_unique<ListColumn>(name, std::move(dataType), dataFH, mm, shadowFile,
            enableCompression);
    }
    case PhysicalTypeID::STRUCT: {
        return std::make_unique<StructColumn>(name, std::move(dataType), dataFH, mm, shadowFile,
            enableCompression);
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

} // namespace storage
} // namespace kuzu

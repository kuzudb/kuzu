#include "storage/store/column.h"

#include <algorithm>
#include <cstdint>
#include <memory>

#include "common/assert.h"
#include "common/null_mask.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "common/utils.h"
#include "expression_evaluator/expression_evaluator.h"
#include "storage/compression/compression.h"
#include "storage/compression/compression_float.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/storage_structure/disk_array_collection.h"
#include "storage/storage_utils.h"
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

        std::unique_ptr<offset_t[]> buffer = std::make_unique<offset_t[]>(numValuesToRead);
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

InternalIDColumn::InternalIDColumn(std::string name, const MetadataDAHInfo& metaDAHeaderInfo,
    BMFileHandle* dataFH, DiskArrayCollection& metadataDAC, BufferManager* bufferManager, WAL* wal,
    transaction::Transaction* transaction, bool enableCompression)
    : Column{name, LogicalType::INTERNAL_ID(), metaDAHeaderInfo, dataFH, metadataDAC, bufferManager,
          wal, transaction, enableCompression, false /*requireNullColumn*/},
      commonTableID{INVALID_TABLE_ID} {}

void InternalIDColumn::populateCommonTableID(const ValueVector* resultVector) const {
    auto nodeIDs = reinterpret_cast<internalID_t*>(resultVector->getData());
    auto& selVector = resultVector->state->getSelVector();
    for (auto i = 0u; i < selVector.getSelSize(); i++) {
        auto pos = selVector[i];
        nodeIDs[pos].tableID = commonTableID;
    }
}

Column::ChunkState& Column::ChunkState::getChildState(common::idx_t childIdx) {
    KU_ASSERT(childrenStates.size() > childIdx);
    return childrenStates[childIdx];
}

const Column::ChunkState& Column::ChunkState::getChildState(common::idx_t childIdx) const {
    KU_ASSERT(childrenStates.size() > childIdx);
    return childrenStates[childIdx];
}

Column::Column(std::string name, LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
    BMFileHandle* dataFH, DiskArrayCollection& metadataDAC, BufferManager* bufferManager, WAL* wal,
    transaction::Transaction* transaction, bool enableCompression, bool requireNullColumn)
    : name{std::move(name)}, dbFileID{DBFileID::newDataFileID()}, dataType{std::move(dataType)},
      dataFH{dataFH}, bufferManager{bufferManager}, wal{wal}, enableCompression{enableCompression} {
    metadataDA = metadataDAC.getDiskArray<ColumnChunkMetadata>(metaDAHeaderInfo.dataDAHIdx,
        dataType.getPhysicalType());
    readToVectorFunc = getReadValuesToVectorFunc(this->dataType);
    readToPageFunc = ReadCompressedValuesFromPage(this->dataType);
    batchLookupFunc = ReadCompressedValuesFromPage(this->dataType);
    writeFromVectorFunc = getWriteValueFromVectorFunc(this->dataType);
    writeFunc = getWriteValuesFunc(this->dataType);
    if (requireNullColumn) {
        auto columnName =
            StorageUtils::getColumnName(this->name, StorageUtils::ColumnType::NULL_MASK, "");
        nullColumn = std::make_unique<NullColumn>(columnName, metaDAHeaderInfo.nullDAHIdx, dataFH,
            metadataDAC, bufferManager, wal, transaction, enableCompression);
    }
}

Column::~Column() = default;

size_t Column::getNumValuesFromDisk(DiskArray<ColumnChunkMetadata>* metadataDA,
    Transaction* transaction, const ChunkState& state, offset_t startOffset, offset_t endOffset) {
    KU_ASSERT(nullptr != metadataDA);

    if (state.nodeGroupIdx >= metadataDA->getNumElements(transaction->getType())) {
        return 0;
    } else {
        auto chunkMetadata = metadataDA->get(state.nodeGroupIdx, transaction->getType());
        auto numValues = chunkMetadata.numValues == 0 ?
                             0 :
                             std::min(endOffset, chunkMetadata.numValues) - startOffset;
        return numValues;
    }
}

Column* Column::getNullColumn() const {
    return nullColumn.get();
}

template<typename T>
EncodeException<T> Column::getExceptionAt(size_t curExceptionIdx, Transaction* transaction,
    PageCursor exceptionPageCursor) {
    EncodeException<T> ret;

    PageCursor curExceptionCursor = PageUtils::getPageCursorForPos(curExceptionIdx,
        BufferPoolConstants::PAGE_4KB_SIZE / EncodeException<T>::sizeBytes());
    curExceptionCursor.pageIdx += exceptionPageCursor.pageIdx;

    readFromPage(transaction, curExceptionCursor.pageIdx,
        [&ret, &curExceptionCursor](uint8_t* frame) -> void {
            memcpy(&ret.value,
                frame + EncodeException<T>::sizeBytes() * curExceptionCursor.elemPosInPage,
                sizeof(ret.value));
            memcpy(&ret.posInPage,
                frame + EncodeException<T>::sizeBytes() * curExceptionCursor.elemPosInPage +
                    sizeof(ret.value),
                sizeof(ret.posInPage));
        });

    return ret;
}

template<typename T>
offset_t Column::findFirstExceptionAtOrPastOffset(Transaction* transaction, offset_t offsetInChunk,
    offset_t exceptionCount, PageCursor exceptionPageCursor) {
    // binary search for chunkOffset in exceptions
    offset_t lo = 0;
    offset_t hi = exceptionCount;
    while (lo < hi) {
        const size_t curExceptionIdx = (lo + hi) / 2;
        EncodeException<T> lastException =
            getExceptionAt<T>(curExceptionIdx, transaction, exceptionPageCursor);

        if (lastException.posInPage < offsetInChunk) {
            lo = curExceptionIdx + 1;
        } else {
            hi = curExceptionIdx;
        }
    }

    return lo;
}

PageCursor Column::getExceptionPageCursor(const ChunkState& readState, PageCursor cursor) {
    // TODO fix
    const size_t numExceptionPages =
        ceilDiv((uint64_t)readState.metadata.compMeta.alpMetadata.exceptionCount,
            BufferPoolConstants::PAGE_4KB_SIZE /
                (dataType.getPhysicalType() == common::PhysicalTypeID::FLOAT ?
                        EncodeException<float>::sizeBytes() :
                        EncodeException<double>::sizeBytes()));
    const size_t exceptionPageOffset = readState.metadata.numPages - numExceptionPages;
    KU_ASSERT(exceptionPageOffset == (page_idx_t)exceptionPageOffset);
    return {cursor.pageIdx + (page_idx_t)exceptionPageOffset, 0};
}

template<typename T, typename OutputType>
void Column::readFloatValue(Transaction* transaction, const ChunkState& readState,
    offset_t offsetInChunk, PageCursor cursor, OutputType result, uint32_t offsetInResult,
    std::function<void(uint8_t* frame, PageCursor& pageCursor, OutputType result,
        uint32_t posInResult, uint64_t numValues, const CompressionMetadata& metadata)>
        readFunc) {
    PageCursor exceptionPageCursor =
        getExceptionPageCursor(readState, getPageCursorForOffsetInGroup(0, readState));

    const offset_t firstExceptionIdx = findFirstExceptionAtOrPastOffset<T>(transaction,
        offsetInChunk, readState.metadata.compMeta.alpMetadata.exceptionCount, exceptionPageCursor);

    const auto searchedException =
        getExceptionAt<T>(firstExceptionIdx, transaction, exceptionPageCursor);
    if (firstExceptionIdx == readState.metadata.compMeta.alpMetadata.exceptionCount ||
        searchedException.posInPage != offsetInChunk) {
        readFromPage(transaction, cursor.pageIdx,
            [&readFunc, &cursor, result, offsetInResult, &readState](uint8_t* frame) -> void {
                readFunc(frame, cursor, result, offsetInResult, 1 /* numValuesToRead */,
                    readState.metadata.compMeta);
            });
    } else {
        if constexpr (std::is_same_v<uint8_t*, OutputType>) {
            *reinterpret_cast<T*>(result) = searchedException.value;
        } else {
            *reinterpret_cast<T*>(result->getData()) = searchedException.value;
        }
    }
}

template<typename OutputType>
void Column::readValue(Transaction* transaction, const ChunkState& readState, offset_t nodeOffset,
    OutputType result, uint32_t offsetInResult,
    std::function<void(uint8_t* frame, PageCursor& pageCursor, OutputType result,
        uint32_t posInResult, uint64_t numValues, const CompressionMetadata& metadata)>
        readFunc) {
    auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
    auto cursor = getPageCursorForOffsetInGroup(offsetInChunk, readState);
    KU_ASSERT(isPageIdxValid(cursor.pageIdx, readState.metadata));
    if (readState.metadata.compMeta.compression == CompressionType::FLOAT) {
        switch (dataType.getPhysicalType()) {
            // TODO deal with enableCompression
        case common::PhysicalTypeID::FLOAT: {
            readFloatValue<float>(transaction, readState, offsetInChunk, cursor, result,
                offsetInResult, readFunc);
            return;
        }
        case common::PhysicalTypeID::DOUBLE: {
            readFloatValue<double>(transaction, readState, offsetInChunk, cursor, result,
                offsetInResult, readFunc);
            return;
        }
        default: {
            KU_UNREACHABLE;
        }
        }
    }

    readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
        readFunc(frame, cursor, result, offsetInResult, 1 /* numValuesToRead */,
            readState.metadata.compMeta);
    });
}

template<typename T, typename OutputType>
void Column::patchFloatExceptions(Transaction* transaction, const ChunkState& readState,
    offset_t startOffsetInChunk, size_t numValuesToScan, OutputType result,
    offset_t startOffsetInResult,
    std::function<bool(common::offset_t, common::offset_t)> filterFunc) {
    // patch exceptions
    PageCursor exceptionPageCursor =
        getExceptionPageCursor(readState, getPageCursorForOffsetInGroup(0, readState));
    offset_t curExceptionIdx = findFirstExceptionAtOrPastOffset<T>(transaction, startOffsetInChunk,
        readState.metadata.compMeta.alpMetadata.exceptionCount, exceptionPageCursor);
    for (; curExceptionIdx < readState.metadata.compMeta.alpMetadata.exceptionCount;
         ++curExceptionIdx) {
        const auto curException =
            getExceptionAt<T>(curExceptionIdx, transaction, exceptionPageCursor);
        KU_ASSERT(curException.posInPage >= curExceptionIdx);
        if (curException.posInPage >= startOffsetInChunk + numValuesToScan) {
            break;
        }
        const offset_t offsetInResult =
            startOffsetInResult + curException.posInPage - startOffsetInChunk;
        if (filterFunc(offsetInResult, offsetInResult + 1)) {
            if constexpr (std::is_same_v<uint8_t*, OutputType>) {
                reinterpret_cast<T*>(result)[offsetInResult] = curException.value;
            } else {
                static_assert(std::is_same_v<ValueVector*, std::remove_cvref_t<OutputType>>);
                reinterpret_cast<T*>(result->getData())[offsetInResult] = curException.value;
            }
        }
    }
}

template<typename OutputType>
uint64_t Column::readValues(Transaction* transaction, const ChunkState& readState,
    OutputType result, uint32_t startOffsetInResult, uint64_t startNodeOffset,
    uint64_t endNodeOffset,
    std::function<void(uint8_t* frame, PageCursor& pageCursor, OutputType result,
        uint32_t posInResult, uint64_t numValues, const CompressionMetadata& metadata)>
        readFunc,
    std::function<bool(common::offset_t, common::offset_t)> filterFunc) {
    const ColumnChunkMetadata& chunkMeta = readState.metadata;
    const auto numValuesPerPage = readState.numValuesPerPage;
    const auto numValuesToScan = endNodeOffset - startNodeOffset;
    if (numValuesToScan == 0) {
        return 0;
    }

    auto pageCursor = getPageCursorForOffsetInGroup(startNodeOffset, readState);
    KU_ASSERT(isPageIdxValid(pageCursor.pageIdx, readState.metadata));

    uint64_t numValuesScanned = 0;
    while (numValuesScanned < numValuesToScan) {
        uint64_t numValuesToScanInPage = std::min(numValuesPerPage - pageCursor.elemPosInPage,
            numValuesToScan - numValuesScanned);
        KU_ASSERT(isPageIdxValid(pageCursor.pageIdx, chunkMeta));
        if (filterFunc(numValuesScanned, numValuesScanned + numValuesToScanInPage)) {
            readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
                readFunc(frame, pageCursor, result, numValuesScanned + startOffsetInResult,
                    numValuesToScanInPage, chunkMeta.compMeta);
            });
        }
        numValuesScanned += numValuesToScanInPage;
        pageCursor.nextPage();
    }

    if (readState.metadata.compMeta.compression == CompressionType::FLOAT) {
        switch (dataType.getPhysicalType()) {
            // TODO deal with enableCompression
        case common::PhysicalTypeID::FLOAT: {
            patchFloatExceptions<float>(transaction, readState, startNodeOffset, numValuesToScan,
                result, startOffsetInResult, filterFunc);
            break;
        }
        case common::PhysicalTypeID::DOUBLE: {
            patchFloatExceptions<double>(transaction, readState, startNodeOffset, numValuesToScan,
                result, startOffsetInResult, filterFunc);
            break;
        }
        default: {
            KU_UNREACHABLE;
        }
        }
    }

    return numValuesScanned;
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
        state.metadata = metadataDA->get(nodeGroupIdx, transaction->getType());
        readValue(transaction, state, nodeOffset, result, i, batchLookupFunc);
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
    if (readState.nodeGroupIdx != nodeGroupIdx) {
        // Only update metadata and numValuesPerPage if we're reading a different node group.
        // This is an optimization to reduce accesses to metadataDA, which can lead to
        // contention due to lock acquiring in DiskArray.
        readState.nodeGroupIdx = nodeGroupIdx;
        if (nodeGroupIdx < metadataDA->getNumElements(transaction->getType())) {
            readState.metadata = metadataDA->get(nodeGroupIdx, transaction->getType());
            readState.numValuesPerPage =
                readState.metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
        }
    }
}

void Column::scan(Transaction* transaction, const ChunkState& state, idx_t vectorIdx,
    row_idx_t numValuesToScan, ValueVector* nodeIDVector, ValueVector* resultVector) {
    if (nullColumn) {
        KU_ASSERT(state.nullState);
        nullColumn->scan(transaction, *state.nullState, vectorIdx, numValuesToScan, nodeIDVector,
            resultVector);
    }
    scanInternal(transaction, state, vectorIdx, numValuesToScan, nodeIDVector, resultVector);
}

void Column::scan(Transaction* transaction, const ChunkState& state, offset_t startOffsetInGroup,
    offset_t endOffsetInGroup, ValueVector* resultVector, uint64_t offsetInVector) {
    if (nullColumn) {
        KU_ASSERT(state.nullState);
        nullColumn->scan(transaction, *state.nullState, startOffsetInGroup, endOffsetInGroup,
            resultVector, offsetInVector);
    }
    readValues(transaction, state, resultVector, offsetInVector, startOffsetInGroup,
        endOffsetInGroup, readToVectorFunc,
        [](offset_t /*startIdx*/, offset_t /*endIdx*/) { return true; });
}

void Column::scan(Transaction* transaction, const ChunkState& state, ColumnChunkData* columnChunk,
    offset_t startOffset, offset_t endOffset) {
    if (nullColumn) {
        nullColumn->scan(transaction, *state.nullState, columnChunk->getNullChunk(), startOffset,
            endOffset);
    }

    startOffset = std::min(startOffset, state.metadata.numValues);
    endOffset = std::min(endOffset, state.metadata.numValues);
    KU_ASSERT(endOffset >= startOffset);
    const auto numValuesToScan = endOffset - startOffset;
    if (getDataTypeSizeInChunk(dataType) == 0) {
        columnChunk->setNumValues(numValuesToScan);
        return;
    }

    if (numValuesToScan > columnChunk->getCapacity()) {
        columnChunk->resize(std::bit_ceil(numValuesToScan));
    }
    KU_ASSERT((numValuesToScan + startOffset) <= state.metadata.numValues);

    const uint64_t numValuesScanned = readValues(transaction, state, columnChunk->getData(), 0,
        startOffset, endOffset, readToPageFunc, [](offset_t, offset_t) { return true; });

    columnChunk->setNumValues(numValuesScanned);
}

void Column::scan(Transaction* transaction, const ChunkState& state, offset_t startOffsetInGroup,
    offset_t endOffsetInGroup, uint8_t* result) {
    readValues(transaction, state, result, 0, startOffsetInGroup, endOffsetInGroup, readToPageFunc,
        [](offset_t /*startIdx*/, offset_t /*endIdx*/) { return true; });
}

void Column::scanInternal(Transaction* transaction, const ChunkState& state, idx_t vectorIdx,
    row_idx_t numValuesToScan, ValueVector* nodeIDVector, ValueVector* resultVector) {
    const auto startOffsetInChunk = vectorIdx * DEFAULT_VECTOR_CAPACITY;
    if (nodeIDVector->state->getSelVector().isUnfiltered()) {
        readValues(transaction, state, resultVector, 0, startOffsetInChunk,
            startOffsetInChunk + numValuesToScan, readToVectorFunc,
            [](offset_t /*startIdx*/, offset_t /*endIdx*/) { return true; });
    } else {
        struct Filterer {
            explicit Filterer(const SelectionVector& selVector)
                : selVector(selVector), posInSelVector(0) {}
            bool operator()(offset_t startIdx, offset_t endIdx) {
                bool ret = isInRange(selVector[posInSelVector], startIdx, endIdx);
                while (
                    posInSelVector < selVector.getSelSize() && selVector[posInSelVector] < endIdx) {
                    posInSelVector++;
                }
                return ret;
            }

            const SelectionVector& selVector;
            offset_t posInSelVector;
        };

        readValues(transaction, state, resultVector, 0, startOffsetInChunk,
            startOffsetInChunk + numValuesToScan, readToVectorFunc,
            Filterer{nodeIDVector->state->getSelVector()});
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

void Column::lookupInternal(Transaction* transaction, ChunkState& readState,
    ValueVector* nodeIDVector, ValueVector* resultVector) {
    auto& selVector = nodeIDVector->state->getSelVector();
    for (auto i = 0ul; i < selVector.getSelSize(); i++) {
        auto pos = selVector[i];
        if (nodeIDVector->isNull(pos) && resultVector->isNull(pos)) {
            continue;
        }
        auto nodeOffset = nodeIDVector->readNodeOffset(pos);
        lookupValue(transaction, readState, nodeOffset, resultVector, pos);
    }
}

void Column::lookupValue(Transaction* transaction, ChunkState& readState, offset_t nodeOffset,
    ValueVector* resultVector, uint32_t posInVector) {
    readValue(transaction, readState, nodeOffset, resultVector, posInVector, readToVectorFunc);
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
    if (metadata.compMeta.compression == CompressionType::FLOAT) {
        if (metadata.compMeta.children.size() == 0) {
            return false;
        }
        // TODO add check for bitwidth 0
        return true;
    }
    if (metadata.compMeta.compression == CompressionType::CONSTANT) {
        return metadata.numPages == 0;
    }
    auto numValuesPerPage =
        metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
    if (numValuesPerPage == UINT64_MAX) {
        return metadata.numPages == 0;
    }
    return std::ceil(static_cast<double>(metadata.numValues) /
                     static_cast<double>(numValuesPerPage)) <= metadata.numPages;
}

void Column::append(ColumnChunkData* columnChunk, ChunkState& state) {
    KU_ASSERT(enableCompression == columnChunk->isCompressionEnabled() &&
              state.nodeGroupIdx != INVALID_NODE_GROUP_IDX);
    KU_ASSERT(columnChunk->sanityCheck());
    // Main column chunk.
    auto preScanMetadata = columnChunk->getMetadataToFlush();
    auto startPageIdx = dataFH->addNewPages(preScanMetadata.numPages);
    state.metadata = columnChunk->flushBuffer(dataFH, startPageIdx, preScanMetadata);
    KU_ASSERT(sanityCheckForWrites(state.metadata, dataType));
    metadataDA->resize(state.nodeGroupIdx + 1);
    metadataDA->update(state.nodeGroupIdx, state.metadata);
    if (nullColumn) {
        // Null column chunk.
        KU_ASSERT(state.nullState);
        nullColumn->append(columnChunk->getNullChunk(), *state.nullState);
    }
}

void Column::write(ChunkState& state, offset_t offsetInChunk, ValueVector* vectorToWriteFrom,
    uint32_t posInVectorToWriteFrom) {
    bool isNull = vectorToWriteFrom->isNull(posInVectorToWriteFrom);
    if (!isNull) {
        writeValue(state, offsetInChunk, vectorToWriteFrom, posInVectorToWriteFrom);
    }
    auto valueToWrite = StorageValue::readFromVector(*vectorToWriteFrom, posInVectorToWriteFrom);
    updateStatistics(state.metadata, offsetInChunk, valueToWrite, valueToWrite);
}

void Column::updateStatistics(ColumnChunkMetadata& metadata, offset_t maxIndex,
    const std::optional<StorageValue>& min, const std::optional<StorageValue>& max) {
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

void Column::writeValue(ChunkState& state, offset_t offsetInChunk, ValueVector* vectorToWriteFrom,
    uint32_t posInVectorToWriteFrom) {
    updatePageWithCursor(getPageCursorForOffsetInGroup(offsetInChunk, state),
        [&](auto frame, auto posInPage) {
            writeFromVectorFunc(frame, posInPage, vectorToWriteFrom, posInVectorToWriteFrom,
                state.metadata.compMeta);
        });
}

void Column::write(ChunkState& state, offset_t offsetInChunk, ColumnChunkData* data,
    offset_t dataOffset, length_t numValues) {
    std::optional<NullMask> nullMask = data->getNullMask();
    NullMask* nullMaskPtr = nullptr;
    if (nullMask) {
        nullMaskPtr = &*nullMask;
    }
    writeValues(state, offsetInChunk, data->getData(), nullMaskPtr, dataOffset, numValues);

    auto [minWritten, maxWritten] = getMinMaxStorageValue(data->getData(), dataOffset, numValues,
        dataType.getPhysicalType(), nullMaskPtr);
    updateStatistics(state.metadata, offsetInChunk + numValues - 1, minWritten, maxWritten);
}

void Column::writeValues(ChunkState& state, offset_t dstOffset, const uint8_t* data,
    const NullMask* nullChunkData, offset_t srcOffset, offset_t numValues) {
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
offset_t Column::appendValues(ChunkState& state, const uint8_t* data, const NullMask* nullChunkData,
    offset_t numValues) {
    auto startOffset = state.metadata.numValues;
    auto numPages = dataFH->getNumPages();
    // TODO: writeValues should return new pages appended if any.
    writeValues(state, state.metadata.numValues, data, nullChunkData, 0 /*dataOffset*/, numValues);
    auto newNumPages = dataFH->getNumPages();
    state.metadata.numPages += (newNumPages - numPages);

    auto [minWritten, maxWritten] = getMinMaxStorageValue(data, 0 /*offset*/, numValues,
        dataType.getPhysicalType(), nullChunkData);
    updateStatistics(state.metadata, startOffset + numValues - 1, minWritten, maxWritten);
    // TODO(bmwinger): it shouldn't be necessary to do this here; it should be handled in
    // prepareCommit
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
    }
    if (cursor.pageIdx >= dataFH->getNumPages()) {
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

void Column::prepareCommitForChunk(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    bool isNewNodeGroup, const ChunkCollection& localInsertChunks,
    const offset_to_row_idx_t& insertInfo, const ChunkCollection& localUpdateChunks,
    const offset_to_row_idx_t& updateInfo, const offset_set_t& deleteInfo) {
    ChunkState state;
    initChunkState(transaction, nodeGroupIdx, state);
    if (isNewNodeGroup) {
        // If this is a new node group, updateInfo should be empty. We should perform out-of-place
        // commit with a new column chunk.
        commitLocalChunkOutOfPlace(transaction, state, isNewNodeGroup, localInsertChunks,
            insertInfo, localUpdateChunks, updateInfo, deleteInfo);
    } else {
        prepareCommitForExistingChunk(transaction, state, localInsertChunks, insertInfo,
            localUpdateChunks, updateInfo, deleteInfo);
    }
}

void Column::prepareCommitForChunk(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    bool isNewNodeGroup, const std::vector<offset_t>& dstOffsets, ColumnChunkData* chunk,
    offset_t startSrcOffset) {
    metadataDA->prepareCommit();
    ChunkState state;
    initChunkState(transaction, nodeGroupIdx, state);
    if (isNewNodeGroup) {
        commitColumnChunkOutOfPlace(transaction, state, isNewNodeGroup, dstOffsets, chunk,
            startSrcOffset);
    } else {
        // If this is not a new node group, we should first check if we can perform in-place commit.
        prepareCommitForExistingChunk(transaction, state, dstOffsets, chunk, startSrcOffset);
    }
}

void Column::prepareCommitForExistingChunk(Transaction* transaction, ChunkState& state,
    const ChunkCollection& localInsertChunks, const offset_to_row_idx_t& insertInfo,
    const ChunkCollection& localUpdateChunks, const offset_to_row_idx_t& updateInfo,
    const offset_set_t& deleteInfo) {
    // If this is not a new node group, we should first check if we can perform in-place commit.
    if (canCommitInPlace(state, localInsertChunks, insertInfo, localUpdateChunks, updateInfo)) {
        commitLocalChunkInPlace(state, localInsertChunks, insertInfo, localUpdateChunks, updateInfo,
            deleteInfo);
        KU_ASSERT(sanityCheckForWrites(state.metadata, dataType));
        // TODO(bmwinger): avoid updating metadata if it has not changed
        metadataDA->update(state.nodeGroupIdx, state.metadata);
        if (nullColumn) {
            KU_ASSERT(state.nullState);
            auto nullInsertChunks = getNullChunkCollection(localInsertChunks);
            auto nullUpdateChunks = getNullChunkCollection(localUpdateChunks);
            nullColumn->prepareCommitForExistingChunk(transaction, *state.nullState,
                nullInsertChunks, insertInfo, nullUpdateChunks, updateInfo, deleteInfo);
        }
    } else {
        commitLocalChunkOutOfPlace(transaction, state, false /*isNewNodeGroup*/, localInsertChunks,
            insertInfo, localUpdateChunks, updateInfo, deleteInfo);
    }
}

void Column::prepareCommitForExistingChunk(Transaction* transaction, ChunkState& state,
    const std::vector<offset_t>& dstOffsets, ColumnChunkData* chunk, offset_t startSrcOffset) {
    if (canCommitInPlace(state, dstOffsets, chunk, startSrcOffset)) {
        prepareCommitForExistingChunkInPlace(transaction, state, dstOffsets, chunk, startSrcOffset);
    } else {
        commitColumnChunkOutOfPlace(transaction, state, false /*isNewNodeGroup*/, dstOffsets, chunk,
            startSrcOffset);
    }
}

void Column::prepareCommitForExistingChunkInPlace(Transaction* transaction, ChunkState& state,
    const std::vector<offset_t>& dstOffsets, ColumnChunkData* chunk, offset_t startSrcOffset) {
    commitColumnChunkInPlace(state, dstOffsets, chunk, startSrcOffset);
    KU_ASSERT(sanityCheckForWrites(state.metadata, dataType));
    metadataDA->update(state.nodeGroupIdx, state.metadata);
    if (nullColumn) {
        nullColumn->prepareCommitForExistingChunk(transaction, *state.nullState, dstOffsets,
            chunk->getNullChunk(), startSrcOffset);
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
                offsetInLocalChunk, 1 /*numValues*/, dataType.getPhysicalType())) {
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

bool Column::canCommitInPlace(const ChunkState& state, const std::vector<offset_t>& dstOffsets,
    ColumnChunkData* chunk, offset_t srcOffset) {
    auto maxDstOffset = getMaxOffset(dstOffsets);
    if (isMaxOffsetOutOfPagesCapacity(state.metadata, maxDstOffset)) {
        return false;
    }
    if (state.metadata.compMeta.canAlwaysUpdateInPlace()) {
        return true;
    }
    if (!dstOffsets.empty() &&
        !state.metadata.compMeta.canUpdateInPlace(chunk->getData(), srcOffset, dstOffsets.size(),
            dataType.getPhysicalType(), chunk->getNullMask())) {
        return false;
    }
    return true;
}

void Column::commitLocalChunkInPlace(ChunkState& state, const ChunkCollection& localInsertChunks,
    const offset_to_row_idx_t& insertInfo, const ChunkCollection& localUpdateChunks,
    const offset_to_row_idx_t& updateInfo, const offset_set_t&) {
    applyLocalChunkToColumn(state, localUpdateChunks, updateInfo);
    applyLocalChunkToColumn(state, localInsertChunks, insertInfo);
}

std::unique_ptr<ColumnChunkData> Column::getEmptyChunkForCommit(uint64_t capacity) {
    return ColumnChunkFactory::createColumnChunkData(dataType.copy(), enableCompression, capacity,
        true /*inMemory*/, nullColumn != nullptr /*hasNull*/);
}

// TODO: Pass state in to avoid access metadata.
void Column::commitLocalChunkOutOfPlace(Transaction* transaction, ChunkState& state,
    bool isNewNodeGroup, const ChunkCollection& localInsertChunks,
    const offset_to_row_idx_t& insertInfo, const ChunkCollection& localUpdateChunks,
    const offset_to_row_idx_t& updateInfo, const offset_set_t& deleteInfo) {
    std::unique_ptr<ColumnChunkData> columnChunk;
    if (isNewNodeGroup) {
        KU_ASSERT(updateInfo.empty() && deleteInfo.empty());
        columnChunk = getEmptyChunkForCommit(getMaxOffset(insertInfo) + 1);
        // Apply inserts from the local chunk.
        applyLocalChunkToColumnChunk(localInsertChunks, columnChunk.get(), insertInfo);
    } else {
        auto maxNodeOffset = std::max(getMaxOffset(insertInfo), getMaxOffset(updateInfo));
        auto chunkMeta = getMetadata(state.nodeGroupIdx, transaction->getType());
        maxNodeOffset = std::max(maxNodeOffset, chunkMeta.numValues);
        columnChunk = getEmptyChunkForCommit(maxNodeOffset + 1);
        // First, scan the whole column chunk from persistent storage.
        scan(transaction, state, columnChunk.get());
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
    append(columnChunk.get(), state);
}

void Column::commitColumnChunkInPlace(ChunkState& state, const std::vector<offset_t>& dstOffsets,
    ColumnChunkData* chunk, offset_t srcOffset) {
    // TODO: Should always sort dstOffsets, and group writes to the same page.
    for (auto i = 0u; i < dstOffsets.size(); i++) {
        write(state, dstOffsets[i], chunk, srcOffset + i, 1 /* numValues */);
    }
}

void Column::commitColumnChunkOutOfPlace(Transaction* transaction, ChunkState& state,
    bool isNewNodeGroup, const std::vector<offset_t>& dstOffsets, ColumnChunkData* chunk,
    offset_t srcOffset) {
    if (isNewNodeGroup) {
        chunk->finalize();
        append(chunk, state);
    } else {
        auto columnChunk = getEmptyChunkForCommit(
            std::max(std::bit_ceil(state.metadata.numValues), getMaxOffset(dstOffsets) + 1));
        scan(transaction, state, columnChunk.get());
        for (auto i = 0u; i < dstOffsets.size(); i++) {
            columnChunk->write(chunk, srcOffset + i, dstOffsets[i], 1 /* numValues */);
        }
        columnChunk->finalize();
        append(columnChunk.get(), state);
    }
}

void Column::applyLocalChunkToColumnChunk(const ChunkCollection& localChunks,
    ColumnChunkData* columnChunk, const offset_to_row_idx_t& updateInfo) {
    for (auto& [offsetInDstChunk, rowIdx] : updateInfo) {
        auto [chunkIdx, offsetInLocalChunk] =
            LocalChunkedGroupCollection::getChunkIdxAndOffsetInChunk(rowIdx);
        columnChunk->write(localChunks[chunkIdx], offsetInLocalChunk, offsetInDstChunk,
            1 /* numValues */);
    }
}

void Column::updateStateMetadataNumValues(ChunkState& state, size_t numValues) {
    state.metadata.numValues = numValues;
}

void Column::applyLocalChunkToColumn(ChunkState& state, const ChunkCollection& localChunks,
    const offset_to_row_idx_t& updateInfo) {
    for (auto& [offsetInDstChunk, rowIdx] : updateInfo) {
        auto [chunkIdx, offsetInLocalChunk] =
            LocalChunkedGroupCollection::getChunkIdxAndOffsetInChunk(rowIdx);
        if (!localChunks[chunkIdx]->isNull(offsetInLocalChunk)) {
            write(state, offsetInDstChunk, localChunks[chunkIdx], offsetInLocalChunk,
                1 /*numValues*/);
        } else {
            if (offsetInDstChunk >= state.metadata.numValues) {
                updateStateMetadataNumValues(state, offsetInDstChunk + 1);
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
    DiskArray<ColumnChunkMetadata>* metadataDA_, ExpressionEvaluator& defaultEvaluator) {
    KU_ASSERT(metadataDA_ != nullptr);
    auto numNodeGroups = metadataDA_->getNumElements(transaction->getType());
    ChunkState state;
    for (auto nodeGroupIdx = 0u; nodeGroupIdx < numNodeGroups; nodeGroupIdx++) {
        initChunkState(transaction, nodeGroupIdx, state);
        auto chunkMeta = metadataDA_->get(nodeGroupIdx, transaction->getType());
        auto capacity = StorageConstants::NODE_GROUP_SIZE;
        while (capacity < chunkMeta.numValues) {
            capacity *= CHUNK_RESIZE_RATIO;
        }
        auto columnChunk =
            ColumnChunkFactory::createColumnChunkData(dataType.copy(), enableCompression, capacity);
        columnChunk->populateWithDefaultVal(defaultEvaluator, chunkMeta.numValues);
        append(columnChunk.get(), state);
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
    const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, DiskArrayCollection& metadataDAC,
    BufferManager* bufferManager, WAL* wal, Transaction* transaction, bool enableCompression) {
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
        return std::make_unique<Column>(name, std::move(dataType), metaDAHeaderInfo, dataFH,
            metadataDAC, bufferManager, wal, transaction, enableCompression);
    }
    case PhysicalTypeID::INTERNAL_ID: {
        return std::make_unique<InternalIDColumn>(name, metaDAHeaderInfo, dataFH, metadataDAC,
            bufferManager, wal, transaction, enableCompression);
    }
    case PhysicalTypeID::STRING: {
        return std::make_unique<StringColumn>(name, std::move(dataType), metaDAHeaderInfo, dataFH,
            metadataDAC, bufferManager, wal, transaction, enableCompression);
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        return std::make_unique<ListColumn>(name, std::move(dataType), metaDAHeaderInfo, dataFH,
            metadataDAC, bufferManager, wal, transaction, enableCompression);
    }
    case PhysicalTypeID::STRUCT: {
        return std::make_unique<StructColumn>(name, std::move(dataType), metaDAHeaderInfo, dataFH,
            metadataDAC, bufferManager, wal, transaction, enableCompression);
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

} // namespace storage
} // namespace kuzu

#include "storage/store/string_column.h"

#include <algorithm>

#include "common/cast.h"
#include "common/null_mask.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/compression/compression.h"
#include "storage/storage_utils.h"
#include "storage/store/column.h"
#include "storage/store/column_chunk.h"
#include "storage/store/column_chunk_data.h"
#include "storage/store/null_column.h"
#include "storage/store/string_chunk_data.h"
#include "transaction/transaction.h"

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

using string_index_t = DictionaryChunk::string_index_t;
using string_offset_t = DictionaryChunk::string_offset_t;

StringColumn::StringColumn(std::string name, common::LogicalType dataType, FileHandle* dataFH,
    MemoryManager* mm, ShadowFile* shadowFile, bool enableCompression)
    : Column{std::move(name), std::move(dataType), dataFH, mm, shadowFile, enableCompression,
          true /* requireNullColumn */},
      dictionary{this->name, dataFH, mm, shadowFile, enableCompression} {
    auto indexColumnName =
        StorageUtils::getColumnName(this->name, StorageUtils::ColumnType::INDEX, "index");
    indexColumn = std::make_unique<Column>(indexColumnName, LogicalType::UINT32(), dataFH, mm,
        shadowFile, enableCompression, false /*requireNullColumn*/);
}

SegmentState& StringColumn::getChildState(SegmentState& state, ChildStateIndex child) {
    const auto childIdx = static_cast<idx_t>(child);
    return state.getChildState(childIdx);
}

const SegmentState& StringColumn::getChildState(const SegmentState& state, ChildStateIndex child) {
    const auto childIdx = static_cast<idx_t>(child);
    return state.getChildState(childIdx);
}

std::unique_ptr<ColumnChunkData> StringColumn::flushChunkData(const ColumnChunkData& chunkData,
    FileHandle& dataFH) {
    auto flushedChunkData = flushNonNestedChunkData(chunkData, dataFH);
    auto& flushedStringData = flushedChunkData->cast<StringChunkData>();

    auto& stringChunk = chunkData.cast<StringChunkData>();
    flushedStringData.setIndexChunk(
        Column::flushChunkData(*stringChunk.getIndexColumnChunk(), dataFH));
    auto& dictChunk = stringChunk.getDictionaryChunk();
    flushedStringData.getDictionaryChunk().setOffsetChunk(
        Column::flushChunkData(*dictChunk.getOffsetChunk(), dataFH));
    flushedStringData.getDictionaryChunk().setStringDataChunk(
        Column::flushChunkData(*dictChunk.getStringDataChunk(), dataFH));
    return flushedChunkData;
}

void StringColumn::lookupInternal(const Transaction* transaction, const SegmentState& state,
    offset_t nodeOffset, ValueVector* resultVector, uint32_t posInVector) const {
    auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
    string_index_t index = 0;
    indexColumn->scanSegment(transaction, getChildState(state, ChildStateIndex::INDEX),
        offsetInChunk, 1, reinterpret_cast<uint8_t*>(&index));
    std::vector<std::pair<string_index_t, uint64_t>> offsetsToScan(1);
    offsetsToScan.emplace_back(index, posInVector);
    dictionary.scan(transaction, getChildState(state, ChildStateIndex::OFFSET),
        getChildState(state, ChildStateIndex::DATA), offsetsToScan, resultVector,
        getChildState(state, ChildStateIndex::INDEX).metadata);
}

void StringColumn::writeInternal(ColumnChunkData& persistentChunk, SegmentState& state,
    offset_t dstOffsetInSegment, const ColumnChunkData& data, offset_t srcOffset,
    length_t numValues) const {
    auto& stringPersistentChunk = persistentChunk.cast<StringChunkData>();
    numValues = std::min(numValues, data.getNumValues() - srcOffset);
    auto& strChunkToWriteFrom = data.cast<StringChunkData>();
    std::vector<string_index_t> indices;
    indices.resize(numValues);
    for (auto i = 0u; i < numValues; i++) {
        if (strChunkToWriteFrom.getNullData()->isNull(i + srcOffset)) {
            indices[i] = 0;
            continue;
        }
        const auto strVal = strChunkToWriteFrom.getValue<std::string_view>(i + srcOffset);
        indices[i] = dictionary.append(persistentChunk.cast<StringChunkData>().getDictionaryChunk(),
            state, strVal);
    }
    NullMask nullMask(numValues);
    nullMask.copyFromNullBits(data.getNullData()->getNullMask().getData(), srcOffset,
        0 /*dstOffset*/, numValues);
    // Write index to main column
    indexColumn->writeValuesInternal(getChildState(state, ChildStateIndex::INDEX),
        dstOffsetInSegment, reinterpret_cast<const uint8_t*>(&indices[0]), &nullMask,
        0 /*srcOffset*/, numValues);
    auto [min, max] = std::minmax_element(indices.begin(), indices.end());
    auto minWritten = StorageValue(*min);
    auto maxWritten = StorageValue(*max);
    updateStatistics(persistentChunk.getMetadata(), dstOffsetInSegment + numValues - 1, minWritten,
        maxWritten);
    indexColumn->updateStatistics(stringPersistentChunk.getIndexColumnChunk()->getMetadata(),
        dstOffsetInSegment + numValues - 1, minWritten, maxWritten);
}

void StringColumn::checkpointSegment(ColumnCheckpointState&& checkpointState) const {
    auto& persistentData = checkpointState.persistentData;
    Column::checkpointSegment(std::move(checkpointState));
    persistentData.syncNumValues();
}

void StringColumn::scanSegment(const Transaction* transaction, const SegmentState& state,
    offset_t startOffsetInChunk, row_idx_t numValuesToScan, ValueVector* resultVector,
    offset_t offsetInResult) const {
    if (nullColumn) {
        KU_ASSERT(state.nullState);
        nullColumn->scanSegment(transaction, *state.nullState, startOffsetInChunk, numValuesToScan,
            resultVector, offsetInResult);
    }

    KU_ASSERT(resultVector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    if (!resultVector->state || resultVector->state->getSelVector().isUnfiltered()) {
        scanUnfiltered(transaction, state, startOffsetInChunk, numValuesToScan, resultVector,
            offsetInResult);
    } else {
        scanFiltered(transaction, state, startOffsetInChunk, resultVector, offsetInResult);
    }
}

void StringColumn::scanSegment(const transaction::Transaction* transaction,
    const SegmentState& state, ColumnChunkData* resultChunk, common::offset_t startOffsetInSegment,
    common::row_idx_t numValuesToScan) const {
    auto startOffsetInResult = resultChunk->getNumValues();
    Column::scanSegment(transaction, state, resultChunk, startOffsetInSegment, numValuesToScan);
    KU_ASSERT(resultChunk->getDataType().getPhysicalType() == PhysicalTypeID::STRING);

    auto* stringResultChunk = ku_dynamic_cast<StringChunkData*>(resultChunk);
    // Revert change to numValues from Column::scanSegment (see note in list_column.cpp)
    // This shouldn't be necessary in future
    stringResultChunk->getIndexColumnChunk()->setNumValues(startOffsetInResult);

    auto* indexChunk = stringResultChunk->getIndexColumnChunk();
    indexColumn->scanSegment(transaction, getChildState(state, ChildStateIndex::INDEX), indexChunk,
        startOffsetInSegment, numValuesToScan);
    // TODO(bmwinger): each index needs to be incremented by the initial size of the dictionary
    auto initialDictSize = stringResultChunk->getDictionaryChunk().getOffsetChunk()->getNumValues();
    for (row_idx_t i = 0; i < numValuesToScan; i++) {
        indexChunk->setValue<string_index_t>(
            indexChunk->getValue<string_index_t>(startOffsetInResult + i) + initialDictSize,
            startOffsetInResult + i);
    }
    auto initialDictDataSize =
        stringResultChunk->getDictionaryChunk().getStringDataChunk()->getNumValues();
    auto* offsetChunk = stringResultChunk->getDictionaryChunk().getOffsetChunk();
    // FIXME(bmwinger): this is temporary and will not perform well (scans everything to avoid
    // implementing partial scans to ColumnChunkData)
    dictionary.scan(transaction, state, stringResultChunk->getDictionaryChunk());
    // Each offset needs to be incremented by the initial size of the dictionary data chunk
    for (row_idx_t i = initialDictSize; i < offsetChunk->getNumValues(); i++) {
        offsetChunk->setValue<string_offset_t>(
            offsetChunk->getValue<string_offset_t>(i) + initialDictDataSize, i);
    }
}

void StringColumn::scanUnfiltered(const Transaction* transaction, const SegmentState& state,
    offset_t startOffsetInChunk, offset_t numValuesToRead, ValueVector* resultVector,
    sel_t startPosInVector) const {
    // TODO: Replace indices with ValueVector to avoid maintaining `scan` interface from
    // uint8_t*.
    auto indices = std::make_unique<string_index_t[]>(numValuesToRead);
    indexColumn->scanSegment(transaction, getChildState(state, ChildStateIndex::INDEX),
        startOffsetInChunk, numValuesToRead, reinterpret_cast<uint8_t*>(indices.get()));

    std::vector<std::pair<string_index_t, uint64_t>> offsetsToScan;
    for (auto i = 0u; i < numValuesToRead; i++) {
        if (!resultVector->isNull(startPosInVector + i)) {
            offsetsToScan.emplace_back(indices[i], startPosInVector + i);
        }
    }

    if (offsetsToScan.size() == 0) {
        // All scanned values are null
        return;
    }
    dictionary.scan(transaction, getChildState(state, ChildStateIndex::OFFSET),
        getChildState(state, ChildStateIndex::DATA), offsetsToScan, resultVector,
        getChildState(state, ChildStateIndex::INDEX).metadata);
}

void StringColumn::scanFiltered(const Transaction* transaction, const SegmentState& state,
    offset_t startOffsetInChunk, ValueVector* resultVector, offset_t offsetInResult) const {
    std::vector<std::pair<string_index_t, uint64_t>> offsetsToScan;
    for (auto i = offsetInResult; i < resultVector->state->getSelVector().getSelSize(); i++) {
        const auto pos = resultVector->state->getSelVector()[i];
        if (!resultVector->isNull(pos)) {
            // TODO(bmwinger): optimize index scans by grouping them when adjacent
            const auto offsetInGroup = startOffsetInChunk + pos;
            string_index_t index = 0;
            indexColumn->scanSegment(transaction, getChildState(state, ChildStateIndex::INDEX),
                offsetInGroup, 1, reinterpret_cast<uint8_t*>(&index));
            offsetsToScan.emplace_back(index, pos);
        }
    }

    if (offsetsToScan.size() == 0) {
        // All scanned values are null
        return;
    }
    dictionary.scan(transaction, getChildState(state, ChildStateIndex::OFFSET),
        getChildState(state, ChildStateIndex::DATA), offsetsToScan, resultVector,
        getChildState(state, ChildStateIndex::INDEX).metadata);
}

bool StringColumn::canCheckpointInPlace(const SegmentState& state,
    const ColumnCheckpointState& checkpointState) const {
    row_idx_t strLenToAdd = 0u;
    idx_t numStrings = 0u;
    for (auto& segmentCheckpointState : checkpointState.segmentCheckpointStates) {
        auto& strChunk = segmentCheckpointState.chunkData.cast<StringChunkData>();
        numStrings += segmentCheckpointState.numRows;
        for (auto i = 0u; i < segmentCheckpointState.numRows; i++) {
            if (strChunk.getNullData()->isNull(segmentCheckpointState.startRowInData + i)) {
                continue;
            }
            strLenToAdd += strChunk.getStringLength(segmentCheckpointState.startRowInData + i);
        }
    }
    if (!dictionary.canCommitInPlace(state, numStrings, strLenToAdd)) {
        return false;
    }
    return canIndexCommitInPlace(state, numStrings, checkpointState.endRowIdxToWrite);
}

bool StringColumn::canIndexCommitInPlace(const SegmentState& state, uint64_t numStrings,
    offset_t maxOffset) const {
    const SegmentState& indexState = getChildState(state, ChildStateIndex::INDEX);
    if (indexState.metadata.getMaxCapacity(indexColumn->dataType.getPhysicalType()) < maxOffset) {
        return false;
    }
    if (indexState.metadata.compMeta.canAlwaysUpdateInPlace()) {
        return true;
    }
    const auto totalStringsAfterUpdate =
        getChildState(state, ChildStateIndex::OFFSET).metadata.numValues + numStrings;
    InPlaceUpdateLocalState localUpdateState{};
    // Check if the index column can store the largest new index in-place
    if (!indexState.metadata.compMeta.canUpdateInPlace(
            reinterpret_cast<const uint8_t*>(&totalStringsAfterUpdate), 0 /*pos*/, 1 /*numValues*/,
            PhysicalTypeID::UINT32, localUpdateState)) {
        return false;
    }
    return true;
}

} // namespace storage
} // namespace kuzu

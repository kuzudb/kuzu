#include "storage/table/string_column.h"

#include <algorithm>

#include "common/null_mask.h"
#include "common/vector/value_vector.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/compression/compression.h"
#include "storage/table/column.h"
#include "storage/table/column_chunk.h"
#include "storage/table/null_column.h"
#include "storage/table/string_chunk_data.h"
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

ChunkState& StringColumn::getChildState(ChunkState& state, ChildStateIndex child) {
    const auto childIdx = static_cast<idx_t>(child);
    return state.getChildState(childIdx);
}

const ChunkState& StringColumn::getChildState(const ChunkState& state, ChildStateIndex child) {
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

void StringColumn::scan(const Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) const {
    nullColumn->scan(transaction, *state.nullState, startOffsetInGroup, endOffsetInGroup,
        resultVector, offsetInVector);
    scanUnfiltered(transaction, state, startOffsetInGroup, endOffsetInGroup - startOffsetInGroup,
        resultVector, offsetInVector);
}

void StringColumn::scan(const Transaction* transaction, const ChunkState& state,
    ColumnChunkData* columnChunk, offset_t startOffset, offset_t endOffset) const {
    KU_ASSERT(state.nullState);
    Column::scan(transaction, state, columnChunk, startOffset, endOffset);
    if (columnChunk->getNumValues() == 0) {
        return;
    }

    auto& stringColumnChunk = columnChunk->cast<StringChunkData>();
    indexColumn->scan(transaction, getChildState(state, ChildStateIndex::INDEX),
        stringColumnChunk.getIndexColumnChunk(), startOffset, endOffset);
    dictionary.scan(transaction, state, stringColumnChunk.getDictionaryChunk());
}

void StringColumn::lookupInternal(const Transaction* transaction, const ChunkState& state,
    offset_t nodeOffset, ValueVector* resultVector, uint32_t posInVector) const {
    auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
    string_index_t index = 0;
    indexColumn->scan(transaction, getChildState(state, ChildStateIndex::INDEX), offsetInChunk,
        offsetInChunk + 1, reinterpret_cast<uint8_t*>(&index));
    std::vector<std::pair<string_index_t, uint64_t>> offsetsToScan(1);
    offsetsToScan.emplace_back(index, posInVector);
    dictionary.scan(transaction, getChildState(state, ChildStateIndex::OFFSET),
        getChildState(state, ChildStateIndex::DATA), offsetsToScan, resultVector,
        getChildState(state, ChildStateIndex::INDEX).metadata);
}

void StringColumn::write(ColumnChunkData& persistentChunk, ChunkState& state, offset_t dstOffset,
    ColumnChunkData* data, offset_t srcOffset, length_t numValues) {
    auto& stringPersistentChunk = persistentChunk.cast<StringChunkData>();
    numValues = std::min(numValues, data->getNumValues() - srcOffset);
    auto& strChunkToWriteFrom = data->cast<StringChunkData>();
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
    nullMask.copyFromNullBits(data->getNullData()->getNullMask().getData(), srcOffset,
        0 /*dstOffset*/, numValues);
    // Write index to main column
    indexColumn->writeValues(getChildState(state, ChildStateIndex::INDEX), dstOffset,
        reinterpret_cast<const uint8_t*>(&indices[0]), &nullMask, 0 /*srcOffset*/, numValues);
    auto [min, max] = std::minmax_element(indices.begin(), indices.end());
    auto minWritten = StorageValue(*min);
    auto maxWritten = StorageValue(*max);
    updateStatistics(persistentChunk.getMetadata(), dstOffset + numValues - 1, minWritten,
        maxWritten);
    indexColumn->updateStatistics(stringPersistentChunk.getIndexColumnChunk()->getMetadata(),
        dstOffset + numValues - 1, minWritten, maxWritten);
}

void StringColumn::checkpointColumnChunk(ColumnCheckpointState& checkpointState) {
    Column::checkpointColumnChunk(checkpointState);
    checkpointState.persistentData.syncNumValues();
}

void StringColumn::scanInternal(Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInChunk, row_idx_t numValuesToScan, ValueVector* resultVector) const {
    KU_ASSERT(resultVector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    if (resultVector->state->getSelVector().isUnfiltered()) {
        scanUnfiltered(transaction, state, startOffsetInChunk, numValuesToScan, resultVector);
    } else {
        scanFiltered(transaction, state, startOffsetInChunk, resultVector);
    }
}

void StringColumn::scanUnfiltered(const Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInChunk, offset_t numValuesToRead, ValueVector* resultVector,
    sel_t startPosInVector) const {
    // TODO: Replace indices with ValueVector to avoid maintaining `scan` interface from
    // uint8_t*.
    auto indices = std::make_unique<string_index_t[]>(numValuesToRead);
    indexColumn->scan(transaction, getChildState(state, ChildStateIndex::INDEX), startOffsetInChunk,
        startOffsetInChunk + numValuesToRead, reinterpret_cast<uint8_t*>(indices.get()));

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

void StringColumn::scanFiltered(Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInChunk, ValueVector* resultVector) const {
    std::vector<std::pair<string_index_t, uint64_t>> offsetsToScan;
    for (auto i = 0u; i < resultVector->state->getSelVector().getSelSize(); i++) {
        const auto pos = resultVector->state->getSelVector()[i];
        if (!resultVector->isNull(pos)) {
            // TODO(bmwinger): optimize index scans by grouping them when adjacent
            const auto offsetInGroup = startOffsetInChunk + pos;
            string_index_t index = 0;
            indexColumn->scan(transaction, getChildState(state, ChildStateIndex::INDEX),
                offsetInGroup, offsetInGroup + 1, reinterpret_cast<uint8_t*>(&index));
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

bool StringColumn::canCheckpointInPlace(const ChunkState& state,
    const ColumnCheckpointState& checkpointState) {
    row_idx_t strLenToAdd = 0u;
    idx_t numStrings = 0u;
    for (auto& chunkCheckpointState : checkpointState.chunkCheckpointStates) {
        auto& strChunk = chunkCheckpointState.chunkData->cast<StringChunkData>();
        KU_ASSERT(chunkCheckpointState.numRows == strChunk.getNumValues());
        numStrings += strChunk.getNumValues();
        for (auto i = 0u; i < strChunk.getNumValues(); i++) {
            if (strChunk.getNullData()->isNull(i)) {
                continue;
            }
            strLenToAdd += strChunk.getStringLength(i);
        }
    }
    if (!dictionary.canCommitInPlace(state, numStrings, strLenToAdd)) {
        return false;
    }
    return canIndexCommitInPlace(state, numStrings, checkpointState.endRowIdxToWrite);
}

bool StringColumn::canIndexCommitInPlace(const ChunkState& state, uint64_t numStrings,
    offset_t maxOffset) const {
    const ChunkState& indexState = getChildState(state, ChildStateIndex::INDEX);
    if (indexColumn->isEndOffsetOutOfPagesCapacity(indexState.metadata, maxOffset)) {
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

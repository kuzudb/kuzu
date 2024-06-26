#include "storage/store/string_column.h"

#include <algorithm>

#include "common/null_mask.h"
#include "storage/compression/compression.h"
#include "storage/store/column.h"
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

StringColumn::StringColumn(std::string name, LogicalType dataType,
    const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, DiskArrayCollection& metadataDAC,
    BufferManager* bufferManager, WAL* wal, Transaction* transaction, bool enableCompression)
    : Column{name, std::move(dataType), metaDAHeaderInfo, dataFH, metadataDAC, bufferManager, wal,
          transaction, enableCompression, true /* requireNullColumn */},
      dictionary{name, metaDAHeaderInfo, dataFH, metadataDAC, bufferManager, wal, transaction,
          enableCompression} {

    auto indexColumnName =
        StorageUtils::getColumnName(name, StorageUtils::ColumnType::INDEX, "index");
    indexColumn = std::make_unique<Column>(indexColumnName, LogicalType::UINT32(),
        *metaDAHeaderInfo.childrenInfos[2], dataFH, metadataDAC, bufferManager, wal, transaction,
        enableCompression, false /*requireNullColumn*/);
}

Column::ChunkState& StringColumn::getChildState(ChunkState& state, ChildStateIndex child) {
    const auto childIdx = static_cast<common::idx_t>(child);
    return state.getChildState(childIdx);
}

const Column::ChunkState& StringColumn::getChildState(const ChunkState& state,
    ChildStateIndex child) {
    const auto childIdx = static_cast<common::idx_t>(child);
    return state.getChildState(childIdx);
}

void StringColumn::initChunkState(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    ChunkState& state) {
    Column::initChunkState(transaction, nodeGroupIdx, state);
    state.childrenStates.resize(CHILD_STATE_COUNT);
    indexColumn->initChunkState(transaction, nodeGroupIdx,
        getChildState(state, ChildStateIndex::INDEX));
    dictionary.initChunkState(transaction, nodeGroupIdx, state);
    KU_ASSERT(state.metadata.numValues ==
              getChildState(state, ChildStateIndex::INDEX).metadata.numValues);
}

void StringColumn::scan(Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    nullColumn->scan(transaction, *state.nullState, startOffsetInGroup, endOffsetInGroup,
        resultVector, offsetInVector);
    scanUnfiltered(transaction, state, startOffsetInGroup, endOffsetInGroup - startOffsetInGroup,
        resultVector, offsetInVector);
}

void StringColumn::scan(Transaction* transaction, const ChunkState& state,
    ColumnChunkData* columnChunk, offset_t startOffset, offset_t endOffset) {
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

void StringColumn::append(ColumnChunkData* columnChunk, ChunkState& state) {
    Column::append(columnChunk, state);
    auto& stringColumnChunk = columnChunk->cast<StringChunkData>();
    indexColumn->append(stringColumnChunk.getIndexColumnChunk(),
        getChildState(state, ChildStateIndex::INDEX));
    dictionary.append(state, stringColumnChunk.getDictionaryChunk());
}

void StringColumn::writeValue(ChunkState& state, offset_t offsetInChunk,
    ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto& kuStr = vectorToWriteFrom->getValue<ku_string_t>(posInVectorToWriteFrom);
    auto index = dictionary.append(state, kuStr.getAsStringView());
    // Write index to main column
    // This function should only be called when the string is non-null, so we don't need to pass
    // null data
    KU_ASSERT(!vectorToWriteFrom->isNull(posInVectorToWriteFrom));
    indexColumn->writeValues(getChildState(state, ChildStateIndex::INDEX), offsetInChunk,
        (uint8_t*)&index, nullptr /*nullChunkData*/);
    updateStatistics(state.metadata, offsetInChunk, StorageValue(index), StorageValue(index));
    indexColumn->updateStatistics(getChildState(state, ChildStateIndex::INDEX).metadata,
        offsetInChunk, StorageValue(index), StorageValue(index));
}

void StringColumn::write(ChunkState& state, offset_t dstOffset, ColumnChunkData* data,
    offset_t srcOffset, length_t numValues) {
    numValues = std::min(numValues, data->getNumValues() - srcOffset);
    auto& strChunk = data->cast<StringChunkData>();
    std::vector<string_index_t> indices;
    indices.resize(numValues);
    for (auto i = 0u; i < numValues; i++) {
        if (strChunk.getNullChunk()->isNull(i + srcOffset)) {
            indices[i] = 0;
            continue;
        }
        auto strVal = strChunk.getValue<std::string_view>(i + srcOffset);
        indices[i] = dictionary.append(state, strVal);
    }
    NullMask nullMask(numValues);
    nullMask.copyFromNullBits(data->getNullChunk()->getNullMask().getData(), srcOffset,
        0 /*dstOffset=*/, numValues);
    // Write index to main column
    indexColumn->writeValues(getChildState(state, ChildStateIndex::INDEX), dstOffset,
        reinterpret_cast<const uint8_t*>(&indices[0]), &nullMask, 0 /*srcOffset*/, numValues);
    auto [min, max] = std::minmax_element(indices.begin(), indices.end());
    StorageValue minWritten = StorageValue(*min);
    StorageValue maxWritten = StorageValue(*max);
    updateStatistics(state.metadata, dstOffset + numValues - 1, minWritten, maxWritten);
    indexColumn->updateStatistics(getChildState(state, ChildStateIndex::INDEX).metadata,
        dstOffset + numValues - 1, minWritten, maxWritten);
}

void StringColumn::scanInternal(Transaction* transaction, const ChunkState& state, idx_t vectorIdx,
    row_idx_t numValuesToScan, ValueVector* nodeIDVector, ValueVector* resultVector) {
    KU_ASSERT(resultVector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    const auto startOffsetInChunk = vectorIdx * DEFAULT_VECTOR_CAPACITY;
    if (nodeIDVector->state->getSelVector().isUnfiltered()) {
        scanUnfiltered(transaction, state, startOffsetInChunk, numValuesToScan, resultVector);
    } else {
        scanFiltered(transaction, state, startOffsetInChunk, nodeIDVector, resultVector);
    }
}

void StringColumn::scanUnfiltered(Transaction* transaction, const ChunkState& readState,
    offset_t startOffsetInChunk, offset_t numValuesToRead, ValueVector* resultVector,
    sel_t startPosInVector) {
    // TODO: Replace indices with ValueVector to avoid maintaining `scan` interface from uint8_t*.
    auto indices = std::make_unique<string_index_t[]>(numValuesToRead);
    indexColumn->scan(transaction, getChildState(readState, ChildStateIndex::INDEX),
        startOffsetInChunk, startOffsetInChunk + numValuesToRead,
        reinterpret_cast<uint8_t*>(indices.get()));

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
    dictionary.scan(transaction, getChildState(readState, ChildStateIndex::OFFSET),
        getChildState(readState, ChildStateIndex::DATA), offsetsToScan, resultVector,
        getChildState(readState, ChildStateIndex::INDEX).metadata);
}

void StringColumn::scanFiltered(Transaction* transaction, const ChunkState& readState,
    offset_t startOffsetInChunk, const ValueVector* nodeIDVector, ValueVector* resultVector) {
    std::vector<std::pair<string_index_t, uint64_t>> offsetsToScan;
    for (auto i = 0u; i < nodeIDVector->state->getSelVector().getSelSize(); i++) {
        const auto pos = nodeIDVector->state->getSelVector()[i];
        if (!resultVector->isNull(pos)) {
            // TODO(bmwinger): optimize index scans by grouping them when adjacent
            const auto offsetInGroup = startOffsetInChunk + pos;
            string_index_t index;
            indexColumn->scan(transaction, getChildState(readState, ChildStateIndex::INDEX),
                offsetInGroup, offsetInGroup + 1, reinterpret_cast<uint8_t*>(&index));
            offsetsToScan.emplace_back(index, pos);
        }
    }

    if (offsetsToScan.size() == 0) {
        // All scanned values are null
        return;
    }
    dictionary.scan(transaction, getChildState(readState, ChildStateIndex::OFFSET),
        getChildState(readState, ChildStateIndex::DATA), offsetsToScan, resultVector,
        getChildState(readState, ChildStateIndex::INDEX).metadata);
}

void StringColumn::lookupInternal(Transaction* transaction, ChunkState& readState,
    ValueVector* nodeIDVector, ValueVector* resultVector) {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::STRING);
    std::vector<std::pair<string_index_t, uint64_t>> offsetsToScan;
    for (auto i = 0u; i < nodeIDVector->state->getSelVector().getSelSize(); i++) {
        auto pos = nodeIDVector->state->getSelVector()[i];
        if (!nodeIDVector->isNull(pos) && !resultVector->isNull(pos)) {
            auto offsetInGroup = nodeIDVector->readNodeOffset(pos) -
                                 StorageUtils::getStartOffsetOfNodeGroup(readState.nodeGroupIdx);
            string_index_t index;
            indexColumn->scan(transaction, getChildState(readState, ChildStateIndex::INDEX),
                offsetInGroup, offsetInGroup + 1, (uint8_t*)&index);
            offsetsToScan.emplace_back(index, pos);
        }
    }
    if (offsetsToScan.size() == 0) {
        // All scanned values are null
        return;
    }
    dictionary.scan(transaction, getChildState(readState, ChildStateIndex::OFFSET),
        getChildState(readState, ChildStateIndex::DATA), offsetsToScan, resultVector,
        getChildState(readState, ChildStateIndex::INDEX).metadata);
}

bool StringColumn::canCommitInPlace(const ChunkState& state,
    const ChunkCollection& localInsertChunks, const offset_to_row_idx_t& insertInfo,
    const ChunkCollection& localUpdateChunks, const offset_to_row_idx_t& updateInfo) {
    auto strLenToAdd = 0u;
    for (auto& [_, rowIdx] : updateInfo) {
        auto [chunkIdx, offsetInLocalChunk] =
            LocalChunkedGroupCollection::getChunkIdxAndOffsetInChunk(rowIdx);
        auto& localUpdateChunk = localUpdateChunks[chunkIdx]->cast<StringChunkData>();
        strLenToAdd += localUpdateChunk.getStringLength(offsetInLocalChunk);
    }
    offset_t maxOffset = 0u;
    for (auto& [offset, rowIdx] : insertInfo) {
        if (offset > maxOffset) {
            maxOffset = offset;
        }
        auto [chunkIdx, offsetInLocalChunk] =
            LocalChunkedGroupCollection::getChunkIdxAndOffsetInChunk(rowIdx);
        auto& localInsertChunk = localInsertChunks[chunkIdx]->cast<StringChunkData>();
        strLenToAdd += localInsertChunk.getStringLength(offsetInLocalChunk);
    }
    auto numStrings = insertInfo.size() + updateInfo.size();
    if (!dictionary.canCommitInPlace(state, numStrings, strLenToAdd)) {
        return false;
    }
    return canIndexCommitInPlace(state, numStrings, maxOffset);
}

bool StringColumn::canCommitInPlace(const ChunkState& state,
    const std::vector<offset_t>& dstOffsets, ColumnChunkData* chunk, offset_t srcOffset) {
    auto strLenToAdd = 0u;
    auto& strChunk = chunk->cast<StringChunkData>();
    auto length = std::min((uint64_t)dstOffsets.size(), strChunk.getNumValues());
    for (auto i = 0u; i < length; i++) {
        if (strChunk.getNullChunk()->isNull(i)) {
            continue;
        }
        strLenToAdd += strChunk.getStringLength(i + srcOffset);
    }
    auto numStrings = dstOffsets.size();
    if (!dictionary.canCommitInPlace(state, numStrings, strLenToAdd)) {
        return false;
    }
    auto maxDstOffset = getMaxOffset(dstOffsets);
    return canIndexCommitInPlace(state, numStrings, maxDstOffset);
}

bool StringColumn::canIndexCommitInPlace(const ChunkState& state, uint64_t numStrings,
    offset_t maxOffset) {
    const ChunkState& indexState = getChildState(state, ChildStateIndex::INDEX);
    if (indexColumn->isMaxOffsetOutOfPagesCapacity(indexState.metadata, maxOffset)) {
        return false;
    }
    if (indexState.metadata.compMeta.canAlwaysUpdateInPlace()) {
        return true;
    }
    auto totalStringsAfterUpdate =
        getChildState(state, ChildStateIndex::OFFSET).metadata.numValues + numStrings;
    // Check if the index column can store the largest new index in-place
    if (!indexState.metadata.compMeta.canUpdateInPlace((const uint8_t*)&totalStringsAfterUpdate,
            0 /*pos*/, 1 /*numValues*/, PhysicalTypeID::UINT32)) {
        return false;
    }
    return true;
}

void StringColumn::prepareCommit() {
    Column::prepareCommit();
    indexColumn->prepareCommit();
    dictionary.prepareCommit();
}

void StringColumn::checkpointInMemory() {
    Column::checkpointInMemory();
    indexColumn->checkpointInMemory();
    dictionary.checkpointInMemory();
}

void StringColumn::rollbackInMemory() {
    Column::rollbackInMemory();
    indexColumn->rollbackInMemory();
    dictionary.rollbackInMemory();
}

void StringColumn::prepareCommitForExistingChunk(Transaction* transaction, ChunkState& state,
    const ChunkCollection& localInsertChunk, const offset_to_row_idx_t& insertInfo,
    const ChunkCollection& localUpdateChunk, const offset_to_row_idx_t& updateInfo,
    const offset_set_t& deleteInfo) {
    Column::prepareCommitForExistingChunk(transaction, state, localInsertChunk, insertInfo,
        localUpdateChunk, updateInfo, deleteInfo);

    const auto& indexState = getChildState(state, ChildStateIndex::INDEX);
    // The changes for the child column are commited by the main column.
    // Explicitly applying the commit for the child column could overwrite the column with the
    // incorrect values. For example, the indices are set by the main column and not the child.
    // Thus, we call prepareCommitForExistingChunk() on the main column only
    indexColumn->metadataDA->update(indexState.nodeGroupIdx, indexState.metadata);
}

void StringColumn::prepareCommitForExistingChunk(Transaction* transaction, ChunkState& state,
    const std::vector<offset_t>& dstOffsets, ColumnChunkData* chunk, offset_t srcOffset) {
    KU_ASSERT(chunk->getDataType().getPhysicalType() == dataType.getPhysicalType());
    Column::prepareCommitForExistingChunk(transaction, state, dstOffsets, chunk, srcOffset);

    const auto& indexState = getChildState(state, ChildStateIndex::INDEX);
    indexColumn->metadataDA->update(indexState.nodeGroupIdx, indexState.metadata);
}

void StringColumn::updateStateMetadataNumValues(ChunkState& state, size_t numValues) {
    state.metadata.numValues = numValues;
    getChildState(state, ChildStateIndex::INDEX).metadata.numValues = numValues;
}

} // namespace storage
} // namespace kuzu

#include "storage/store/string_column.h"

#include <algorithm>

#include "common/null_mask.h"
#include "storage/compression/compression.h"
#include "storage/store/column.h"
#include "storage/store/null_column.h"
#include "storage/store/string_column_chunk.h"
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

    // add child column for index
    auto indexColName = StorageUtils::getColumnName(name, StorageUtils::ColumnType::INDEX, "index");
    indexColumn = ColumnFactory::createColumn(indexColName, *LogicalType::UINT32(),
        *metaDAHeaderInfo.childrenInfos.at(2), dataFH, metadataDAC, bufferManager, wal, transaction,
        enableCompression);
}

Column::ChunkState& StringColumn::getIndexState(ChunkState& state) {
    static constexpr size_t indexColumnChildIndex =
        static_cast<size_t>(StringChunkData::ChildType::INDEX);
    KU_ASSERT(state.childrenStates.size() > indexColumnChildIndex);
    return state.childrenStates[indexColumnChildIndex];
}

const Column::ChunkState& StringColumn::getIndexState(const ChunkState& state) {
    static constexpr size_t indexColumnChildIndex =
        static_cast<size_t>(StringChunkData::ChildType::INDEX);
    KU_ASSERT(state.childrenStates.size() > indexColumnChildIndex);
    return state.childrenStates[indexColumnChildIndex];
}

void StringColumn::initChunkState(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    ChunkState& state) {
    Column::initChunkState(transaction, nodeGroupIdx, state);
    state.childrenStates.resize(CHILD_COLUMN_COUNT);
    indexColumn->initChunkState(transaction, nodeGroupIdx, getIndexState(state));
    dictionary.initChunkState(transaction, nodeGroupIdx, getIndexState(state));
    KU_ASSERT(state.metadata.numValues == getIndexState(state).metadata.numValues);
}

void StringColumn::scan(Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    nullColumn->scan(transaction, *state.nullState, startOffsetInGroup, endOffsetInGroup,
        resultVector, offsetInVector);
    scanUnfiltered(transaction, state, startOffsetInGroup, endOffsetInGroup - startOffsetInGroup,
        resultVector, offsetInVector);
    KU_ASSERT(state.metadata.numValues == getIndexState(state).metadata.numValues);
}

void StringColumn::scan(Transaction* transaction, const ChunkState& state,
    ColumnChunkData* columnChunk, offset_t startOffset, offset_t endOffset) {
    nullColumn->scan(transaction, state, columnChunk->getNullChunk(), startOffset, endOffset);
    if (state.nodeGroupIdx >= metadataDA->getNumElements(transaction->getType())) {
        columnChunk->setNumValues(0);
    } else {
        auto chunkMetadata = metadataDA->get(state.nodeGroupIdx, transaction->getType());
        auto numValues = chunkMetadata.numValues == 0 ?
                             0 :
                             std::min(endOffset, chunkMetadata.numValues) - startOffset;
        columnChunk->setNumValues(numValues);
    }

    if (columnChunk->getNumValues() == 0) {
        return;
    }

    auto& stringColumnChunk = columnChunk->cast<StringChunkData>();
    indexColumn->scan(transaction, state, stringColumnChunk.getIndexColumnChunk(), startOffset,
        endOffset);
    dictionary.scan(transaction, state, stringColumnChunk.getDictionaryChunk());
}

void StringColumn::append(ColumnChunkData* columnChunk, ChunkState& state) {
    Column::append(columnChunk, state);
    auto& stringColumnChunk = columnChunk->cast<StringChunkData>();
    indexColumn->append(stringColumnChunk.getIndexColumnChunk(), getIndexState(state));
    dictionary.append(getIndexState(state), stringColumnChunk.getDictionaryChunk());
    KU_ASSERT(state.metadata.numValues == getIndexState(state).metadata.numValues);
}

void StringColumn::writeValue(ChunkState& state, offset_t offsetInChunk,
    ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto& kuStr = vectorToWriteFrom->getValue<ku_string_t>(posInVectorToWriteFrom);
    auto index = dictionary.append(getIndexState(state), kuStr.getAsStringView());
    // Write index to main column
    // This function should only be called when the string is non-null, so we don't need to pass
    // null data
    KU_ASSERT(!vectorToWriteFrom->isNull(posInVectorToWriteFrom));
    indexColumn->writeValues(getIndexState(state), offsetInChunk, (uint8_t*)&index,
        nullptr /*nullChunkData*/);
    updateStatistics(state.metadata, offsetInChunk, StorageValue(index), StorageValue(index));
    indexColumn->updateStatistics(getIndexState(state).metadata, offsetInChunk, StorageValue(index),
        StorageValue(index));
    KU_ASSERT(state.metadata.numValues == getIndexState(state).metadata.numValues);
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
        indices[i] = dictionary.append(getIndexState(state), strVal);
    }
    NullMask nullMask(numValues);
    nullMask.copyFromNullBits(data->getNullChunk()->getNullMask().getData(), srcOffset,
        0 /*dstOffset=*/, numValues);
    // Write index to main column
    indexColumn->writeValues(getIndexState(state), dstOffset,
        reinterpret_cast<const uint8_t*>(&indices[0]), &nullMask, 0 /*srcOffset*/, numValues);
    auto [min, max] = std::minmax_element(indices.begin(), indices.end());
    StorageValue minWritten = StorageValue(*min);
    StorageValue maxWritten = StorageValue(*max);
    updateStatistics(state.metadata, dstOffset + numValues - 1, minWritten, maxWritten);
    indexColumn->updateStatistics(getIndexState(state).metadata, dstOffset + numValues - 1,
        minWritten, maxWritten);
    KU_ASSERT(state.metadata.numValues == getIndexState(state).metadata.numValues);
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
    KU_ASSERT(state.metadata.numValues == getIndexState(state).metadata.numValues);
}

void StringColumn::scanUnfiltered(Transaction* transaction, const ChunkState& readState,
    offset_t startOffsetInChunk, offset_t numValuesToRead, ValueVector* resultVector,
    sel_t startPosInVector) {
    // TODO: Replace indices with ValueVector to avoid maintaining `scan` interface from uint8_t*.
    auto indices = std::make_unique<string_index_t[]>(numValuesToRead);
    indexColumn->scan(transaction, getIndexState(readState), startOffsetInChunk,
        startOffsetInChunk + numValuesToRead, reinterpret_cast<uint8_t*>(indices.get()));

    std::vector<std::pair<string_index_t, uint64_t>> offsetsToScan;
    for (auto i = 0u; i < numValuesToRead; i++) {
        if (!resultVector->isNull(startPosInVector + i)) {
            offsetsToScan.emplace_back(indices[i], startPosInVector + i);
        }
    }

    KU_ASSERT(readState.metadata.numValues == getIndexState(readState).metadata.numValues);

    if (offsetsToScan.size() == 0) {
        // All scanned values are null
        return;
    }
    dictionary.scan(transaction,
        getIndexState(readState)
            .childrenStates[DictionaryColumn::OFFSET_COLUMN_CHILD_READ_STATE_IDX],
        getIndexState(readState).childrenStates[DictionaryColumn::DATA_COLUMN_CHILD_READ_STATE_IDX],
        offsetsToScan, resultVector, getIndexState(readState).metadata);
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
            indexColumn->scan(transaction, getIndexState(readState), offsetInGroup,
                offsetInGroup + 1, reinterpret_cast<uint8_t*>(&index));
            offsetsToScan.emplace_back(index, pos);
        }
    }

    KU_ASSERT(readState.metadata.numValues == getIndexState(readState).metadata.numValues);

    if (offsetsToScan.size() == 0) {
        // All scanned values are null
        return;
    }
    dictionary.scan(transaction,
        getIndexState(readState)
            .childrenStates[DictionaryColumn::OFFSET_COLUMN_CHILD_READ_STATE_IDX],
        getIndexState(readState).childrenStates[DictionaryColumn::DATA_COLUMN_CHILD_READ_STATE_IDX],
        offsetsToScan, resultVector, getIndexState(readState).metadata);
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
            indexColumn->scan(transaction, getIndexState(readState), offsetInGroup,
                offsetInGroup + 1, (uint8_t*)&index);
            offsetsToScan.emplace_back(index, pos);
        }
    }
    if (offsetsToScan.size() == 0) {
        // All scanned values are null
        return;
    }
    dictionary.scan(transaction,
        getIndexState(readState)
            .childrenStates[DictionaryColumn::OFFSET_COLUMN_CHILD_READ_STATE_IDX],
        getIndexState(readState).childrenStates[DictionaryColumn::DATA_COLUMN_CHILD_READ_STATE_IDX],
        offsetsToScan, resultVector, getIndexState(readState).metadata);
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
    if (!dictionary.canCommitInPlace(getIndexState(state), numStrings, strLenToAdd)) {
        return false;
    }
    return canIndexCommitInPlace(getIndexState(state), numStrings, maxOffset);
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
    if (!dictionary.canCommitInPlace(getIndexState(state), numStrings, strLenToAdd)) {
        return false;
    }
    auto maxDstOffset = getMaxOffset(dstOffsets);
    return canIndexCommitInPlace(getIndexState(state), numStrings, maxDstOffset);
}

bool StringColumn::canIndexCommitInPlace(const ChunkState& state, uint64_t numStrings,
    offset_t maxOffset) {
    if (indexColumn->isMaxOffsetOutOfPagesCapacity(state.metadata, maxOffset)) {
        return false;
    }
    if (state.metadata.compMeta.canAlwaysUpdateInPlace()) {
        return true;
    }
    auto totalStringsAfterUpdate =
        state.childrenStates[DictionaryColumn::OFFSET_COLUMN_CHILD_READ_STATE_IDX]
            .metadata.numValues +
        numStrings;
    // Check if the index column can store the largest new index in-place
    if (!state.metadata.compMeta.canUpdateInPlace((const uint8_t*)&totalStringsAfterUpdate,
            0 /*pos*/, PhysicalTypeID::UINT32)) {
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

    const auto& indexState = getIndexState(state);
    // the changes for the child column are commited by the main column
    // explicitly applying the commit for the child column could overwrite the column with the
    // incorrect values for example, the indices are set by the main column and not the child
    indexColumn->metadataDA->update(indexState.nodeGroupIdx, indexState.metadata);
}

void StringColumn::prepareCommitForExistingChunk(Transaction* transaction, ChunkState& state,
    const std::vector<offset_t>& dstOffsets, ColumnChunkData* chunk, offset_t srcOffset) {
    KU_ASSERT(chunk->getDataType().getPhysicalType() == dataType.getPhysicalType());
    Column::prepareCommitForExistingChunk(transaction, state, dstOffsets, chunk, srcOffset);

    const auto& indexState = getIndexState(state);
    indexColumn->metadataDA->update(indexState.nodeGroupIdx, indexState.metadata);
}

void StringColumn::applyLocalChunkToColumn(ChunkState& state, const ChunkCollection& localChunks,
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
                getIndexState(state).metadata.numValues = state.metadata.numValues;
            }
        }
    }
}

ChunkCollection StringColumn::getStringIndexChunkCollection(
    const ChunkCollection& chunkCollection) {
    ChunkCollection childChunkCollection;
    for (const auto& chunk : chunkCollection) {
        auto& stringChunk = chunk->cast<StringChunkData>();
        childChunkCollection.push_back(stringChunk.getIndexColumnChunk());
    }
    return childChunkCollection;
}

} // namespace storage
} // namespace kuzu

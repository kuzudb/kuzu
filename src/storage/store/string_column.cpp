#include "storage/store/string_column.h"

#include "common/null_mask.h"
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
    const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, transaction::Transaction* transaction,
    RWPropertyStats stats, bool enableCompression)
    : Column{name, std::move(dataType), metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal,
          transaction, stats, enableCompression, true /* requireNullColumn */},
      dictionary{name, metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, transaction, stats,
          enableCompression} {}

void StringColumn::initReadState(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    offset_t startOffsetInChunk, ReadState& readState) {
    Column::initReadState(transaction, nodeGroupIdx, startOffsetInChunk, readState);
    dictionary.initReadState(transaction, nodeGroupIdx, startOffsetInChunk, readState);
}

void StringColumn::scan(Transaction* transaction, ReadState& readState, offset_t startOffsetInGroup,
    offset_t endOffsetInGroup, ValueVector* resultVector, uint64_t offsetInVector) {
    nullColumn->scan(transaction, *readState.nullState, startOffsetInGroup, endOffsetInGroup,
        resultVector, offsetInVector);
    scanUnfiltered(transaction, readState, startOffsetInGroup,
        endOffsetInGroup - startOffsetInGroup, resultVector, offsetInVector);
}

void StringColumn::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    ColumnChunk* columnChunk, offset_t startOffset, offset_t endOffset) {
    Column::scan(transaction, nodeGroupIdx, columnChunk, startOffset, endOffset);
    if (columnChunk->getNumValues() == 0) {
        return;
    }
    auto stringColumnChunk = ku_dynamic_cast<ColumnChunk*, StringColumnChunk*>(columnChunk);
    dictionary.scan(transaction, nodeGroupIdx, stringColumnChunk->getDictionaryChunk());
}

void StringColumn::append(ColumnChunk* columnChunk, node_group_idx_t nodeGroupIdx) {
    Column::append(columnChunk, nodeGroupIdx);
    auto stringColumnChunk = ku_dynamic_cast<ColumnChunk*, StringColumnChunk*>(columnChunk);
    dictionary.append(nodeGroupIdx, stringColumnChunk->getDictionaryChunk());
}

void StringColumn::writeValue(const ColumnChunkMetadata& chunkMeta, node_group_idx_t nodeGroupIdx,
    offset_t offsetInChunk, ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto& kuStr = vectorToWriteFrom->getValue<ku_string_t>(posInVectorToWriteFrom);
    auto index = dictionary.append(nodeGroupIdx, kuStr.getAsStringView());
    NullMask nullMask(1);
    nullMask.setNull(0, vectorToWriteFrom->isNull(posInVectorToWriteFrom));
    // Write index to main column
    auto state = ReadState{chunkMeta,
        chunkMeta.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType)};
    // This function should only be called when the string is non-null, so we don't need to pass
    // null data
    Column::writeValues(state, offsetInChunk, (uint8_t*)&index, nullptr /*nullChunkData*/);
}

void StringColumn::write(node_group_idx_t nodeGroupIdx, offset_t dstOffset, ColumnChunk* data,
    offset_t srcOffset, length_t numValues) {
    auto state = getReadState(TransactionType::WRITE, nodeGroupIdx);
    numValues = std::min(numValues, data->getNumValues() - srcOffset);
    auto strChunk = ku_dynamic_cast<ColumnChunk*, StringColumnChunk*>(data);
    std::vector<string_index_t> indices;
    indices.resize(numValues);
    for (auto i = 0u; i < numValues; i++) {
        if (strChunk->getNullChunk()->isNull(i + srcOffset)) {
            indices[i] = 0;
            continue;
        }
        auto strVal = strChunk->getValue<std::string_view>(i + srcOffset);
        indices[i] = dictionary.append(nodeGroupIdx, strVal);
    }
    NullMask nullMask(numValues);
    nullMask.copyFromNullBits(data->getNullChunk()->getNullMask().getData(), srcOffset,
        0 /*dstOffset=*/, numValues);
    // Write index to main column
    Column::writeValues(state, dstOffset, reinterpret_cast<const uint8_t*>(&indices[0]), &nullMask,
        0 /*srcOffset*/, numValues);
    if (dstOffset + numValues > state.metadata.numValues) {
        state.metadata.numValues = dstOffset + numValues;
        metadataDA->update(nodeGroupIdx, state.metadata);
    }
}

void StringColumn::scanInternal(Transaction* transaction, ReadState& readState,
    ValueVector* nodeIDVector, ValueVector* resultVector) {
    KU_ASSERT(resultVector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto [nodeGroupIdx, startOffsetInChunk] =
        StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeIDVector->readNodeOffset(0));
    if (nodeIDVector->state->selVector->isUnfiltered()) {
        scanUnfiltered(transaction, readState, startOffsetInChunk,
            nodeIDVector->state->selVector->selectedSize, resultVector);
    } else {
        scanFiltered(transaction, readState, startOffsetInChunk, nodeIDVector, resultVector);
    }
}

void StringColumn::scanUnfiltered(transaction::Transaction* transaction, ReadState& readState,
    offset_t startOffsetInChunk, offset_t numValuesToRead, ValueVector* resultVector,
    sel_t startPosInVector) {
    // TODO: Replace indices with ValueVector to avoid maintaining `scan` interface from uint8_t*.
    auto indices = std::make_unique<string_index_t[]>(numValuesToRead);
    Column::scan(transaction, readState, startOffsetInChunk, startOffsetInChunk + numValuesToRead,
        (uint8_t*)indices.get());

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
    dictionary.scan(transaction,
        readState.childrenStates[DictionaryColumn::OFFSET_COLUMN_CHILD_READ_STATE_IDX],
        readState.childrenStates[DictionaryColumn::DATA_COLUMN_CHILD_READ_STATE_IDX], offsetsToScan,
        resultVector, readState.metadata);
}

void StringColumn::scanFiltered(transaction::Transaction* transaction, ReadState& readState,
    common::offset_t startOffsetInChunk, ValueVector* nodeIDVector, ValueVector* resultVector) {
    std::vector<std::pair<string_index_t, uint64_t>> offsetsToScan;
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        if (!resultVector->isNull(pos)) {
            // TODO(bmwinger): optimize index scans by grouping them when adjacent
            auto offsetInGroup = startOffsetInChunk + pos;
            string_index_t index;
            Column::scan(transaction, readState, offsetInGroup, offsetInGroup + 1,
                (uint8_t*)&index);
            offsetsToScan.emplace_back(index, pos);
        }
    }
    if (offsetsToScan.size() == 0) {
        // All scanned values are null
        return;
    }
    dictionary.scan(transaction,
        readState.childrenStates[DictionaryColumn::OFFSET_COLUMN_CHILD_READ_STATE_IDX],
        readState.childrenStates[DictionaryColumn::DATA_COLUMN_CHILD_READ_STATE_IDX], offsetsToScan,
        resultVector, readState.metadata);
}

void StringColumn::lookupInternal(Transaction* transaction, ReadState& readState,
    ValueVector* nodeIDVector, ValueVector* resultVector) {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::STRING);
    std::vector<std::pair<string_index_t, uint64_t>> offsetsToScan;
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        if (!nodeIDVector->isNull(pos)) {
            auto offsetInGroup = nodeIDVector->readNodeOffset(pos) -
                                 StorageUtils::getStartOffsetOfNodeGroup(readState.nodeGroupIdx);
            string_index_t index;
            Column::scan(transaction, readState, offsetInGroup, offsetInGroup + 1,
                (uint8_t*)&index);
            offsetsToScan.emplace_back(index, pos);
        }
    }
    if (offsetsToScan.size() == 0) {
        // All scanned values are null
        return;
    }
    dictionary.scan(transaction,
        readState.childrenStates[DictionaryColumn::OFFSET_COLUMN_CHILD_READ_STATE_IDX],
        readState.childrenStates[DictionaryColumn::DATA_COLUMN_CHILD_READ_STATE_IDX], offsetsToScan,
        resultVector, readState.metadata);
}

bool StringColumn::canCommitInPlace(transaction::Transaction* transaction,
    node_group_idx_t nodeGroupIdx, const ChunkCollection& localInsertChunks,
    const offset_to_row_idx_t& insertInfo, const ChunkCollection& localUpdateChunks,
    const offset_to_row_idx_t& updateInfo) {
    auto strLenToAdd = 0u;
    for (auto& [_, rowIdx] : updateInfo) {
        auto [chunkIdx, offsetInLocalChunk] =
            LocalChunkedGroupCollection::getChunkIdxAndOffsetInChunk(rowIdx);
        auto localUpdateChunk =
            ku_dynamic_cast<ColumnChunk*, StringColumnChunk*>(localUpdateChunks[chunkIdx]);
        strLenToAdd += localUpdateChunk->getStringLength(offsetInLocalChunk);
    }
    offset_t maxOffset = 0u;
    for (auto& [offset, rowIdx] : insertInfo) {
        if (offset > maxOffset) {
            maxOffset = offset;
        }
        auto [chunkIdx, offsetInLocalChunk] =
            LocalChunkedGroupCollection::getChunkIdxAndOffsetInChunk(rowIdx);
        auto localInsertChunk =
            ku_dynamic_cast<ColumnChunk*, StringColumnChunk*>(localInsertChunks[chunkIdx]);
        strLenToAdd += localInsertChunk->getStringLength(offsetInLocalChunk);
    }
    auto numStrings = insertInfo.size() + updateInfo.size();
    if (!dictionary.canCommitInPlace(transaction, nodeGroupIdx, numStrings, strLenToAdd)) {
        return false;
    }
    return canIndexCommitInPlace(transaction, nodeGroupIdx, numStrings, maxOffset);
}

bool StringColumn::canCommitInPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    const std::vector<offset_t>& dstOffsets, ColumnChunk* chunk, offset_t srcOffset) {
    auto strLenToAdd = 0u;
    auto strChunk = ku_dynamic_cast<ColumnChunk*, StringColumnChunk*>(chunk);
    auto length = std::min((uint64_t)dstOffsets.size(), strChunk->getNumValues());
    for (auto i = 0u; i < length; i++) {
        if (strChunk->getNullChunk()->isNull(i)) {
            continue;
        }
        strLenToAdd += strChunk->getStringLength(i + srcOffset);
    }
    auto numStrings = dstOffsets.size();
    if (!dictionary.canCommitInPlace(transaction, nodeGroupIdx, numStrings, strLenToAdd)) {
        return false;
    }
    auto maxDstOffset = getMaxOffset(dstOffsets);
    return canIndexCommitInPlace(transaction, nodeGroupIdx, numStrings, maxDstOffset);
}

bool StringColumn::canIndexCommitInPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    uint64_t numStrings, offset_t maxOffset) {
    auto metadata = getMetadata(nodeGroupIdx, transaction->getType());
    if (isMaxOffsetOutOfPagesCapacity(metadata, maxOffset)) {
        return false;
    }
    if (metadata.compMeta.canAlwaysUpdateInPlace()) {
        return true;
    }
    auto totalStringsAfterUpdate =
        dictionary.getNumValuesInOffsets(transaction, nodeGroupIdx) + numStrings;
    // Check if the index column can store the largest new index in-place
    if (!metadata.compMeta.canUpdateInPlace((const uint8_t*)&totalStringsAfterUpdate, 0 /*pos*/,
            PhysicalTypeID::UINT32)) {
        return false;
    }
    return true;
}

void StringColumn::prepareCommit() {
    Column::prepareCommit();
    dictionary.prepareCommit();
}

void StringColumn::checkpointInMemory() {
    Column::checkpointInMemory();
    dictionary.checkpointInMemory();
}

void StringColumn::rollbackInMemory() {
    Column::rollbackInMemory();
    dictionary.rollbackInMemory();
}

} // namespace storage
} // namespace kuzu

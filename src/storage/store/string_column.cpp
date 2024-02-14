#include "storage/store/string_column.h"

#include "storage/store/null_column.h"
#include "storage/store/string_column_chunk.h"

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

void StringColumn::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    nullColumn->scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector,
        offsetInVector);
    scanUnfiltered(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup, resultVector,
        offsetInVector);
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
    // Write index to main column
    Column::writeValues(chunkMeta, nodeGroupIdx, offsetInChunk, (uint8_t*)&index);
}

void StringColumn::write(node_group_idx_t nodeGroupIdx, offset_t dstOffset, ColumnChunk* data,
    offset_t srcOffset, length_t numValues) {
    auto chunkMeta = metadataDA->get(nodeGroupIdx, TransactionType::WRITE);
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
    // Write index to main column
    Column::writeValues(chunkMeta, nodeGroupIdx, dstOffset,
        reinterpret_cast<const uint8_t*>(&indices[0]), 0 /*srcOffset*/, numValues);
    if (dstOffset + numValues > chunkMeta.numValues) {
        chunkMeta.numValues = dstOffset + numValues;
        metadataDA->update(nodeGroupIdx, chunkMeta);
    }
}

void StringColumn::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    KU_ASSERT(resultVector->dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    KU_ASSERT(startNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(startNodeOffset);
    auto startOffsetInGroup =
        startNodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    if (nodeIDVector->state->selVector->isUnfiltered()) {
        scanUnfiltered(transaction, nodeGroupIdx, startOffsetInGroup,
            startOffsetInGroup + nodeIDVector->state->selVector->selectedSize, resultVector);
    } else {
        scanFiltered(transaction, nodeGroupIdx, startOffsetInGroup, nodeIDVector, resultVector);
    }
}

void StringColumn::scanUnfiltered(transaction::Transaction* transaction,
    node_group_idx_t nodeGroupIdx, offset_t startOffsetInGroup, offset_t endOffsetInGroup,
    ValueVector* resultVector, sel_t startPosInVector) {
    auto numValuesToRead = endOffsetInGroup - startOffsetInGroup;
    auto indices = std::make_unique<string_index_t[]>(numValuesToRead);
    auto indexState = getReadState(transaction->getType(), nodeGroupIdx);
    Column::scan(
        transaction, indexState, startOffsetInGroup, endOffsetInGroup, (uint8_t*)indices.get());

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
    dictionary.scan(transaction, nodeGroupIdx, offsetsToScan, resultVector, indexState.metadata);
}

void StringColumn::scanFiltered(transaction::Transaction* transaction,
    node_group_idx_t nodeGroupIdx, offset_t startOffsetInGroup, ValueVector* nodeIDVector,
    ValueVector* resultVector) {
    auto indexState = getReadState(transaction->getType(), nodeGroupIdx);

    std::vector<std::pair<string_index_t, uint64_t>> offsetsToScan;
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        if (!resultVector->isNull(pos)) {
            // TODO(bmwinger): optimize index scans by grouping them when adjacent
            auto offsetInGroup = startOffsetInGroup + pos;
            string_index_t index;
            Column::scan(
                transaction, indexState, offsetInGroup, offsetInGroup + 1, (uint8_t*)&index);
            offsetsToScan.emplace_back(index, pos);
        }
    }
    if (offsetsToScan.size() == 0) {
        // All scanned values are null
        return;
    }
    dictionary.scan(transaction, nodeGroupIdx, offsetsToScan, resultVector, indexState.metadata);
}

void StringColumn::lookupInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    KU_ASSERT(dataType.getPhysicalType() == PhysicalTypeID::STRING);
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(startNodeOffset);

    auto indexState = getReadState(transaction->getType(), nodeGroupIdx);
    std::vector<std::pair<string_index_t, uint64_t>> offsetsToScan;
    for (auto i = 0u; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        if (!nodeIDVector->isNull(pos)) {
            auto offsetInGroup = nodeIDVector->readNodeOffset(pos) -
                                 StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
            string_index_t index;
            Column::scan(
                transaction, indexState, offsetInGroup, offsetInGroup + 1, (uint8_t*)&index);
            offsetsToScan.emplace_back(index, pos);
        }
    }
    if (offsetsToScan.size() == 0) {
        // All scanned values are null
        return;
    }
    dictionary.scan(transaction, nodeGroupIdx, offsetsToScan, resultVector, indexState.metadata);
}

bool StringColumn::canCommitInPlace(transaction::Transaction* transaction,
    node_group_idx_t nodeGroupIdx, LocalVectorCollection* localChunk,
    const offset_to_row_idx_t& insertInfo, const offset_to_row_idx_t& updateInfo) {
    std::vector<row_idx_t> rowIdxesToRead;
    for (auto& [offset, rowIdx] : updateInfo) {
        rowIdxesToRead.push_back(rowIdx);
    }
    offset_t maxOffset = 0u;
    for (auto& [offset, rowIdx] : insertInfo) {
        rowIdxesToRead.push_back(rowIdx);
        if (offset > maxOffset) {
            maxOffset = offset;
        }
    }
    std::sort(rowIdxesToRead.begin(), rowIdxesToRead.end());
    auto strLenToAdd = 0u;
    for (auto rowIdx : rowIdxesToRead) {
        auto localVector = localChunk->getLocalVector(rowIdx);
        auto offsetInVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);
        auto kuStr = localVector->getVector()->getValue<ku_string_t>(offsetInVector);
        strLenToAdd += kuStr.len;
    }
    auto numStrings = rowIdxesToRead.size();
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
    if (!metadata.compMeta.canUpdateInPlace(
            (const uint8_t*)&totalStringsAfterUpdate, 0 /*pos*/, dataType.getPhysicalType())) {
        return false;
    }
    return true;
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

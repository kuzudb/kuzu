#include "storage/store/list_column.h"

#include "common/assert.h"
#include "storage/storage_structure/disk_array_collection.h"
#include "storage/store/column.h"
#include "storage/store/column_chunk_data.h"
#include "storage/store/list_chunk_data.h"
#include "storage/store/null_column.h"
#include <bit>

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

offset_t ListOffsetSizeInfo::getListStartOffset(uint64_t pos) const {
    if (numTotal == 0) {
        return 0;
    }
    return pos == numTotal ? getListEndOffset(pos - 1) : getListEndOffset(pos) - getListSize(pos);
}

offset_t ListOffsetSizeInfo::getListEndOffset(uint64_t pos) const {
    if (numTotal == 0) {
        return 0;
    }
    KU_ASSERT(pos < offsetColumnChunk->getNumValues());
    return offsetColumnChunk->getValue<offset_t>(pos);
}

list_size_t ListOffsetSizeInfo::getListSize(uint64_t pos) const {
    if (numTotal == 0) {
        return 0;
    }
    KU_ASSERT(pos < sizeColumnChunk->getNumValues());
    return sizeColumnChunk->getValue<list_size_t>(pos);
}

bool ListOffsetSizeInfo::isOffsetSortedAscending(uint64_t startPos, uint64_t endPos) const {
    offset_t prevEndOffset = getListStartOffset(startPos);
    for (auto i = startPos; i < endPos; i++) {
        offset_t currentEndOffset = getListEndOffset(i);
        auto size = getListSize(i);
        prevEndOffset += size;
        if (currentEndOffset != prevEndOffset) {
            return false;
        }
    }
    return true;
}

ListColumn::ListColumn(std::string name, LogicalType dataType,
    const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, DiskArrayCollection& metadataDAC,
    BufferManager* bufferManager, WAL* wal, Transaction* transaction, bool enableCompression)
    : Column{name, std::move(dataType), metaDAHeaderInfo, dataFH, metadataDAC, bufferManager, wal,
          transaction, enableCompression, true /* requireNullColumn */} {
    auto offsetColName =
        StorageUtils::getColumnName(name, StorageUtils::ColumnType::OFFSET, "offset_");
    auto sizeColName = StorageUtils::getColumnName(name, StorageUtils::ColumnType::OFFSET, "size_");
    auto dataColName = StorageUtils::getColumnName(name, StorageUtils::ColumnType::DATA, "");
    sizeColumn = std::make_unique<Column>(sizeColName, LogicalType::UINT32(),
        *metaDAHeaderInfo.childrenInfos[SIZE_COLUMN_CHILD_READ_STATE_IDX], dataFH, metadataDAC,
        bufferManager, wal, transaction, enableCompression, false /*requireNullColumn*/);
    dataColumn =
        ColumnFactory::createColumn(dataColName, ListType::getChildType(this->dataType).copy(),
            *metaDAHeaderInfo.childrenInfos[DATA_COLUMN_CHILD_READ_STATE_IDX], dataFH, metadataDAC,
            bufferManager, wal, transaction, enableCompression);
    offsetColumn = std::make_unique<Column>(offsetColName, LogicalType::UINT64(),
        *metaDAHeaderInfo.childrenInfos[OFFSET_COLUMN_CHILD_READ_STATE_IDX], dataFH, metadataDAC,
        bufferManager, wal, transaction, enableCompression, false /*requireNullColumn*/);
}

void ListColumn::initChunkState(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    ChunkState& readState) {
    Column::initChunkState(transaction, nodeGroupIdx, readState);
    // We put states for size and data column into childrenStates.
    readState.childrenStates.resize(CHILD_COLUMN_COUNT);
    sizeColumn->initChunkState(transaction, nodeGroupIdx,
        readState.childrenStates[SIZE_COLUMN_CHILD_READ_STATE_IDX]);
    dataColumn->initChunkState(transaction, nodeGroupIdx,
        readState.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX]);
    offsetColumn->initChunkState(transaction, nodeGroupIdx,
        readState.childrenStates[OFFSET_COLUMN_CHILD_READ_STATE_IDX]);
    KU_ASSERT(readState.metadata.numValues ==
              readState.childrenStates[OFFSET_COLUMN_CHILD_READ_STATE_IDX].metadata.numValues);
    KU_ASSERT(readState.metadata.numValues ==
              readState.childrenStates[SIZE_COLUMN_CHILD_READ_STATE_IDX].metadata.numValues);
}

void ListColumn::scan(Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    nullColumn->scan(transaction, *state.nullState, startOffsetInGroup, endOffsetInGroup,
        resultVector, offsetInVector);
    auto listOffsetInfoInStorage =
        getListOffsetSizeInfo(transaction, state, startOffsetInGroup, endOffsetInGroup);
    offset_t listOffsetInVector =
        offsetInVector == 0 ? 0 :
                              resultVector->getValue<list_entry_t>(offsetInVector - 1).offset +
                                  resultVector->getValue<list_entry_t>(offsetInVector - 1).size;
    auto offsetToWriteListData = listOffsetInVector;
    auto numValues = endOffsetInGroup - startOffsetInGroup;
    numValues = std::min(numValues, listOffsetInfoInStorage.numTotal);
    KU_ASSERT(endOffsetInGroup >= startOffsetInGroup);
    for (auto i = 0u; i < numValues; i++) {
        list_size_t size = listOffsetInfoInStorage.getListSize(i);
        resultVector->setValue(i + offsetInVector, list_entry_t{listOffsetInVector, size});
        listOffsetInVector += size;
    }
    ListVector::resizeDataVector(resultVector, listOffsetInVector);
    auto dataVector = ListVector::getDataVector(resultVector);
    bool isOffsetSortedAscending = listOffsetInfoInStorage.isOffsetSortedAscending(0, numValues);
    if (isOffsetSortedAscending) {
        dataColumn->scan(transaction, state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX],
            listOffsetInfoInStorage.getListStartOffset(0),
            listOffsetInfoInStorage.getListStartOffset(numValues), dataVector,
            offsetToWriteListData);
    } else {
        for (auto i = 0u; i < numValues; i++) {
            offset_t startOffset = listOffsetInfoInStorage.getListStartOffset(i);
            offset_t appendSize = listOffsetInfoInStorage.getListSize(i);
            dataColumn->scan(transaction, state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX],
                startOffset, startOffset + appendSize, dataVector, offsetToWriteListData);
            offsetToWriteListData += appendSize;
        }
    }
}

void ListColumn::scan(Transaction* transaction, const ChunkState& state,
    ColumnChunkData* columnChunk, offset_t startOffset, offset_t endOffset) {
    Column::scan(transaction, state, columnChunk, startOffset, endOffset);
    if (columnChunk->getNumValues() == 0) {
        return;
    }

    auto& listColumnChunk = columnChunk->cast<ListChunkData>();
    offsetColumn->scan(transaction, state.childrenStates[OFFSET_COLUMN_CHILD_READ_STATE_IDX],
        listColumnChunk.getOffsetColumnChunk(), startOffset, endOffset);
    sizeColumn->scan(transaction, state.childrenStates[SIZE_COLUMN_CHILD_READ_STATE_IDX],
        listColumnChunk.getSizeColumnChunk(), startOffset, endOffset);
    auto resizeNumValues = listColumnChunk.getDataColumnChunk()->getNumValues();
    bool isOffsetSortedAscending = true;
    offset_t prevOffset = listColumnChunk.getListStartOffset(0);
    for (auto i = 0u; i < columnChunk->getNumValues(); i++) {
        auto currentEndOffset = listColumnChunk.getListEndOffset(i);
        auto appendSize = listColumnChunk.getListSize(i);
        prevOffset += appendSize;
        if (currentEndOffset != prevOffset) {
            isOffsetSortedAscending = false;
        }
        resizeNumValues += appendSize;
    }
    if (isOffsetSortedAscending) {
        listColumnChunk.resizeDataColumnChunk(std::bit_ceil(resizeNumValues));
        offset_t startListOffset = listColumnChunk.getListStartOffset(0);
        offset_t endListOffset = listColumnChunk.getListStartOffset(columnChunk->getNumValues());
        dataColumn->scan(transaction, state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX],
            listColumnChunk.getDataColumnChunk(), startListOffset, endListOffset);
        listColumnChunk.resetOffset();
    } else {
        listColumnChunk.resizeDataColumnChunk(std::bit_ceil(resizeNumValues));
        auto tmpDataColumnChunk =
            ColumnChunkFactory::createColumnChunkData(ListType::getChildType(this->dataType).copy(),
                enableCompression, std::bit_ceil(resizeNumValues));
        auto* dataListColumnChunk = listColumnChunk.getDataColumnChunk();
        for (auto i = 0u; i < columnChunk->getNumValues(); i++) {
            offset_t startListOffset = listColumnChunk.getListStartOffset(i);
            offset_t endListOffset = listColumnChunk.getListEndOffset(i);
            dataColumn->scan(transaction, state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX],
                tmpDataColumnChunk.get(), startListOffset, endListOffset);
            KU_ASSERT(endListOffset - startListOffset == tmpDataColumnChunk->getNumValues());
            dataListColumnChunk->append(tmpDataColumnChunk.get(), 0,
                tmpDataColumnChunk->getNumValues());
        }
        listColumnChunk.resetOffset();
    }
}

void ListColumn::scanInternal(Transaction* transaction, const ChunkState& state, idx_t vectorIdx,
    row_idx_t numValuesToScan, ValueVector* nodeIDVector, ValueVector* resultVector) {
    KU_ASSERT(resultVector->state);
    auto startOffsetInChunk = vectorIdx * DEFAULT_VECTOR_CAPACITY;
    auto listOffsetSizeInfo = getListOffsetSizeInfo(transaction, state, startOffsetInChunk,
        startOffsetInChunk + numValuesToScan);
    if (nodeIDVector->state->getSelVector().isUnfiltered()) {
        scanUnfiltered(transaction, state, resultVector, numValuesToScan, listOffsetSizeInfo);
    } else {
        scanFiltered(transaction, state, resultVector, listOffsetSizeInfo);
    }
}

void ListColumn::lookupValue(Transaction* transaction, ChunkState& readState, offset_t nodeOffset,
    ValueVector* resultVector, uint32_t posInVector) {
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    KU_ASSERT(readState.nodeGroupIdx == nodeGroupIdx);
    auto nodeOffsetInGroup = nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    auto listEndOffset = readOffset(transaction, readState, nodeOffsetInGroup);
    auto size = readSize(transaction, readState, nodeOffsetInGroup);
    auto listStartOffset = listEndOffset - size;
    auto offsetInVector = posInVector == 0 ? 0 : resultVector->getValue<offset_t>(posInVector - 1);
    resultVector->setValue(posInVector, list_entry_t{offsetInVector, size});
    ListVector::resizeDataVector(resultVector, offsetInVector + size);
    auto dataVector = ListVector::getDataVector(resultVector);
    dataColumn->scan(transaction, readState.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX],
        listStartOffset, listEndOffset, dataVector, offsetInVector);
}

void ListColumn::append(ColumnChunkData* columnChunk, ChunkState& state) {
    KU_ASSERT(columnChunk->getDataType().getPhysicalType() == dataType.getPhysicalType());
    auto& listColumnChunk = columnChunk->cast<ListChunkData>();
    Column::append(columnChunk, state);

    auto* offsetColumnChunk = listColumnChunk.getOffsetColumnChunk();
    offsetColumn->append(offsetColumnChunk,
        state.childrenStates[OFFSET_COLUMN_CHILD_READ_STATE_IDX]);
    auto* sizeColumnChunk = listColumnChunk.getSizeColumnChunk();
    sizeColumn->append(sizeColumnChunk, state.childrenStates[SIZE_COLUMN_CHILD_READ_STATE_IDX]);
    auto* dataColumnChunk = listColumnChunk.getDataColumnChunk();
    dataColumn->append(dataColumnChunk, state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX]);
}

void ListColumn::scanUnfiltered(Transaction* transaction, const ChunkState& state,
    ValueVector* resultVector, uint64_t numValuesToScan,
    const ListOffsetSizeInfo& listOffsetInfoInStorage) const {
    numValuesToScan = std::min(numValuesToScan, listOffsetInfoInStorage.numTotal);
    offset_t offsetInVector = 0;
    for (auto i = 0u; i < numValuesToScan; i++) {
        auto listLen = listOffsetInfoInStorage.getListSize(i);
        resultVector->setValue(i, list_entry_t{offsetInVector, listLen});
        offsetInVector += listLen;
    }
    ListVector::resizeDataVector(resultVector, offsetInVector);
    auto dataVector = ListVector::getDataVector(resultVector);
    offsetInVector = 0;
    const bool checkOffsetOrder =
        listOffsetInfoInStorage.isOffsetSortedAscending(0, numValuesToScan);
    if (checkOffsetOrder) {
        auto startListOffsetInStorage = listOffsetInfoInStorage.getListStartOffset(0);
        numValuesToScan = numValuesToScan == 0 ? 0 : numValuesToScan - 1;
        auto endListOffsetInStorage = listOffsetInfoInStorage.getListEndOffset(numValuesToScan);
        dataColumn->scan(transaction, state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX],
            startListOffsetInStorage, endListOffsetInStorage, dataVector, 0 /* offsetInVector */);
    } else {
        for (auto i = 0u; i < numValuesToScan; i++) {
            // Nulls are scanned to the resultVector first
            if (!resultVector->isNull(i)) {
                auto startListOffsetInStorage = listOffsetInfoInStorage.getListStartOffset(i);
                auto appendSize = listOffsetInfoInStorage.getListSize(i);
                dataColumn->scan(transaction,
                    state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX],
                    startListOffsetInStorage, startListOffsetInStorage + appendSize, dataVector,
                    offsetInVector);
                offsetInVector += appendSize;
            }
        }
    }
}

void ListColumn::scanFiltered(Transaction* transaction, const ChunkState& state,
    ValueVector* resultVector, const ListOffsetSizeInfo& listOffsetSizeInfo) const {
    offset_t listOffset = 0;
    for (auto i = 0u; i < resultVector->state->getSelVector().getSelSize(); i++) {
        auto pos = resultVector->state->getSelVector()[i];
        auto listSize = listOffsetSizeInfo.getListSize(pos);
        resultVector->setValue(pos, list_entry_t{(offset_t)listOffset, listSize});
        listOffset += listSize;
    }
    ListVector::resizeDataVector(resultVector, listOffset);
    listOffset = 0;
    for (auto i = 0u; i < resultVector->state->getSelVector().getSelSize(); i++) {
        auto pos = resultVector->state->getSelVector()[i];
        // Nulls are scanned to the resultVector first
        if (!resultVector->isNull(pos)) {
            auto startOffsetInStorageToScan = listOffsetSizeInfo.getListStartOffset(pos);
            auto appendSize = listOffsetSizeInfo.getListSize(pos);
            auto dataVector = ListVector::getDataVector(resultVector);
            dataColumn->scan(transaction, state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX],
                startOffsetInStorageToScan, startOffsetInStorageToScan + appendSize, dataVector,
                listOffset);
            listOffset += resultVector->getValue<list_entry_t>(pos).size;
        }
    }
}

void ListColumn::prepareCommit() {
    Column::prepareCommit();
    sizeColumn->prepareCommit();
    dataColumn->prepareCommit();
    offsetColumn->prepareCommit();
}

void ListColumn::checkpointInMemory() {
    Column::checkpointInMemory();
    sizeColumn->checkpointInMemory();
    dataColumn->checkpointInMemory();
    offsetColumn->checkpointInMemory();
}

void ListColumn::rollbackInMemory() {
    Column::rollbackInMemory();
    sizeColumn->rollbackInMemory();
    dataColumn->rollbackInMemory();
    offsetColumn->rollbackInMemory();
}

offset_t ListColumn::readOffset(Transaction* transaction, const ChunkState& readState,
    offset_t offsetInNodeGroup) {
    const auto& offsetState = readState.childrenStates[OFFSET_COLUMN_CHILD_READ_STATE_IDX];
    auto pageCursor = offsetColumn->getPageCursorForOffsetInGroup(offsetInNodeGroup, offsetState);
    offset_t value;
    offsetColumn->readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
        offsetColumn->readToPageFunc(frame, pageCursor, (uint8_t*)&value, 0 /* posInVector */,
            1 /* numValuesToRead */, offsetState.metadata.compMeta);
    });
    return value;
}

list_size_t ListColumn::readSize(Transaction* transaction, const ChunkState& readState,
    offset_t offsetInNodeGroup) {
    const auto& sizeState = readState.childrenStates[SIZE_COLUMN_CHILD_READ_STATE_IDX];
    auto pageCursor = sizeColumn->getPageCursorForOffsetInGroup(offsetInNodeGroup, sizeState);
    offset_t value;
    sizeColumn->readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
        sizeColumn->readToPageFunc(frame, pageCursor, (uint8_t*)&value, 0 /* posInVector */,
            1 /* numValuesToRead */, sizeState.metadata.compMeta);
    });
    return value;
}

ListOffsetSizeInfo ListColumn::getListOffsetSizeInfo(Transaction* transaction,
    const ChunkState& state, offset_t startOffsetInNodeGroup, offset_t endOffsetInNodeGroup) {
    auto numOffsetsToRead = endOffsetInNodeGroup - startOffsetInNodeGroup;
    auto offsetColumnChunk = ColumnChunkFactory::createColumnChunkData(LogicalType::INT64(),
        enableCompression, numOffsetsToRead);
    auto sizeColumnChunk = ColumnChunkFactory::createColumnChunkData(LogicalType::UINT32(),
        enableCompression, numOffsetsToRead);
    offsetColumn->scan(transaction, state.childrenStates[OFFSET_COLUMN_CHILD_READ_STATE_IDX],
        offsetColumnChunk.get(), startOffsetInNodeGroup, endOffsetInNodeGroup);
    sizeColumn->scan(transaction, state.childrenStates[SIZE_COLUMN_CHILD_READ_STATE_IDX],
        sizeColumnChunk.get(), startOffsetInNodeGroup, endOffsetInNodeGroup);
    auto numValuesScan = offsetColumnChunk->getNumValues();
    return {numValuesScan, std::move(offsetColumnChunk), std::move(sizeColumnChunk)};
}

void ListColumn::prepareCommitForExistingChunk(Transaction* transaction, ChunkState& state,
    const ChunkCollection& localInsertChunks, const offset_to_row_idx_t& insertInfo,
    const ChunkCollection& localUpdateChunks, const offset_to_row_idx_t& updateInfo,
    const offset_set_t&) {
    // TODO: Should handle deletions.
    auto columnChunk = getEmptyChunkForCommit(updateInfo.size() + insertInfo.size());
    std::vector<offset_t> dstOffsets;
    for (auto& [offsetInDstChunk, rowIdx] : updateInfo) {
        auto [chunkIdx, offsetInLocalChunk] =
            LocalChunkedGroupCollection::getChunkIdxAndOffsetInChunk(rowIdx);
        auto localUpdateChunk = localUpdateChunks[chunkIdx];
        dstOffsets.push_back(offsetInDstChunk);
        columnChunk->append(localUpdateChunk, offsetInLocalChunk, 1);
    }
    for (auto& [offsetInDstChunk, rowIdx] : insertInfo) {
        auto [chunkIdx, offsetInLocalChunk] =
            LocalChunkedGroupCollection::getChunkIdxAndOffsetInChunk(rowIdx);
        auto localInsertChunk = localInsertChunks[chunkIdx];
        dstOffsets.push_back(offsetInDstChunk);
        columnChunk->append(localInsertChunk, offsetInLocalChunk, 1);
    }
    prepareCommitForExistingChunk(transaction, state, dstOffsets, columnChunk.get(),
        0 /*startSrcOffset*/);
}

void ListColumn::prepareCommitForExistingChunk(Transaction* transaction, ChunkState& state,
    const std::vector<offset_t>& dstOffsets, ColumnChunkData* chunk, offset_t startSrcOffset) {
    // we first check if we can in place commit data column chunk
    // if we can not in place commit data column chunk, we will out place commit offset/size/data
    // column chunk otherwise, we commit data column chunk in place and separately commit
    // offset/size column chunk
    auto& listChunk = chunk->cast<ListChunkData>();
    auto dataColumnSize = state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX].metadata.numValues;
    auto dataColumnChunk = listChunk.getDataColumnChunk();
    auto numListsToAppend = std::min(chunk->getNumValues(), (uint64_t)dstOffsets.size());
    auto dataSize = 0u;
    auto startListOffset = listChunk.getListStartOffset(startSrcOffset);
    std::vector<offset_t> dstOffsetsInDataColumn;
    for (auto i = 0u; i < numListsToAppend; i++) {
        for (auto j = 0u; j < listChunk.getListSize(startSrcOffset + i); j++) {
            dstOffsetsInDataColumn.push_back(dataColumnSize + dataSize++);
        }
    }
    bool dataColumnCanCommitInPlace =
        dataColumn->canCommitInPlace(state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX],
            dstOffsetsInDataColumn, dataColumnChunk, startListOffset);
    if (!dataColumnCanCommitInPlace) {
        commitColumnChunkOutOfPlace(transaction, state, false /*isNewNodeGroup*/, dstOffsets, chunk,
            startSrcOffset);
    } else {
        // TODO: Shouldn't here be in place commit?
        dataColumn->commitColumnChunkOutOfPlace(transaction,
            state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX], false /*isNewNodeGroup*/,
            dstOffsetsInDataColumn, dataColumnChunk, startListOffset);
        sizeColumn->prepareCommitForExistingChunk(transaction,
            state.childrenStates[SIZE_COLUMN_CHILD_READ_STATE_IDX], dstOffsets,
            listChunk.getSizeColumnChunk(), startSrcOffset);
        for (auto i = 0u; i < numListsToAppend; i++) {
            auto listEndOffset = listChunk.getListEndOffset(startSrcOffset + i);
            listChunk.getOffsetColumnChunk()->setValue<offset_t>(dataColumnSize + listEndOffset,
                startSrcOffset + i);
        }
        prepareCommitForOffsetChunk(transaction,
            state.childrenStates[OFFSET_COLUMN_CHILD_READ_STATE_IDX], dstOffsets, &listChunk,
            startSrcOffset);

        // since there is no data stored in the main column's buffer we only need to update the null
        // column
        nullColumn->prepareCommitForExistingChunk(transaction, *state.nullState, dstOffsets,
            chunk->getNullChunk(), startSrcOffset);
        if (state.metadata.numValues != state.nullState->metadata.numValues) {
            state.metadata.numValues = state.nullState->metadata.numValues;
            metadataDA->update(state.nodeGroupIdx, state.metadata);
        }
    }
}

void ListColumn::prepareCommitForOffsetChunk(Transaction* transaction, ChunkState& offsetState,
    const std::vector<offset_t>& dstOffsets, ColumnChunkData* chunk, offset_t startSrcOffset) {
    metadataDA->prepareCommit();

    auto& listChunk = chunk->cast<ListChunkData>();
    auto* offsetChunk = listChunk.getOffsetColumnChunk();
    if (offsetColumn->canCommitInPlace(offsetState, dstOffsets, offsetChunk, startSrcOffset)) {
        offsetColumn->prepareCommitForExistingChunkInPlace(transaction, offsetState, dstOffsets,
            offsetChunk, startSrcOffset);
    } else {
        commitOffsetColumnChunkOutOfPlace(transaction, offsetState, dstOffsets, chunk,
            startSrcOffset);
    }
}

void ListColumn::updateStateMetadataNumValues(ChunkState& state, size_t numValues) {
    state.metadata.numValues = numValues;
    state.childrenStates[SIZE_COLUMN_CHILD_READ_STATE_IDX].metadata.numValues = numValues;
    state.childrenStates[OFFSET_COLUMN_CHILD_READ_STATE_IDX].metadata.numValues = numValues;
}

void ListColumn::commitOffsetColumnChunkOutOfPlace(Transaction* transaction,
    ChunkState& offsetState, const std::vector<offset_t>& dstOffsets, ColumnChunkData* chunk,
    offset_t startSrcOffset) {
    auto offsetColumnChunk =
        ColumnChunkFactory::createColumnChunkData(offsetColumn->dataType.copy(), enableCompression,
            1.5 * std::bit_ceil(offsetState.metadata.numValues + dstOffsets.size()));
    offsetColumn->scan(transaction, offsetState, offsetColumnChunk.get());

    auto numListsToAppend = std::min(chunk->getNumValues(), (uint64_t)dstOffsets.size());
    auto& listChunk = chunk->cast<ListChunkData>();
    for (auto i = 0u; i < numListsToAppend; i++) {
        auto listEndOffset = listChunk.getListEndOffset(startSrcOffset + i);
        auto isNull = listChunk.getNullChunk()->isNull(startSrcOffset + i);
        offsetColumnChunk->setValue<offset_t>(listEndOffset, dstOffsets[i]);
        offsetColumnChunk->getNullChunk()->setNull(dstOffsets[i], isNull);
    }
    offsetColumn->append(offsetColumnChunk.get(), offsetState);
}

} // namespace storage
} // namespace kuzu

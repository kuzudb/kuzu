#include "storage/store/list_column.h"

#include "storage/store/column.h"
#include "storage/store/list_column_chunk.h"
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
    const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, Transaction* transaction,
    RWPropertyStats propertyStatistics, bool enableCompression)
    : Column{name, std::move(dataType), metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal,
          transaction, propertyStatistics, enableCompression, true /* requireNullColumn */} {
    auto sizeColName = StorageUtils::getColumnName(name, StorageUtils::ColumnType::OFFSET, "");
    auto dataColName = StorageUtils::getColumnName(name, StorageUtils::ColumnType::DATA, "");
    sizeColumn = ColumnFactory::createColumn(sizeColName, *LogicalType::UINT32(),
        *metaDAHeaderInfo.childrenInfos[0], dataFH, metadataFH, bufferManager, wal, transaction,
        propertyStatistics, enableCompression);
    dataColumn = ColumnFactory::createColumn(dataColName,
        *ListType::getChildType(this->dataType).copy(), *metaDAHeaderInfo.childrenInfos[1], dataFH,
        metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression);
}

void ListColumn::initChunkState(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    ChunkState& readState) {
    Column::initChunkState(transaction, nodeGroupIdx, readState);
    // We put states for size and data column into childrenStates.
    readState.childrenStates.resize(2);
    sizeColumn->initChunkState(transaction, nodeGroupIdx,
        readState.childrenStates[SIZE_COLUMN_CHILD_READ_STATE_IDX]);
    dataColumn->initChunkState(transaction, nodeGroupIdx,
        readState.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX]);
}

void ListColumn::scan(Transaction* transaction, ChunkState& readState, offset_t startOffsetInGroup,
    offset_t endOffsetInGroup, ValueVector* resultVector, uint64_t offsetInVector) {
    nullColumn->scan(transaction, *readState.nullState, startOffsetInGroup, endOffsetInGroup,
        resultVector, offsetInVector);
    auto listOffsetInfoInStorage = getListOffsetSizeInfo(transaction, readState.nodeGroupIdx,
        startOffsetInGroup, endOffsetInGroup);
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
        dataColumn->scan(transaction, readState.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX],
            listOffsetInfoInStorage.getListStartOffset(0),
            listOffsetInfoInStorage.getListStartOffset(numValues), dataVector,
            offsetToWriteListData);
    } else {
        for (auto i = 0u; i < numValues; i++) {
            offset_t startOffset = listOffsetInfoInStorage.getListStartOffset(i);
            offset_t appendSize = listOffsetInfoInStorage.getListSize(i);
            dataColumn->scan(transaction,
                readState.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX], startOffset,
                startOffset + appendSize, dataVector, offsetToWriteListData);
            offsetToWriteListData += appendSize;
        }
    }
}

void ListColumn::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    ColumnChunk* columnChunk, offset_t startOffset, offset_t endOffset) {
    if (nodeGroupIdx >= metadataDA->getNumElements(transaction->getType())) {
        columnChunk->setNumValues(0);
    } else {
        auto listColumnChunk = ku_dynamic_cast<ColumnChunk*, ListColumnChunk*>(columnChunk);
        Column::scan(transaction, nodeGroupIdx, columnChunk, startOffset, endOffset);
        auto sizeColumnChunk = listColumnChunk->getSizeColumnChunk();
        // TODO: Should use readState here too.
        sizeColumn->scan(transaction, nodeGroupIdx, sizeColumnChunk, startOffset, endOffset);
        auto resizeNumValues = listColumnChunk->getDataColumnChunk()->getNumValues();
        bool isOffsetSortedAscending = true;
        offset_t prevOffset = listColumnChunk->getListStartOffset(0);
        for (auto i = 0u; i < columnChunk->getNumValues(); i++) {
            auto currentEndOffset = listColumnChunk->getListEndOffset(i);
            auto appendSize = listColumnChunk->getListSize(i);
            prevOffset += appendSize;
            if (currentEndOffset != prevOffset) {
                isOffsetSortedAscending = false;
            }
            resizeNumValues += appendSize;
        }
        if (isOffsetSortedAscending) {
            listColumnChunk->resizeDataColumnChunk(std::bit_ceil(resizeNumValues));
            offset_t startListOffset = listColumnChunk->getListStartOffset(0);
            offset_t endListOffset =
                listColumnChunk->getListStartOffset(columnChunk->getNumValues());
            // TODO: Should use readState here too.
            dataColumn->scan(transaction, nodeGroupIdx, listColumnChunk->getDataColumnChunk(),
                startListOffset, endListOffset);
            listColumnChunk->resetOffset();
        } else {
            listColumnChunk->resizeDataColumnChunk(std::bit_ceil(resizeNumValues));
            auto tmpDataColumnChunk =
                std::make_unique<ListDataColumnChunk>(ColumnChunkFactory::createColumnChunk(
                    *ListType::getChildType(this->dataType).copy(), enableCompression,
                    std::bit_ceil(resizeNumValues)));
            auto dataListColumnChunk = listColumnChunk->getDataColumnChunk();
            for (auto i = 0u; i < columnChunk->getNumValues(); i++) {
                offset_t startListOffset = listColumnChunk->getListStartOffset(i);
                offset_t endListOffset = listColumnChunk->getListEndOffset(i);
                // TODO: Should use readState here too.
                dataColumn->scan(transaction, nodeGroupIdx,
                    tmpDataColumnChunk->dataColumnChunk.get(), startListOffset, endListOffset);
                KU_ASSERT(endListOffset - startListOffset ==
                          tmpDataColumnChunk->dataColumnChunk->getNumValues());
                dataListColumnChunk->append(tmpDataColumnChunk->dataColumnChunk.get(), 0,
                    tmpDataColumnChunk->dataColumnChunk->getNumValues());
            }
            listColumnChunk->resetOffset();
        }
    }
}

void ListColumn::scanInternal(Transaction* transaction, ChunkState& readState,
    ValueVector* nodeIDVector, ValueVector* resultVector) {
    KU_ASSERT(resultVector->state);
    auto [nodeGroupIdx, startOffsetInChunk] =
        StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeIDVector->readNodeOffset(0));
    auto listOffsetSizeInfo = getListOffsetSizeInfo(transaction, readState.nodeGroupIdx,
        startOffsetInChunk, startOffsetInChunk + nodeIDVector->state->getOriginalSize());
    if (resultVector->state->getSelVector().isUnfiltered()) {
        scanUnfiltered(transaction, readState, resultVector, listOffsetSizeInfo);
    } else {
        scanFiltered(transaction, readState, resultVector, listOffsetSizeInfo);
    }
}

void ListColumn::lookupValue(Transaction* transaction, ChunkState& readState, offset_t nodeOffset,
    ValueVector* resultVector, uint32_t posInVector) {
    auto nodeGroupIdx = StorageUtils::getNodeGroupIdx(nodeOffset);
    KU_ASSERT(readState.nodeGroupIdx == nodeGroupIdx);
    auto nodeOffsetInGroup = nodeOffset - StorageUtils::getStartOffsetOfNodeGroup(nodeGroupIdx);
    auto listEndOffset = readOffset(transaction, readState, nodeOffsetInGroup);
    auto size = readSize(transaction, readState.childrenStates[SIZE_COLUMN_CHILD_READ_STATE_IDX],
        nodeOffsetInGroup);
    auto listStartOffset = listEndOffset - size;
    auto offsetInVector = posInVector == 0 ? 0 : resultVector->getValue<offset_t>(posInVector - 1);
    resultVector->setValue(posInVector, list_entry_t{offsetInVector, size});
    ListVector::resizeDataVector(resultVector, offsetInVector + size);
    auto dataVector = ListVector::getDataVector(resultVector);
    dataColumn->scan(transaction, readState.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX],
        listStartOffset, listEndOffset, dataVector, offsetInVector);
}

void ListColumn::append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) {
    KU_ASSERT(columnChunk->getDataType().getPhysicalType() == dataType.getPhysicalType());
    auto listColumnChunk = ku_dynamic_cast<ColumnChunk*, ListColumnChunk*>(columnChunk);
    Column::append(listColumnChunk, nodeGroupIdx);
    auto sizeColumnChunk = listColumnChunk->getSizeColumnChunk();
    sizeColumn->append(sizeColumnChunk, nodeGroupIdx);
    auto dataColumnChunk = listColumnChunk->getDataColumnChunk();
    dataColumn->append(dataColumnChunk, nodeGroupIdx);
}

void ListColumn::scanUnfiltered(Transaction* transaction, ChunkState& readState,
    ValueVector* resultVector, const ListOffsetSizeInfo& listOffsetInfoInStorage) {
    auto numValuesToScan = resultVector->state->getSelVector().getSelSize();
    numValuesToScan = std::min((uint64_t)numValuesToScan, listOffsetInfoInStorage.numTotal);
    offset_t offsetInVector = 0;
    for (auto i = 0u; i < numValuesToScan; i++) {
        auto listLen = listOffsetInfoInStorage.getListSize(i);
        resultVector->setValue(i, list_entry_t{offsetInVector, listLen});
        offsetInVector += listLen;
    }
    ListVector::resizeDataVector(resultVector, offsetInVector);
    auto dataVector = ListVector::getDataVector(resultVector);
    offsetInVector = 0;
    bool checkOffsetOrder = listOffsetInfoInStorage.isOffsetSortedAscending(0, numValuesToScan);
    if (checkOffsetOrder) {
        auto startListOffsetInStorage = listOffsetInfoInStorage.getListStartOffset(0);
        numValuesToScan = numValuesToScan == 0 ? 0 : numValuesToScan - 1;
        auto endListOffsetInStorage = listOffsetInfoInStorage.getListEndOffset(numValuesToScan);
        dataColumn->scan(transaction, readState.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX],
            startListOffsetInStorage, endListOffsetInStorage, dataVector, 0 /* offsetInVector */);
    } else {
        for (auto i = 0u; i < numValuesToScan; i++) {
            auto startListOffsetInStorage = listOffsetInfoInStorage.getListStartOffset(i);
            auto appendSize = listOffsetInfoInStorage.getListSize(i);
            dataColumn->scan(transaction,
                readState.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX],
                startListOffsetInStorage, startListOffsetInStorage + appendSize, dataVector,
                offsetInVector);
            offsetInVector += appendSize;
        }
    }
}

void ListColumn::scanFiltered(Transaction* transaction, ChunkState& readState,
    ValueVector* resultVector, const ListOffsetSizeInfo& listOffsetSizeInfo) {
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
        auto startOffsetInStorageToScan = listOffsetSizeInfo.getListStartOffset(pos);
        auto appendSize = listOffsetSizeInfo.getListSize(pos);
        auto dataVector = ListVector::getDataVector(resultVector);
        dataColumn->scan(transaction, readState.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX],
            startOffsetInStorageToScan, startOffsetInStorageToScan + appendSize, dataVector,
            listOffset);
        listOffset += resultVector->getValue<list_entry_t>(pos).size;
    }
}

void ListColumn::prepareCommit() {
    Column::prepareCommit();
    sizeColumn->prepareCommit();
    dataColumn->prepareCommit();
}

void ListColumn::checkpointInMemory() {
    Column::checkpointInMemory();
    sizeColumn->checkpointInMemory();
    dataColumn->checkpointInMemory();
}

void ListColumn::rollbackInMemory() {
    Column::rollbackInMemory();
    sizeColumn->rollbackInMemory();
    dataColumn->rollbackInMemory();
}

offset_t ListColumn::readOffset(Transaction* transaction, const ChunkState& readState,
    offset_t offsetInNodeGroup) {
    auto pageCursor = getPageCursorForOffsetInGroup(offsetInNodeGroup, readState);
    offset_t value;
    readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
        readToPageFunc(frame, pageCursor, (uint8_t*)&value, 0 /* posInVector */,
            1 /* numValuesToRead */, readState.metadata.compMeta);
    });
    return value;
}

list_size_t ListColumn::readSize(Transaction* transaction, const ChunkState& readState,
    offset_t offsetInNodeGroup) {
    auto pageCursor = getPageCursorForOffsetInGroup(offsetInNodeGroup, readState);
    offset_t value;
    readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
        readToPageFunc(frame, pageCursor, (uint8_t*)&value, 0 /* posInVector */,
            1 /* numValuesToRead */, readState.metadata.compMeta);
    });
    return value;
}

ListOffsetSizeInfo ListColumn::getListOffsetSizeInfo(Transaction* transaction,
    node_group_idx_t nodeGroupIdx, offset_t startOffsetInNodeGroup, offset_t endOffsetInNodeGroup) {
    auto numOffsetsToRead = endOffsetInNodeGroup - startOffsetInNodeGroup;
    auto offsetColumnChunk = ColumnChunkFactory::createColumnChunk(*common::LogicalType::INT64(),
        enableCompression, numOffsetsToRead);
    auto sizeColumnChunk = ColumnChunkFactory::createColumnChunk(*common::LogicalType::UINT32(),
        enableCompression, numOffsetsToRead);
    // TODO: Should use readState here too.
    Column::scan(transaction, nodeGroupIdx, offsetColumnChunk.get(), startOffsetInNodeGroup,
        endOffsetInNodeGroup);
    sizeColumn->scan(transaction, nodeGroupIdx, sizeColumnChunk.get(), startOffsetInNodeGroup,
        endOffsetInNodeGroup);
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
    const std::vector<common::offset_t>& dstOffsets, ColumnChunk* chunk, offset_t startSrcOffset) {
    // we first check if we can in place commit data column chunk
    // if we can not in place commit data column chunk, we will out place commit offset/size/data
    // column chunk otherwise, we commit data column chunk in place and separately commit
    // offset/size column chunk
    auto listChunk = ku_dynamic_cast<ColumnChunk*, ListColumnChunk*>(chunk);
    auto dataColumnSize = state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX].metadata.numValues;
    auto dataColumnChunk = listChunk->getDataColumnChunk();
    auto numListsToAppend = std::min(chunk->getNumValues(), (uint64_t)dstOffsets.size());
    auto dataSize = 0u;
    auto startListOffset = listChunk->getListStartOffset(startSrcOffset);
    std::vector<common::offset_t> dstOffsetsInDataColumn;
    for (auto i = 0u; i < numListsToAppend; i++) {
        for (auto j = 0u; j < listChunk->getListSize(startSrcOffset + i); j++) {
            dstOffsetsInDataColumn.push_back(dataColumnSize + dataSize++);
        }
    }
    bool dataColumnCanCommitInPlace =
        dataColumn->canCommitInPlace(state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX],
            dstOffsetsInDataColumn, dataColumnChunk, startListOffset);
    if (!dataColumnCanCommitInPlace) {
        commitColumnChunkOutOfPlace(transaction, state.nodeGroupIdx, false /*isNewNodeGroup*/,
            dstOffsets, chunk, startSrcOffset);
    } else {
        // TODO: Shouldn't here be in place commit?
        dataColumn->commitColumnChunkOutOfPlace(transaction, state.nodeGroupIdx,
            false /*isNewNodeGroup*/, dstOffsetsInDataColumn, dataColumnChunk, startListOffset);
        sizeColumn->prepareCommitForExistingChunk(transaction,
            state.childrenStates[SIZE_COLUMN_CHILD_READ_STATE_IDX], dstOffsets,
            listChunk->getSizeColumnChunk(), startSrcOffset);
        for (auto i = 0u; i < numListsToAppend; i++) {
            auto listEndOffset = listChunk->getListEndOffset(startSrcOffset + i);
            chunk->setValue<offset_t>(dataColumnSize + listEndOffset, startSrcOffset + i);
        }
        prepareCommitForOffsetChunk(transaction, state, dstOffsets, chunk, startSrcOffset);
    }
}

void ListColumn::prepareCommitForOffsetChunk(Transaction* transaction, ChunkState& offsetState,
    const std::vector<common::offset_t>& dstOffsets, ColumnChunk* chunk, offset_t startSrcOffset) {
    metadataDA->prepareCommit();
    if (canCommitInPlace(offsetState, dstOffsets, chunk, startSrcOffset)) {
        Column::commitColumnChunkInPlace(offsetState, dstOffsets, chunk, startSrcOffset);
        metadataDA->update(offsetState.nodeGroupIdx, offsetState.metadata);
        if (nullColumn) {
            nullColumn->prepareCommitForChunk(transaction, offsetState.nodeGroupIdx, false,
                dstOffsets, chunk->getNullChunk(), startSrcOffset);
        }
    } else {
        commitOffsetColumnChunkOutOfPlace(transaction, offsetState, dstOffsets, chunk,
            startSrcOffset);
    }
}

void ListColumn::commitOffsetColumnChunkOutOfPlace(Transaction* transaction,
    const ChunkState& offsetState, const std::vector<offset_t>& dstOffsets, ColumnChunk* chunk,
    offset_t startSrcOffset) {
    auto listChunk = ku_dynamic_cast<ColumnChunk*, ListColumnChunk*>(chunk);
    auto offsetColumnChunk = ColumnChunkFactory::createColumnChunk(*dataType.copy(),
        enableCompression, 1.5 * std::bit_ceil(offsetState.metadata.numValues + dstOffsets.size()));
    Column::scan(transaction, offsetState.nodeGroupIdx, offsetColumnChunk.get());
    auto numListsToAppend = std::min(chunk->getNumValues(), (uint64_t)dstOffsets.size());
    for (auto i = 0u; i < numListsToAppend; i++) {
        auto listEndOffset = listChunk->getListEndOffset(startSrcOffset + i);
        auto isNull = listChunk->getNullChunk()->isNull(startSrcOffset + i);
        offsetColumnChunk->setValue<offset_t>(listEndOffset, dstOffsets[i]);
        offsetColumnChunk->getNullChunk()->setNull(dstOffsets[i], isNull);
    }
    auto offsetListChunk = ku_dynamic_cast<ColumnChunk*, ListColumnChunk*>(offsetColumnChunk.get());
    offsetListChunk->getSizeColumnChunk()->setNumValues(offsetColumnChunk->getNumValues());
    Column::append(offsetColumnChunk.get(), offsetState.nodeGroupIdx);
}

} // namespace storage
} // namespace kuzu

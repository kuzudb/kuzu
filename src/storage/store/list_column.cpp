#include "storage/store/list_column.h"

#include <algorithm>

#include "common/assert.h"
#include "common/vector/value_vector.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/storage_structure/disk_array_collection.h"
#include "storage/store/column.h"
#include "storage/store/column_chunk.h"
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

ListColumn::ListColumn(std::string name, LogicalType dataType, FileHandle* dataFH,
    MemoryManager* mm, ShadowFile* shadowFile, bool enableCompression)
    : Column{name, std::move(dataType), dataFH, mm, shadowFile, enableCompression,
          true /* requireNullColumn */} {
    auto offsetColName =
        StorageUtils::getColumnName(name, StorageUtils::ColumnType::OFFSET, "offset_");
    auto sizeColName = StorageUtils::getColumnName(name, StorageUtils::ColumnType::OFFSET, "");
    auto dataColName = StorageUtils::getColumnName(name, StorageUtils::ColumnType::DATA, "");
    sizeColumn = std::make_unique<Column>(sizeColName, LogicalType::UINT32(), dataFH, mm,
        shadowFile, enableCompression, false /*requireNullColumn*/);
    dataColumn = ColumnFactory::createColumn(dataColName,
        ListType::getChildType(this->dataType).copy(), dataFH, mm, shadowFile, enableCompression);
    offsetColumn = std::make_unique<Column>(offsetColName, LogicalType::UINT64(), dataFH, mm,
        shadowFile, enableCompression, false /*requireNullColumn*/);
}

std::unique_ptr<ColumnChunkData> ListColumn::flushChunkData(const ColumnChunkData& chunk,
    FileHandle& dataFH) {
    auto flushedChunk = flushNonNestedChunkData(chunk, dataFH);
    auto& listChunk = chunk.cast<ListChunkData>();
    auto& flushedListChunk = flushedChunk->cast<ListChunkData>();
    flushedListChunk.setOffsetColumnChunk(
        Column::flushChunkData(*listChunk.getOffsetColumnChunk(), dataFH));
    flushedListChunk.setSizeColumnChunk(
        Column::flushChunkData(*listChunk.getSizeColumnChunk(), dataFH));
    flushedListChunk.setDataColumnChunk(
        Column::flushChunkData(*listChunk.getDataColumnChunk(), dataFH));
    return flushedChunk;
}

void ListColumn::scan(const Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) const {
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
        dataColumn->scan(transaction,
            state.childrenStates[ListChunkData::DATA_COLUMN_CHILD_READ_STATE_IDX],
            listOffsetInfoInStorage.getListStartOffset(0),
            listOffsetInfoInStorage.getListStartOffset(numValues), dataVector,
            offsetToWriteListData);
    } else {
        for (auto i = 0u; i < numValues; i++) {
            offset_t startOffset = listOffsetInfoInStorage.getListStartOffset(i);
            offset_t appendSize = listOffsetInfoInStorage.getListSize(i);
            dataColumn->scan(transaction,
                state.childrenStates[ListChunkData::DATA_COLUMN_CHILD_READ_STATE_IDX], startOffset,
                startOffset + appendSize, dataVector, offsetToWriteListData);
            offsetToWriteListData += appendSize;
        }
    }
}

void ListColumn::scan(const Transaction* transaction, const ChunkState& state,
    ColumnChunkData* columnChunk, offset_t startOffset, offset_t endOffset) const {
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
        auto tmpDataColumnChunk = ColumnChunkFactory::createColumnChunkData(*mm,
            ListType::getChildType(this->dataType).copy(), enableCompression,
            std::bit_ceil(resizeNumValues), ResidencyState::IN_MEMORY);
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

    KU_ASSERT(listColumnChunk.sanityCheck());
}

void ListColumn::scanInternal(Transaction* transaction, const ChunkState& state,
    offset_t startOffsetInChunk, row_idx_t numValuesToScan, ValueVector* resultVector) const {
    KU_ASSERT(resultVector->state);
    auto listOffsetSizeInfo = getListOffsetSizeInfo(transaction, state, startOffsetInChunk,
        startOffsetInChunk + numValuesToScan);
    if (resultVector->state->getSelVector().isUnfiltered()) {
        scanUnfiltered(transaction, state, resultVector, numValuesToScan, listOffsetSizeInfo);
    } else {
        scanFiltered(transaction, state, resultVector, listOffsetSizeInfo);
    }
}

void ListColumn::lookupInternal(const Transaction* transaction, const ChunkState& state,
    offset_t nodeOffset, ValueVector* resultVector, uint32_t posInVector) const {
    auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
    const auto listEndOffset = readOffset(transaction, state, offsetInChunk);
    const auto size = readSize(transaction, state, offsetInChunk);
    const auto listStartOffset = listEndOffset - size;
    const auto offsetInVector =
        posInVector == 0 ? 0 : resultVector->getValue<offset_t>(posInVector - 1);
    resultVector->setValue(posInVector, list_entry_t{offsetInVector, size});
    ListVector::resizeDataVector(resultVector, offsetInVector + size);
    const auto dataVector = ListVector::getDataVector(resultVector);
    dataColumn->scan(transaction,
        state.childrenStates[ListChunkData::DATA_COLUMN_CHILD_READ_STATE_IDX], listStartOffset,
        listEndOffset, dataVector, offsetInVector);
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
        dataColumn->scan(transaction,
            state.childrenStates[ListChunkData::DATA_COLUMN_CHILD_READ_STATE_IDX],
            startListOffsetInStorage, endListOffsetInStorage, dataVector,
            static_cast<uint64_t>(0 /* offsetInVector */));
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

offset_t ListColumn::readOffset(const Transaction* transaction, const ChunkState& readState,
    offset_t offsetInNodeGroup) const {
    offset_t ret = INVALID_OFFSET;
    const auto& offsetState = readState.childrenStates[OFFSET_COLUMN_CHILD_READ_STATE_IDX];
    offsetColumn->columnReadWriter->readCompressedValueToPage(transaction, offsetState,
        offsetInNodeGroup, reinterpret_cast<uint8_t*>(&ret), 0, offsetColumn->readToPageFunc);
    return ret;
}

list_size_t ListColumn::readSize(const Transaction* transaction, const ChunkState& readState,
    offset_t offsetInNodeGroup) const {
    const auto& sizeState = readState.childrenStates[SIZE_COLUMN_CHILD_READ_STATE_IDX];
    offset_t value = INVALID_OFFSET;
    sizeColumn->columnReadWriter->readCompressedValueToPage(transaction, sizeState,
        offsetInNodeGroup, reinterpret_cast<uint8_t*>(&value), 0, sizeColumn->readToPageFunc);
    return value;
}

ListOffsetSizeInfo ListColumn::getListOffsetSizeInfo(const Transaction* transaction,
    const ChunkState& state, offset_t startOffsetInNodeGroup, offset_t endOffsetInNodeGroup) const {
    const auto numOffsetsToRead = endOffsetInNodeGroup - startOffsetInNodeGroup;
    auto offsetColumnChunk = ColumnChunkFactory::createColumnChunkData(*mm, LogicalType::INT64(),
        enableCompression, numOffsetsToRead, ResidencyState::IN_MEMORY);
    auto sizeColumnChunk = ColumnChunkFactory::createColumnChunkData(*mm, LogicalType::UINT32(),
        enableCompression, numOffsetsToRead, ResidencyState::IN_MEMORY);
    offsetColumn->scan(transaction, state.childrenStates[OFFSET_COLUMN_CHILD_READ_STATE_IDX],
        offsetColumnChunk.get(), startOffsetInNodeGroup, endOffsetInNodeGroup);
    sizeColumn->scan(transaction, state.childrenStates[SIZE_COLUMN_CHILD_READ_STATE_IDX],
        sizeColumnChunk.get(), startOffsetInNodeGroup, endOffsetInNodeGroup);
    auto numValuesScan = offsetColumnChunk->getNumValues();
    return {numValuesScan, std::move(offsetColumnChunk), std::move(sizeColumnChunk)};
}

void ListColumn::checkpointColumnChunk(ColumnCheckpointState& checkpointState) {
    auto& persistentListChunk = checkpointState.persistentData.cast<ListChunkData>();
    const auto persistentDataChunk = persistentListChunk.getDataColumnChunk();
    // First, check if we can checkpoint list data chunk in place.
    const auto persistentListDataSize = persistentDataChunk->getNumValues();
    row_idx_t newListDataSize = persistentListDataSize;

    std::vector<const ChunkCheckpointState*> listDataNonEmptyChunkCheckpointStates;
    std::vector<ChunkCheckpointState> listDataChunkCheckpointStates;
    for (const auto& chunkCheckpointState : checkpointState.chunkCheckpointStates) {
        KU_ASSERT(chunkCheckpointState.chunkData->getNumValues() == chunkCheckpointState.numRows);
        const auto chunkData =
            chunkCheckpointState.chunkData->cast<ListChunkData>().getDataColumnChunk();
        const row_idx_t listDataSizeToAppend = chunkData->getNumValues();
        if (listDataSizeToAppend > 0) {
            listDataNonEmptyChunkCheckpointStates.push_back(&chunkCheckpointState);
            listDataChunkCheckpointStates.push_back(ChunkCheckpointState{
                chunkCheckpointState.chunkData->cast<ListChunkData>().moveDataColumnChunk(),
                newListDataSize, listDataSizeToAppend});
            newListDataSize += listDataSizeToAppend;
        }
    }

    ChunkState chunkState;
    checkpointState.persistentData.initializeScanState(chunkState, this);
    ColumnCheckpointState listDataCheckpointState(*persistentDataChunk,
        std::move(listDataChunkCheckpointStates));
    const auto listDataCanCheckpointInPlace = dataColumn->canCheckpointInPlace(
        chunkState.childrenStates[ListChunkData::DATA_COLUMN_CHILD_READ_STATE_IDX],
        listDataCheckpointState);
    if (!listDataCanCheckpointInPlace) {
        // If we cannot checkpoint list data chunk in place, we need to checkpoint the whole chunk
        // out of place.
        // Move list data chunks back to the original chunk in checkpointState.
        for (auto i = 0u; i < listDataNonEmptyChunkCheckpointStates.size(); i++) {
            const auto* chunkCheckpointState = listDataNonEmptyChunkCheckpointStates[i];
            chunkCheckpointState->chunkData->cast<ListChunkData>().setDataColumnChunk(
                std::move(listDataCheckpointState.chunkCheckpointStates[i].chunkData));
        }
        checkpointColumnChunkOutOfPlace(chunkState, checkpointState);
    } else {
        // In place checkpoint for list data.
        dataColumn->checkpointColumnChunkInPlace(
            chunkState.childrenStates[ListChunkData::DATA_COLUMN_CHILD_READ_STATE_IDX],
            listDataCheckpointState);

        // Checkpoint offset data.
        std::vector<ChunkCheckpointState> offsetChunkCheckpointStates;
        std::vector<std::unique_ptr<ColumnChunk>> offsetChunks;

        KU_ASSERT(std::is_sorted(checkpointState.chunkCheckpointStates.begin(),
            checkpointState.chunkCheckpointStates.end(),
            [](const auto& a, const auto& b) { return a.startRow < b.startRow; }));
        for (const auto& chunkCheckpointState : checkpointState.chunkCheckpointStates) {
            KU_ASSERT(
                chunkCheckpointState.chunkData->getNumValues() == chunkCheckpointState.numRows);
            offsetChunks.push_back(std::make_unique<ColumnChunk>(*mm, LogicalType::UINT64(),
                chunkCheckpointState.numRows, false, ResidencyState::IN_MEMORY));
            const auto& offsetsToWrite = offsetChunks.back();
            const auto& listChunk = chunkCheckpointState.chunkData->cast<ListChunkData>();
            for (auto i = 0u; i < chunkCheckpointState.numRows; i++) {
                offsetsToWrite->getData().setValue<offset_t>(
                    persistentListDataSize + listChunk.getListEndOffset(i), i);
            }
            offsetChunkCheckpointStates.push_back(ChunkCheckpointState{offsetsToWrite->moveData(),
                chunkCheckpointState.startRow, chunkCheckpointState.numRows});
        }

        ColumnCheckpointState offsetCheckpointState(*persistentListChunk.getOffsetColumnChunk(),
            std::move(offsetChunkCheckpointStates));
        offsetColumn->checkpointColumnChunk(offsetCheckpointState);

        // Checkpoint size data.
        std::vector<ChunkCheckpointState> sizeChunkCheckpointStates;
        for (const auto& chunkCheckpointState : checkpointState.chunkCheckpointStates) {
            sizeChunkCheckpointStates.push_back(ChunkCheckpointState{
                chunkCheckpointState.chunkData->cast<ListChunkData>().moveSizeColumnChunk(),
                chunkCheckpointState.startRow, chunkCheckpointState.numRows});
        }
        ColumnCheckpointState sizeCheckpointState(*persistentListChunk.getSizeColumnChunk(),
            std::move(sizeChunkCheckpointStates));
        sizeColumn->checkpointColumnChunk(sizeCheckpointState);
        // Checkpoint null data.
        Column::checkpointNullData(checkpointState);

        KU_ASSERT(persistentListChunk.getNullData()->getNumValues() ==
                      persistentListChunk.getOffsetColumnChunk()->getNumValues() &&
                  persistentListChunk.getNullData()->getNumValues() ==
                      persistentListChunk.getSizeColumnChunk()->getNumValues());

        persistentListChunk.syncNumValues();
    }
}

} // namespace storage
} // namespace kuzu

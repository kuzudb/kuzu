#include "storage/store/list_column.h"

#include <algorithm>

#include "common/assert.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/storage_utils.h"
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
    : Column{std::move(name), std::move(dataType), dataFH, mm, shadowFile, enableCompression,
          true /* requireNullColumn */} {
    auto offsetColName =
        StorageUtils::getColumnName(this->name, StorageUtils::ColumnType::OFFSET, "offset_");
    auto sizeColName =
        StorageUtils::getColumnName(this->name, StorageUtils::ColumnType::OFFSET, "");
    auto dataColName = StorageUtils::getColumnName(this->name, StorageUtils::ColumnType::DATA, "");
    sizeColumn = std::make_unique<Column>(sizeColName, LogicalType::UINT32(), dataFH, mm,
        shadowFile, enableCompression, false /*requireNullColumn*/);
    offsetColumn = std::make_unique<Column>(offsetColName, LogicalType::UINT64(), dataFH, mm,
        shadowFile, enableCompression, false /*requireNullColumn*/);
    if (disableCompressionOnData(this->dataType)) {
        enableCompression = false;
    }
    dataColumn = ColumnFactory::createColumn(dataColName,
        ListType::getChildType(this->dataType).copy(), dataFH, mm, shadowFile, enableCompression);
}

bool ListColumn::disableCompressionOnData(const LogicalType& dataType) {
    if (dataType.getLogicalTypeID() == LogicalTypeID::ARRAY &&
        (ListType::getChildType(dataType).getPhysicalType() == PhysicalTypeID::FLOAT ||
            ListType::getChildType(dataType).getPhysicalType() == PhysicalTypeID::DOUBLE)) {
        // Force disable compression for floating point types.
        return true;
    }
    return false;
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

void ListColumn::scanInternal(const Transaction* transaction, const SegmentState& state,
    offset_t startOffsetInChunk, row_idx_t numValuesToScan, ValueVector* resultVector,
    offset_t offsetInResult) const {
    auto listOffsetSizeInfo =
        getListOffsetSizeInfo(transaction, state, startOffsetInChunk, numValuesToScan);
    if (!resultVector->state || resultVector->state->getSelVector().isUnfiltered()) {
        scanUnfiltered(transaction, state, resultVector, numValuesToScan, listOffsetSizeInfo,
            offsetInResult);
    } else {
        scanFiltered(transaction, state, resultVector, listOffsetSizeInfo, offsetInResult);
    }
}

/* FIXME(bmwinger): why was there essentially two different implementations of the same function
(signatures were originally slightly different)?

void ListColumn::scanInternal(const Transaction* transaction, const SegmentState& state,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) const {
    nullColumn->scanInternal(transaction, *state.nullState, startOffsetInGroup, endOffsetInGroup,
        resultVector, offsetInVector);
    auto listOffsetInfoInStorage =
        getListOffsetSizeInfo(transaction, state, startOffsetInGroup, endOffsetInGroup -
startOffsetInGroup); offset_t listOffsetInVector = offsetInVector == 0 ? 0 :
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
        auto startOffset = listOffsetInfoInStorage.getListStartOffset(0);
        auto length = listOffsetInfoInStorage.getListStartOffset(numValues) - startOffset;
        dataColumn->scanInternal(transaction,
            state.childrenStates[ListChunkData::DATA_COLUMN_CHILD_READ_STATE_IDX], startOffset,
            length, dataVector, offsetToWriteListData);
    } else {
        for (auto i = 0u; i < numValues; i++) {
            offset_t startOffset = listOffsetInfoInStorage.getListStartOffset(i);
            offset_t appendSize = listOffsetInfoInStorage.getListSize(i);
            dataColumn->scanInternal(transaction,
                state.childrenStates[ListChunkData::DATA_COLUMN_CHILD_READ_STATE_IDX], startOffset,
                appendSize, dataVector, offsetToWriteListData);
            offsetToWriteListData += appendSize;
        }
    }
}
*/

void ListColumn::scanInternal(const transaction::Transaction* transaction,
    const SegmentState& state, common::offset_t startOffsetInSegment,
    common::row_idx_t numValuesToScan, ColumnChunkData* resultChunk,
    common::offset_t offsetInResult) const {
    Column::scanInternal(transaction, state, startOffsetInSegment, numValuesToScan, resultChunk,
        offsetInResult);
    if (resultChunk->getNumValues() == 0) {
        return;
    }

    auto& listColumnChunk = resultChunk->cast<ListChunkData>();
    offsetColumn->scanInternal(transaction,
        state.childrenStates[OFFSET_COLUMN_CHILD_READ_STATE_IDX], startOffsetInSegment,
        numValuesToScan, listColumnChunk.getOffsetColumnChunk(), offsetInResult);
    sizeColumn->scanInternal(transaction, state.childrenStates[SIZE_COLUMN_CHILD_READ_STATE_IDX],
        startOffsetInSegment, numValuesToScan, listColumnChunk.getSizeColumnChunk(),
        offsetInResult);
    auto resizeNumValues = listColumnChunk.getDataColumnChunk()->getNumValues();
    bool isOffsetSortedAscending = true;
    offset_t prevOffset = listColumnChunk.getListStartOffset(0);
    for (auto i = 0u; i < resultChunk->getNumValues(); i++) {
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
        offset_t endListOffset = listColumnChunk.getListStartOffset(resultChunk->getNumValues());
        KU_ASSERT(endListOffset >= startListOffset);
        dataColumn->scanInternal(transaction,
            state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX], startListOffset,
            endListOffset - startListOffset, listColumnChunk.getDataColumnChunk(),
            listColumnChunk.getDataColumnChunk()->getNumValues());
        listColumnChunk.resetOffset();
    } else {
        listColumnChunk.resizeDataColumnChunk(std::bit_ceil(resizeNumValues));
        auto tmpDataColumnChunk = ColumnChunkFactory::createColumnChunkData(*mm,
            ListType::getChildType(this->dataType).copy(), enableCompression,
            std::bit_ceil(resizeNumValues), ResidencyState::IN_MEMORY);
        auto* dataListColumnChunk = listColumnChunk.getDataColumnChunk();
        for (auto i = 0u; i < resultChunk->getNumValues(); i++) {
            offset_t startListOffset = listColumnChunk.getListStartOffset(i);
            offset_t endListOffset = listColumnChunk.getListEndOffset(i);
            // TODO(bmwinger): we should be able to scan directly into the dataListColumnChunk, but
            // it will need to be resized first
            dataColumn->scanInternal(transaction,
                state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX], startListOffset,
                endListOffset - startListOffset, tmpDataColumnChunk.get(), 0);
            KU_ASSERT(endListOffset - startListOffset == tmpDataColumnChunk->getNumValues());
            dataListColumnChunk->append(tmpDataColumnChunk.get(), 0,
                endListOffset - startListOffset);
        }
        listColumnChunk.resetOffset();
    }

    KU_ASSERT(listColumnChunk.sanityCheck());
}

void ListColumn::lookupInternal(const Transaction* transaction, const SegmentState& state,
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
    dataColumn->scanInternal(transaction,
        state.childrenStates[ListChunkData::DATA_COLUMN_CHILD_READ_STATE_IDX], listStartOffset,
        listEndOffset - listStartOffset, dataVector, offsetInVector);
}

void ListColumn::scanUnfiltered(const Transaction* transaction, const SegmentState& state,
    ValueVector* resultVector, uint64_t numValuesToScan,
    const ListOffsetSizeInfo& listOffsetInfoInStorage, offset_t offsetInResult) const {
    numValuesToScan = std::min(numValuesToScan, listOffsetInfoInStorage.numTotal);
    offset_t offsetInVector = offsetInResult;
    for (auto i = 0u; i < numValuesToScan; i++) {
        auto listLen = listOffsetInfoInStorage.getListSize(i);
        resultVector->setValue(i, list_entry_t{offsetInVector, listLen});
        offsetInVector += listLen;
    }
    ListVector::resizeDataVector(resultVector, offsetInVector);
    auto dataVector = ListVector::getDataVector(resultVector);
    offsetInVector = offsetInResult;
    const bool checkOffsetOrder =
        listOffsetInfoInStorage.isOffsetSortedAscending(0, numValuesToScan);
    if (checkOffsetOrder) {
        auto startListOffsetInStorage = listOffsetInfoInStorage.getListStartOffset(0);
        numValuesToScan = numValuesToScan == 0 ? 0 : numValuesToScan - 1;
        auto endListOffsetInStorage = listOffsetInfoInStorage.getListEndOffset(numValuesToScan);
        dataColumn->scanInternal(transaction,
            state.childrenStates[ListChunkData::DATA_COLUMN_CHILD_READ_STATE_IDX],
            startListOffsetInStorage, endListOffsetInStorage - startListOffsetInStorage, dataVector,
            static_cast<uint64_t>(offsetInResult /* offsetInVector */));
    } else {
        for (auto i = 0u; i < numValuesToScan; i++) {
            // Nulls are scanned to the resultVector first
            if (!resultVector->isNull(i)) {
                auto startListOffsetInStorage = listOffsetInfoInStorage.getListStartOffset(i);
                auto appendSize = listOffsetInfoInStorage.getListSize(i);
                dataColumn->scanInternal(transaction,
                    state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX],
                    startListOffsetInStorage, appendSize, dataVector, offsetInVector);
                offsetInVector += appendSize;
            }
        }
    }
}

void ListColumn::scanFiltered(const Transaction* transaction, const SegmentState& state,
    ValueVector* resultVector, const ListOffsetSizeInfo& listOffsetSizeInfo,
    offset_t offsetInResult) const {
    offset_t listOffset = offsetInResult;
    for (auto i = 0u; i < resultVector->state->getSelVector().getSelSize(); i++) {
        auto pos = resultVector->state->getSelVector()[i];
        auto listSize = listOffsetSizeInfo.getListSize(pos);
        resultVector->setValue(pos, list_entry_t{(offset_t)listOffset, listSize});
        listOffset += listSize;
    }
    ListVector::resizeDataVector(resultVector, listOffset);
    listOffset = offsetInResult;
    for (auto i = 0u; i < resultVector->state->getSelVector().getSelSize(); i++) {
        auto pos = resultVector->state->getSelVector()[i];
        // Nulls are scanned to the resultVector first
        if (!resultVector->isNull(pos)) {
            auto startOffsetInStorageToScan = listOffsetSizeInfo.getListStartOffset(pos);
            auto appendSize = listOffsetSizeInfo.getListSize(pos);
            auto dataVector = ListVector::getDataVector(resultVector);
            dataColumn->scanInternal(transaction,
                state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX], startOffsetInStorageToScan,
                appendSize, dataVector, listOffset);
            listOffset += resultVector->getValue<list_entry_t>(pos).size;
        }
    }
}

offset_t ListColumn::readOffset(const Transaction* transaction, const SegmentState& readState,
    offset_t offsetInNodeGroup) const {
    offset_t ret = INVALID_OFFSET;
    const auto& offsetState = readState.childrenStates[OFFSET_COLUMN_CHILD_READ_STATE_IDX];
    offsetColumn->columnReadWriter->readCompressedValueToPage(transaction, offsetState,
        offsetInNodeGroup, reinterpret_cast<uint8_t*>(&ret), 0, offsetColumn->readToPageFunc);
    return ret;
}

list_size_t ListColumn::readSize(const Transaction* transaction, const SegmentState& readState,
    offset_t offsetInNodeGroup) const {
    const auto& sizeState = readState.childrenStates[SIZE_COLUMN_CHILD_READ_STATE_IDX];
    offset_t value = INVALID_OFFSET;
    sizeColumn->columnReadWriter->readCompressedValueToPage(transaction, sizeState,
        offsetInNodeGroup, reinterpret_cast<uint8_t*>(&value), 0, sizeColumn->readToPageFunc);
    return value;
}

ListOffsetSizeInfo ListColumn::getListOffsetSizeInfo(const Transaction* transaction,
    const SegmentState& state, offset_t startOffsetInSegment, offset_t numOffsetsToRead) const {
    auto offsetColumnChunk = ColumnChunkFactory::createColumnChunkData(*mm, LogicalType::INT64(),
        enableCompression, numOffsetsToRead, ResidencyState::IN_MEMORY);
    auto sizeColumnChunk = ColumnChunkFactory::createColumnChunkData(*mm, LogicalType::UINT32(),
        enableCompression, numOffsetsToRead, ResidencyState::IN_MEMORY);
    offsetColumn->scanInternal(transaction,
        state.childrenStates[OFFSET_COLUMN_CHILD_READ_STATE_IDX], startOffsetInSegment,
        numOffsetsToRead, offsetColumnChunk.get(), 0);
    sizeColumn->scanInternal(transaction, state.childrenStates[SIZE_COLUMN_CHILD_READ_STATE_IDX],
        startOffsetInSegment, numOffsetsToRead, sizeColumnChunk.get(), 0);
    auto numValuesScan = offsetColumnChunk->getNumValues();
    return {numValuesScan, std::move(offsetColumnChunk), std::move(sizeColumnChunk)};
}

void ListColumn::checkpointSegment(ColumnCheckpointState&& checkpointState) const {
    auto& persistentListChunk = checkpointState.persistentData.cast<ListChunkData>();
    const auto persistentDataChunk = persistentListChunk.getDataColumnChunk();
    // First, check if we can checkpoint list data chunk in place.
    const auto persistentListDataSize = persistentDataChunk->getNumValues();
    row_idx_t newListDataSize = persistentListDataSize;

    std::vector<SegmentCheckpointState> listDataChunkCheckpointStates;
    for (const auto& segmentCheckpointState : checkpointState.segmentCheckpointStates) {
        const auto& listChunk = segmentCheckpointState.chunkData.cast<ListChunkData>();
        auto startRowInListChunk =
            listChunk.getListStartOffset(segmentCheckpointState.startRowInData);
        row_idx_t listDataSizeToAppend = 0;
        for (offset_t i = 0; i < segmentCheckpointState.numRows; i++) {
            listDataSizeToAppend +=
                listChunk.getListSize(segmentCheckpointState.startRowInData + i);
        }
        if (listDataSizeToAppend > 0) {
            listDataChunkCheckpointStates.push_back(SegmentCheckpointState{
                *segmentCheckpointState.chunkData.cast<ListChunkData>().getDataColumnChunk(),
                startRowInListChunk, newListDataSize, listDataSizeToAppend});
            newListDataSize += listDataSizeToAppend;
        }
    }

    SegmentState chunkState;
    checkpointState.persistentData.initializeScanState(chunkState, this);
    ColumnCheckpointState listDataCheckpointState(*persistentDataChunk,
        std::move(listDataChunkCheckpointStates));
    const auto listDataCanCheckpointInPlace = dataColumn->canCheckpointInPlace(
        chunkState.childrenStates[ListChunkData::DATA_COLUMN_CHILD_READ_STATE_IDX],
        listDataCheckpointState);
    if (!listDataCanCheckpointInPlace) {
        // If we cannot checkpoint list data chunk in place, we need to checkpoint the whole chunk
        // out of place.
        checkpointColumnChunkOutOfPlace(chunkState, checkpointState);
        return;
    }

    // In place checkpoint for list data.
    dataColumn->checkpointColumnChunkInPlace(
        chunkState.childrenStates[ListChunkData::DATA_COLUMN_CHILD_READ_STATE_IDX],
        listDataCheckpointState);

    // Checkpoint offset data.
    std::vector<SegmentCheckpointState> offsetChunkCheckpointStates;

    KU_ASSERT(std::is_sorted(checkpointState.segmentCheckpointStates.begin(),
        checkpointState.segmentCheckpointStates.end(),
        [](const auto& a, const auto& b) { return a.startRowInData < b.startRowInData; }));
    std::vector<std::unique_ptr<ColumnChunkData>> offsetsToWrite;
    for (const auto& segmentCheckpointState : checkpointState.segmentCheckpointStates) {
        KU_ASSERT(
            segmentCheckpointState.chunkData.getNumValues() == segmentCheckpointState.numRows);
        offsetsToWrite.push_back(
            ColumnChunkFactory::createColumnChunkData(*mm, LogicalType::UINT64(), false,
                segmentCheckpointState.numRows, ResidencyState::IN_MEMORY));
        const auto& listChunk = segmentCheckpointState.chunkData.cast<ListChunkData>();
        for (auto i = 0u; i < segmentCheckpointState.numRows; i++) {
            offsetsToWrite.back()->setValue<offset_t>(
                persistentListDataSize + listChunk.getListEndOffset(i), i);
        }
        offsetChunkCheckpointStates.push_back(
            SegmentCheckpointState{*offsetsToWrite.back(), segmentCheckpointState.startRowInData,
                segmentCheckpointState.offsetInSegment, segmentCheckpointState.numRows});
    }

    offsetColumn->checkpointSegment(ColumnCheckpointState(
        *persistentListChunk.getOffsetColumnChunk(), std::move(offsetChunkCheckpointStates)));

    // Checkpoint size data.
    std::vector<SegmentCheckpointState> sizeChunkCheckpointStates;
    for (const auto& segmentCheckpointState : checkpointState.segmentCheckpointStates) {
        sizeChunkCheckpointStates.push_back(SegmentCheckpointState{
            *segmentCheckpointState.chunkData.cast<ListChunkData>().getSizeColumnChunk(),
            segmentCheckpointState.startRowInData, segmentCheckpointState.offsetInSegment,
            segmentCheckpointState.numRows});
    }
    sizeColumn->checkpointSegment(ColumnCheckpointState(*persistentListChunk.getSizeColumnChunk(),
        std::move(sizeChunkCheckpointStates)));
    // Checkpoint null data.
    Column::checkpointNullData(checkpointState);

    KU_ASSERT(persistentListChunk.getNullData()->getNumValues() ==
                  persistentListChunk.getOffsetColumnChunk()->getNumValues() &&
              persistentListChunk.getNullData()->getNumValues() ==
                  persistentListChunk.getSizeColumnChunk()->getNumValues());

    persistentListChunk.syncNumValues();
}

} // namespace storage
} // namespace kuzu

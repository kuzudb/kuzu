#include "storage/table/list_column.h"

#include <algorithm>

#include "common/assert.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/storage_utils.h"
#include "storage/table/column.h"
#include "storage/table/column_chunk.h"
#include "storage/table/column_chunk_data.h"
#include "storage/table/list_chunk_data.h"
#include "storage/table/null_column.h"
#include <bit>

using namespace kuzu::common;

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
    PageAllocator& pageAllocator) {
    auto flushedChunk = flushNonNestedChunkData(chunk, pageAllocator);
    auto& listChunk = chunk.cast<ListChunkData>();
    auto& flushedListChunk = flushedChunk->cast<ListChunkData>();
    flushedListChunk.setOffsetColumnChunk(
        Column::flushChunkData(*listChunk.getOffsetColumnChunk(), pageAllocator));
    flushedListChunk.setSizeColumnChunk(
        Column::flushChunkData(*listChunk.getSizeColumnChunk(), pageAllocator));
    flushedListChunk.setDataColumnChunk(
        Column::flushChunkData(*listChunk.getDataColumnChunk(), pageAllocator));
    return flushedChunk;
}

void ListColumn::scanSegment(const SegmentState& state, offset_t startOffsetInChunk,
    row_idx_t numValuesToScan, ValueVector* resultVector, offset_t offsetInResult) const {
    if (nullColumn) {
        KU_ASSERT(state.nullState);
        nullColumn->scanSegment(*state.nullState, startOffsetInChunk, numValuesToScan, resultVector,
            offsetInResult);
    }
    auto listOffsetSizeInfo = getListOffsetSizeInfo(state, startOffsetInChunk, numValuesToScan);
    if (!resultVector->state || resultVector->state->getSelVector().isUnfiltered()) {
        scanUnfiltered(state, resultVector, numValuesToScan, listOffsetSizeInfo, offsetInResult);
    } else {
        scanFiltered(state, resultVector, listOffsetSizeInfo, offsetInResult);
    }
}

/* FIXME(bmwinger): why was there essentially two different implementations of the same function
(signatures were originally slightly different)?

void ListColumn::scanInternal(const SegmentState& state,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) const {
    nullColumn->scanInternal(*state.nullState, startOffsetInGroup, endOffsetInGroup,
        resultVector, offsetInVector);
    auto listOffsetInfoInStorage =
        getListOffsetSizeInfo(state, startOffsetInGroup, endOffsetInGroup -
startOffsetInGroup);
    offset_t listOffsetInVector = offsetInVector == 0 ? 0 :
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
        dataColumn->scanInternal(
            state.childrenStates[ListChunkData::DATA_COLUMN_CHILD_READ_STATE_IDX], startOffset,
            length, dataVector, offsetToWriteListData);
    } else {
        for (auto i = 0u; i < numValues; i++) {
            offset_t startOffset = listOffsetInfoInStorage.getListStartOffset(i);
            offset_t appendSize = listOffsetInfoInStorage.getListSize(i);
            dataColumn->scanInternal(
                state.childrenStates[ListChunkData::DATA_COLUMN_CHILD_READ_STATE_IDX], startOffset,
                appendSize, dataVector, offsetToWriteListData);
            offsetToWriteListData += appendSize;
        }
    }
}
*/

void ListColumn::scanSegment(const SegmentState& state, ColumnChunkData* resultChunk,
    common::offset_t startOffsetInSegment, common::row_idx_t numValuesToScan) const {
    auto startOffsetInResult = resultChunk->getNumValues();
    Column::scanSegment(state, resultChunk, startOffsetInSegment, numValuesToScan);
    if (numValuesToScan == 0) {
        return;
    }
    // ListChunkData::setNumValues will modify the size of the offset/size chunks before we scan
    // them Revert this so that they scan to the correct position
    // FIXME(bmwinger): there should be a better solution to this, but it will probably be removed
    // later anyway
    auto& listColumnChunk = resultChunk->cast<ListChunkData>();
    listColumnChunk.getOffsetColumnChunk()->setNumValues(startOffsetInResult);
    listColumnChunk.getSizeColumnChunk()->setNumValues(startOffsetInResult);

    offsetColumn->scanSegment(state.childrenStates[OFFSET_COLUMN_CHILD_READ_STATE_IDX],
        listColumnChunk.getOffsetColumnChunk(), startOffsetInSegment, numValuesToScan);
    sizeColumn->scanSegment(state.childrenStates[SIZE_COLUMN_CHILD_READ_STATE_IDX],
        listColumnChunk.getSizeColumnChunk(), startOffsetInSegment, numValuesToScan);
    auto resizeNumValues = listColumnChunk.getDataColumnChunk()->getNumValues();
    bool isOffsetSortedAscending = true;
    KU_ASSERT(listColumnChunk.getSizeColumnChunk()->getNumValues() ==
              startOffsetInResult + numValuesToScan);
    offset_t prevOffset = listColumnChunk.getListStartOffset(startOffsetInResult);
    for (auto i = startOffsetInResult; i < startOffsetInResult + numValuesToScan; i++) {
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
        offset_t startListOffset = listColumnChunk.getListStartOffset(startOffsetInResult);
        offset_t endListOffset =
            listColumnChunk.getListStartOffset(startOffsetInResult + numValuesToScan);
        KU_ASSERT(endListOffset >= startListOffset);
        dataColumn->scanSegment(state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX],
            listColumnChunk.getDataColumnChunk(), startListOffset, endListOffset - startListOffset);
    } else {
        listColumnChunk.resizeDataColumnChunk(std::bit_ceil(resizeNumValues));
        for (auto i = startOffsetInResult; i < startOffsetInResult + numValuesToScan; i++) {
            offset_t startListOffset = listColumnChunk.getListStartOffset(i);
            offset_t endListOffset = listColumnChunk.getListEndOffset(i);
            dataColumn->scanSegment(state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX],
                listColumnChunk.getDataColumnChunk(), startListOffset,
                endListOffset - startListOffset);
        }
    }
    listColumnChunk.resetOffset();

    KU_ASSERT(listColumnChunk.sanityCheck());
}

void ListColumn::lookupInternal(const SegmentState& state, offset_t nodeOffset,
    ValueVector* resultVector, uint32_t posInVector) const {
    auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
    const auto listEndOffset = readOffset(state, offsetInChunk);
    const auto size = readSize(state, offsetInChunk);
    const auto listStartOffset = listEndOffset - size;
    const auto offsetInVector =
        posInVector == 0 ? 0 : resultVector->getValue<offset_t>(posInVector - 1);
    resultVector->setValue(posInVector, list_entry_t{offsetInVector, size});
    ListVector::resizeDataVector(resultVector, offsetInVector + size);
    const auto dataVector = ListVector::getDataVector(resultVector);
    dataColumn->scanSegment(state.childrenStates[ListChunkData::DATA_COLUMN_CHILD_READ_STATE_IDX],
        listStartOffset, listEndOffset - listStartOffset, dataVector, offsetInVector);
}

void ListColumn::scanUnfiltered(const SegmentState& state, ValueVector* resultVector,
    uint64_t numValuesToScan, const ListOffsetSizeInfo& listOffsetInfoInStorage,
    offset_t offsetInResult) const {
    auto dataVector = ListVector::getDataVector(resultVector);
    // Scans append to the end of the vector, so we need to start at the end of the last list
    auto startOffsetInDataVector = ListVector::getDataVectorSize(resultVector);
    auto offsetInDataVector = startOffsetInDataVector;

    numValuesToScan = std::min(numValuesToScan, listOffsetInfoInStorage.numTotal);
    for (auto i = 0u; i < numValuesToScan; i++) {
        auto listLen = listOffsetInfoInStorage.getListSize(i);
        resultVector->setValue(offsetInResult + i, list_entry_t{offsetInDataVector, listLen});
        offsetInDataVector += listLen;
    }
    ListVector::resizeDataVector(resultVector, offsetInDataVector);
    const bool checkOffsetOrder =
        listOffsetInfoInStorage.isOffsetSortedAscending(0, numValuesToScan);
    if (checkOffsetOrder) {
        auto startListOffsetInStorage = listOffsetInfoInStorage.getListStartOffset(0);
        numValuesToScan = numValuesToScan == 0 ? 0 : numValuesToScan - 1;
        auto endListOffsetInStorage = listOffsetInfoInStorage.getListEndOffset(numValuesToScan);
        dataColumn->scanSegment(
            state.childrenStates[ListChunkData::DATA_COLUMN_CHILD_READ_STATE_IDX],
            startListOffsetInStorage, endListOffsetInStorage - startListOffsetInStorage, dataVector,
            static_cast<uint64_t>(startOffsetInDataVector /* offsetInVector */));
    } else {
        offsetInDataVector = startOffsetInDataVector;
        for (auto i = 0u; i < numValuesToScan; i++) {
            // Nulls are scanned to the resultVector first
            if (!resultVector->isNull(i)) {
                auto startListOffsetInStorage = listOffsetInfoInStorage.getListStartOffset(i);
                auto appendSize = listOffsetInfoInStorage.getListSize(i);
                dataColumn->scanSegment(state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX],
                    startListOffsetInStorage, appendSize, dataVector, offsetInDataVector);
                offsetInDataVector += appendSize;
            }
        }
    }
}

void ListColumn::scanFiltered(const SegmentState& state, ValueVector* resultVector,
    const ListOffsetSizeInfo& listOffsetSizeInfo, offset_t offsetInResult) const {
    auto dataVector = ListVector::getDataVector(resultVector);
    auto startOffsetInDataVector = ListVector::getDataVectorSize(resultVector);
    auto offsetInDataVector = startOffsetInDataVector;

    for (auto i = 0u; i < resultVector->state->getSelVector().getSelSize(); i++) {
        auto pos = resultVector->state->getSelVector()[i];
        auto listSize = listOffsetSizeInfo.getListSize(pos);
        resultVector->setValue(offsetInResult + pos,
            list_entry_t{(offset_t)offsetInDataVector, listSize});
        offsetInDataVector += listSize;
    }
    ListVector::resizeDataVector(resultVector, offsetInDataVector);
    offsetInDataVector = startOffsetInDataVector;
    for (auto i = 0u; i < resultVector->state->getSelVector().getSelSize(); i++) {
        auto pos = resultVector->state->getSelVector()[i];
        // Nulls are scanned to the resultVector first
        if (!resultVector->isNull(pos)) {
            auto startOffsetInStorageToScan = listOffsetSizeInfo.getListStartOffset(pos);
            auto appendSize = listOffsetSizeInfo.getListSize(pos);
            dataColumn->scanSegment(state.childrenStates[DATA_COLUMN_CHILD_READ_STATE_IDX],
                startOffsetInStorageToScan, appendSize, dataVector, offsetInDataVector);
            offsetInDataVector += resultVector->getValue<list_entry_t>(pos).size;
        }
    }
}

offset_t ListColumn::readOffset(const SegmentState& state, offset_t offsetInNodeGroup) const {
    offset_t ret = INVALID_OFFSET;
    const auto& offsetState = state.childrenStates[OFFSET_COLUMN_CHILD_READ_STATE_IDX];
    offsetColumn->columnReadWriter->readCompressedValueToPage(offsetState, offsetInNodeGroup,
        reinterpret_cast<uint8_t*>(&ret), 0, offsetColumn->readToPageFunc);
    return ret;
}

list_size_t ListColumn::readSize(const SegmentState& readState, offset_t offsetInNodeGroup) const {
    const auto& sizeState = readState.childrenStates[SIZE_COLUMN_CHILD_READ_STATE_IDX];
    offset_t value = INVALID_OFFSET;
    sizeColumn->columnReadWriter->readCompressedValueToPage(sizeState, offsetInNodeGroup,
        reinterpret_cast<uint8_t*>(&value), 0, sizeColumn->readToPageFunc);
    return value;
}

ListOffsetSizeInfo ListColumn::getListOffsetSizeInfo(const SegmentState& state,
    offset_t startOffsetInSegment, offset_t numOffsetsToRead) const {
    auto offsetColumnChunk = ColumnChunkFactory::createColumnChunkData(*mm, LogicalType::INT64(),
        enableCompression, numOffsetsToRead, ResidencyState::IN_MEMORY);
    auto sizeColumnChunk = ColumnChunkFactory::createColumnChunkData(*mm, LogicalType::UINT32(),
        enableCompression, numOffsetsToRead, ResidencyState::IN_MEMORY);
    offsetColumn->scanSegment(state.childrenStates[OFFSET_COLUMN_CHILD_READ_STATE_IDX],
        offsetColumnChunk.get(), startOffsetInSegment, numOffsetsToRead);
    sizeColumn->scanSegment(state.childrenStates[SIZE_COLUMN_CHILD_READ_STATE_IDX],
        sizeColumnChunk.get(), startOffsetInSegment, numOffsetsToRead);
    auto numValuesScan = offsetColumnChunk->getNumValues();
    return {numValuesScan, std::move(offsetColumnChunk), std::move(sizeColumnChunk)};
}

std::vector<std::unique_ptr<ColumnChunkData>> ListColumn::checkpointSegment(
    ColumnCheckpointState&& checkpointState, PageAllocator& pageAllocator) const {
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
        return checkpointColumnChunkOutOfPlace(chunkState, checkpointState, pageAllocator);
    }

    // In place checkpoint for list data.
    dataColumn->checkpointColumnChunkInPlace(
        chunkState.childrenStates[ListChunkData::DATA_COLUMN_CHILD_READ_STATE_IDX],
        listDataCheckpointState, pageAllocator);

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

    offsetColumn->checkpointSegment(
        ColumnCheckpointState(*persistentListChunk.getOffsetColumnChunk(),
            std::move(offsetChunkCheckpointStates)),
        pageAllocator);

    // Checkpoint size data.
    std::vector<SegmentCheckpointState> sizeChunkCheckpointStates;
    for (const auto& segmentCheckpointState : checkpointState.segmentCheckpointStates) {
        sizeChunkCheckpointStates.push_back(SegmentCheckpointState{
            *segmentCheckpointState.chunkData.cast<ListChunkData>().getSizeColumnChunk(),
            segmentCheckpointState.startRowInData, segmentCheckpointState.offsetInSegment,
            segmentCheckpointState.numRows});
    }
    sizeColumn->checkpointSegment(ColumnCheckpointState(*persistentListChunk.getSizeColumnChunk(),
                                      std::move(sizeChunkCheckpointStates)),
        pageAllocator);
    // Checkpoint null data.
    Column::checkpointNullData(checkpointState, pageAllocator);

    KU_ASSERT(persistentListChunk.getNullData()->getNumValues() ==
                  persistentListChunk.getOffsetColumnChunk()->getNumValues() &&
              persistentListChunk.getNullData()->getNumValues() ==
                  persistentListChunk.getSizeColumnChunk()->getNumValues());

    persistentListChunk.syncNumValues();
    return {};
}

} // namespace storage
} // namespace kuzu

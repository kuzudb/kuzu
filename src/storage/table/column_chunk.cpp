#include "storage/table/column_chunk.h"

#include <algorithm>
#include <memory>

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/vector/value_vector.h"
#include "main/client_context.h"
#include "storage/buffer_manager/memory_manager.h"
#include "storage/enums/residency_state.h"
#include "storage/file_handle.h"
#include "storage/page_allocator.h"
#include "storage/storage_utils.h"
#include "storage/table/column.h"
#include "storage/table/column_chunk_data.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

ColumnChunk::ColumnChunk(MemoryManager& mm, LogicalType&& dataType, uint64_t capacity,
    bool enableCompression, ResidencyState residencyState, bool initializeToZero)
    : mm{mm}, enableCompression{enableCompression} {
    data.push_back(ColumnChunkFactory::createColumnChunkData(mm, std::move(dataType),
        enableCompression, capacity, residencyState, true, initializeToZero));
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
}

ColumnChunk::ColumnChunk(MemoryManager& mm, LogicalType&& dataType, bool enableCompression,
    ColumnChunkMetadata metadata)
    : mm{mm}, enableCompression{enableCompression} {
    data.push_back(ColumnChunkFactory::createColumnChunkData(mm, std::move(dataType),
        enableCompression, metadata, true, true));
}

ColumnChunk::ColumnChunk(MemoryManager& mm, bool enableCompression,
    std::unique_ptr<ColumnChunkData> data)
    : mm{mm}, enableCompression{enableCompression}, data{} {
    this->data.push_back(std::move(data));
}
ColumnChunk::ColumnChunk(MemoryManager& mm, bool enableCompression,
    std::vector<std::unique_ptr<ColumnChunkData>> segments)
    : mm{mm}, enableCompression{enableCompression}, data{std::move(segments)} {}

void ColumnChunk::initializeScanState(ChunkState& state, const Column* column) const {
    state.column = column;
    state.segmentStates.resize(data.size());
    for (size_t i = 0; i < data.size(); i++) {
        data[i]->initializeScanState(state.segmentStates[i], column);
    }
}

void ColumnChunk::scan(const Transaction* transaction, const ChunkState& state, ValueVector& output,
    offset_t offsetInChunk, length_t length) const {
    // Check if there is deletions or insertions. If so, update selVector based on transaction.
    switch (getResidencyState()) {
    case ResidencyState::IN_MEMORY: {
        rangeSegments(offsetInChunk, length,
            [&](auto& segment, auto offsetInSegment, auto lengthInSegment, auto dstOffset) {
                segment.scan(output, offsetInSegment, lengthInSegment, dstOffset);
            });
    } break;
    case ResidencyState::ON_DISK: {
        state.column->scan(state, offsetInChunk, length, &output, 0);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    if (!updateInfo) {
        return;
    }
    auto [startVectorIdx, startOffsetInVector] =
        StorageUtils::getQuotientRemainder(offsetInChunk, DEFAULT_VECTOR_CAPACITY);
    auto [endVectorIdx, endOffsetInVector] =
        StorageUtils::getQuotientRemainder(offsetInChunk + length, DEFAULT_VECTOR_CAPACITY);
    idx_t idx = startVectorIdx;
    sel_t posInVector = 0u;
    while (idx <= endVectorIdx) {
        const auto startOffset = idx == startVectorIdx ? startOffsetInVector : 0;
        const auto endOffset = idx == endVectorIdx ? endOffsetInVector : DEFAULT_VECTOR_CAPACITY;
        const auto numRowsInVector = endOffset - startOffset;
        if (const auto vectorInfo = updateInfo->getVectorInfo(transaction, idx);
            vectorInfo && vectorInfo->numRowsUpdated > 0) {
            for (auto i = 0u; i < numRowsInVector; i++) {
                if (const auto itr = std::find_if(vectorInfo->rowsInVector.begin(),
                        vectorInfo->rowsInVector.begin() + vectorInfo->numRowsUpdated,
                        [i, startOffset](auto row) { return row == i + startOffset; });
                    itr != vectorInfo->rowsInVector.begin() + vectorInfo->numRowsUpdated) {
                    vectorInfo->data->lookup(itr - vectorInfo->rowsInVector.begin(), output,
                        posInVector + i);
                }
            }
        }
        posInVector += numRowsInVector;
        idx++;
    }
}

template<ResidencyState SCAN_RESIDENCY_STATE>
void ColumnChunk::scanCommitted(const Transaction* transaction, ChunkState& chunkState,
    ColumnChunkData& output, row_idx_t startRow, row_idx_t numRows) const {
    auto numValuesInChunk = getNumValues();
    if (numRows == INVALID_ROW_IDX || startRow + numRows > numValuesInChunk) {
        numRows = numValuesInChunk - startRow;
    }
    if (numRows == 0 || startRow >= numValuesInChunk) {
        return;
    }
    const auto numValuesBeforeScan = output.getNumValues();
    switch (const auto residencyState = getResidencyState()) {
    case ResidencyState::ON_DISK: {
        if (SCAN_RESIDENCY_STATE == residencyState) {
            chunkState.column->scan(chunkState, &output, startRow, numRows);
            scanCommittedUpdates(transaction, output, numValuesBeforeScan, startRow, numRows);
        }
    } break;
    case ResidencyState::IN_MEMORY: {
        if (SCAN_RESIDENCY_STATE == residencyState) {
            rangeSegments(startRow, numRows,
                [&](auto& segment, auto offsetInSegment, auto lengthInSegment, auto dstOffset) {
                    output.append(&segment, startRow, lengthInSegment);
                    scanCommittedUpdates(transaction, output, numValuesBeforeScan + dstOffset,
                        offsetInSegment, lengthInSegment);
                });
        }
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

template void ColumnChunk::scanCommitted<ResidencyState::ON_DISK>(const Transaction* transaction,
    ChunkState& chunkState, ColumnChunkData& output, row_idx_t startRow, row_idx_t numRows) const;
template void ColumnChunk::scanCommitted<ResidencyState::IN_MEMORY>(const Transaction* transaction,
    ChunkState& chunkState, ColumnChunkData& output, row_idx_t startRow, row_idx_t numRows) const;

bool ColumnChunk::hasUpdates(const Transaction* transaction, row_idx_t startRow,
    length_t numRows) const {
    return updateInfo && updateInfo->hasUpdates(transaction, startRow, numRows);
}

void ColumnChunk::scanCommittedUpdates(const Transaction* transaction, ColumnChunkData& output,
    offset_t startOffsetInOutput, row_idx_t startRowScanned, row_idx_t numRows) const {
    if (!updateInfo) {
        return;
    }
    auto [startVectorIdx, startRowInVector] =
        StorageUtils::getQuotientRemainder(startRowScanned, DEFAULT_VECTOR_CAPACITY);
    auto [endVectorIdx, endRowInVector] =
        StorageUtils::getQuotientRemainder(startRowScanned + numRows, DEFAULT_VECTOR_CAPACITY);
    idx_t vectorIdx = startVectorIdx;
    while (vectorIdx <= endVectorIdx) {
        const auto startRow = vectorIdx == startVectorIdx ? startRowInVector : 0;
        const auto endRow = vectorIdx == endVectorIdx ? endRowInVector : DEFAULT_VECTOR_CAPACITY;
        // const auto numRowsInVector = endRow - startRow;
        const auto vectorInfo = updateInfo->getVectorInfo(transaction, vectorIdx);
        if (vectorInfo && vectorInfo->numRowsUpdated > 0) {
            if (vectorIdx != startVectorIdx && vectorIdx != endVectorIdx) {
                for (auto i = 0u; i < vectorInfo->numRowsUpdated; i++) {
                    output.write(vectorInfo->data.get(), i,
                        startOffsetInOutput + vectorIdx * DEFAULT_VECTOR_CAPACITY +
                            vectorInfo->rowsInVector[i] - startRowScanned,
                        1);
                }
            } else {
                for (auto i = 0u; i < vectorInfo->numRowsUpdated; i++) {
                    const auto rowInVecUpdated = vectorInfo->rowsInVector[i];
                    if (rowInVecUpdated >= startRow && rowInVecUpdated < endRow) {
                        output.write(vectorInfo->data.get(), i,
                            startOffsetInOutput + vectorIdx * DEFAULT_VECTOR_CAPACITY +
                                rowInVecUpdated - startRowScanned,
                            1);
                    }
                }
            }
        }
        vectorIdx++;
    }
}

void ColumnChunk::lookup(const Transaction* transaction, const ChunkState& state,
    offset_t rowInChunk, ValueVector& output, sel_t posInOutputVector) const {
    switch (getResidencyState()) {
    case ResidencyState::IN_MEMORY: {
        rangeSegments(rowInChunk, 1, [&](auto& segment, auto offsetInSegment, auto, auto) {
            segment.lookup(offsetInSegment, output, posInOutputVector);
        });
    } break;
    case ResidencyState::ON_DISK: {
        state.column->lookupValue(state, rowInChunk, &output, posInOutputVector);
    } break;
    }
    if (updateInfo) {
        auto [vectorIdx, rowInVector] =
            StorageUtils::getQuotientRemainder(rowInChunk, DEFAULT_VECTOR_CAPACITY);
        if (const auto vectorInfo = updateInfo->getVectorInfo(transaction, vectorIdx)) {
            for (auto i = 0u; i < vectorInfo->numRowsUpdated; i++) {
                if (vectorInfo->rowsInVector[i] == rowInVector) {
                    vectorInfo->data->lookup(i, output, posInOutputVector);
                    return;
                }
            }
        }
    }
}

void ColumnChunk::update(const Transaction* transaction, offset_t offsetInChunk,
    const ValueVector& values) {
    if (transaction->getType() == TransactionType::DUMMY) {
        rangeSegments(offsetInChunk, 1, [&](auto& segment, auto offsetInSegment, auto, auto) {
            segment.write(&values, values.state->getSelVector().getSelectedPositions()[0],
                offsetInSegment);
        });
        return;
    }

    // TODO(bmwinger): this doesn't really work. The metadata should be left as-is until the update
    // is checkpointed. If it's necessary for zone-mapping we should store either separate
    // ColumnChunkStatistics, or statistics just for the updated values
    /*
    auto segment = data.begin();
    while (offsetInChunk > segment->get()->getNumValues()) {
        offsetInChunk -= segment->get()->getNumValues();
        segment++;
    }
    segment->get()->updateStats(&values, values.state->getSelVector());
    */
    if (!updateInfo) {
        updateInfo = std::make_unique<UpdateInfo>();
    }
    const auto vectorIdx = offsetInChunk / DEFAULT_VECTOR_CAPACITY;
    const auto rowIdxInVector = offsetInChunk % DEFAULT_VECTOR_CAPACITY;
    const auto vectorUpdateInfo = updateInfo->update(data.front()->getMemoryManager(), transaction,
        vectorIdx, rowIdxInVector, values);
    transaction->pushVectorUpdateInfo(*updateInfo, vectorIdx, *vectorUpdateInfo);
}

MergedColumnChunkStats ColumnChunk::getMergedColumnChunkStats(
    const Transaction* transaction) const {
    auto baseStats = MergedColumnChunkStats{ColumnChunkStats{}, true, true};
    for (auto& segment : data) {
        // TODO: Replace with a function that modifies the existing stats in-place?
        auto segmentStats = segment->getMergedColumnChunkStats();
        baseStats.merge(segmentStats, segment->getDataType().getPhysicalType());
    }
    if (updateInfo) {
        for (idx_t i = 0; i < updateInfo->getNumVectors(); ++i) {
            const auto* vectorInfo = updateInfo->getVectorInfo(transaction, i);
            if (vectorInfo) {
                baseStats.merge(vectorInfo->data->getMergedColumnChunkStats(),
                    getDataType().getPhysicalType());
            }
        }
    }
    return baseStats;
}

void ColumnChunk::serialize(Serializer& serializer) const {
    serializer.writeDebuggingInfo("enable_compression");
    serializer.write<bool>(enableCompression);
    serializer.write<uint64_t>(data.size());
    for (auto& segment : data) {
        segment->serialize(serializer);
    }
}

std::unique_ptr<ColumnChunk> ColumnChunk::deserialize(MemoryManager& mm, Deserializer& deSer) {
    std::string key;
    bool enableCompression = false;
    deSer.validateDebuggingInfo(key, "enable_compression");
    deSer.deserializeValue<bool>(enableCompression);
    uint64_t numSegments = 0;
    deSer.deserializeValue(numSegments);
    std::vector<std::unique_ptr<ColumnChunkData>> segments;
    for (uint64_t i = 0; i < numSegments; i++) {
        segments.push_back(ColumnChunkData::deserialize(mm, deSer));
    }
    return std::make_unique<ColumnChunk>(mm, enableCompression, std::move(segments));
}

row_idx_t ColumnChunk::getNumUpdatedRows(const Transaction* transaction) const {
    return updateInfo ? updateInfo->getNumUpdatedRows(transaction) : 0;
}

std::pair<std::unique_ptr<ColumnChunkData>, std::unique_ptr<ColumnChunkData>>
ColumnChunk::scanUpdates(const Transaction* transaction) const {
    auto numUpdatedRows = getNumUpdatedRows(transaction);
    // TODO(Guodong): Actually for row idx in a column chunk, UINT32 should be enough.
    auto updatedRows = ColumnChunkFactory::createColumnChunkData(mm, LogicalType::UINT64(), false,
        numUpdatedRows, ResidencyState::IN_MEMORY);
    auto updatedData = ColumnChunkFactory::createColumnChunkData(mm, getDataType().copy(), false,
        numUpdatedRows, ResidencyState::IN_MEMORY);
    const auto numUpdateVectors = updateInfo->getNumVectors();
    row_idx_t numAppendedRows = 0;
    for (auto vectorIdx = 0u; vectorIdx < numUpdateVectors; vectorIdx++) {
        const auto vectorInfo = updateInfo->getVectorInfo(transaction, vectorIdx);
        if (!vectorInfo) {
            continue;
        }
        const row_idx_t startRowIdx = vectorIdx * DEFAULT_VECTOR_CAPACITY;
        for (auto rowIdx = 0u; rowIdx < vectorInfo->numRowsUpdated; rowIdx++) {
            updatedRows->setValue<row_idx_t>(vectorInfo->rowsInVector[rowIdx] + startRowIdx,
                numAppendedRows++);
        }
        updatedData->append(vectorInfo->data.get(), 0, vectorInfo->numRowsUpdated);
        KU_ASSERT(updatedData->getNumValues() == updatedRows->getNumValues());
    }
    return {std::move(updatedRows), std::move(updatedData)};
}

void ColumnChunk::reclaimStorage(PageAllocator& pageAllocator) const {
    for (const auto& segment : data) {
        segment->reclaimStorage(pageAllocator);
    }
}

void ColumnChunk::append(common::ValueVector* vector, const common::SelectionView& selView) {
    data.back()->append(vector, selView);
}

void ColumnChunk::append(const ColumnChunk* other, common::offset_t startPosInOtherChunk,
    uint32_t numValuesToAppend) {
    for (auto& otherSegment : other->data) {
        if (numValuesToAppend == 0) {
            return;
        }
        if (otherSegment->getNumValues() < startPosInOtherChunk) {
            startPosInOtherChunk -= otherSegment->getNumValues();
        } else {
            auto numValuesToAppendInSegment =
                std::min(otherSegment->getNumValues(), uint64_t{numValuesToAppend});
            append(otherSegment.get(), startPosInOtherChunk, numValuesToAppendInSegment);
            numValuesToAppend -= numValuesToAppendInSegment;
            startPosInOtherChunk = 0;
        }
    }
}

void ColumnChunk::append(const ColumnChunkData* other, common::offset_t startPosInOtherChunk,
    uint32_t numValuesToAppend) {
    data.back()->append(other, startPosInOtherChunk, numValuesToAppend);
}

std::unique_ptr<ColumnChunk> ColumnChunk::flushAsNewColumnChunk(
    PageAllocator& pageAllocator) const {
    std::vector<std::unique_ptr<ColumnChunkData>> segments;
    for (auto& segment : data) {
        segments.push_back(Column::flushChunkData(*segment, pageAllocator));
    }
    return std::make_unique<ColumnChunk>(mm, isCompressionEnabled(), std::move(segments));
}

void ColumnChunk::write(Column& column, ChunkState& state, offset_t dstOffset,
    const ColumnChunkData& dataToWrite, offset_t srcOffset, common::length_t numValues) {
    auto segment = data.begin();
    auto offsetInSegment = dstOffset;
    while (segment->get()->getNumValues() < offsetInSegment) {
        offsetInSegment -= segment->get()->getNumValues();
        segment++;
    }
    while (numValues > 0) {
        auto numValuesToWriteInSegment =
            std::min(numValues, segment->get()->getNumValues()) - offsetInSegment;
        column.write(*segment->get(), state, offsetInSegment, dataToWrite, srcOffset,
            numValuesToWriteInSegment);
        offsetInSegment = 0;
        numValues -= numValuesToWriteInSegment;
        srcOffset += numValuesToWriteInSegment;
    }
}

void ColumnChunk::checkpoint(Column& column,
    std::vector<ChunkCheckpointState>&& chunkCheckpointStates, PageAllocator& pageAllocator) {
    offset_t segmentStart = 0;
    for (size_t i = 0; i < data.size(); i++) {
        std::vector<SegmentCheckpointState> segmentCheckpointStates;
        auto& segment = data[i];
        KU_ASSERT(segment->getResidencyState() == ResidencyState::ON_DISK);
        for (auto& state : chunkCheckpointStates) {
            if (state.startRow + state.numRows > segmentStart &&
                (i == data.size() - 1 || state.startRow < segmentStart + segment->getNumValues())) {
                auto startOffsetInSegment = state.startRow - segmentStart;
                uint64_t startRowInChunk = 0;
                if (state.startRow < segmentStart) {
                    startRowInChunk = segmentStart - state.startRow;
                    startOffsetInSegment = 0;
                }
                const auto endRowInChunk =
                    std::min(startRowInChunk + segment->getNumValues(), state.numRows);
                segmentCheckpointStates.push_back({*state.chunkData, startRowInChunk,
                    startOffsetInSegment, endRowInChunk - startRowInChunk});
            }
        }
        auto segmentEnd = segmentStart + segment->getNumValues();
        // If the segment was split during checkpointing we need to insert the new segments into the
        // ColumnChunk
        auto newSegments = column.checkpointSegment(
            ColumnCheckpointState(*segment, std::move(segmentCheckpointStates)), pageAllocator);
        if (!newSegments.empty()) {
            auto oldSize = data.size();
            data.resize(data.size() - 1 + newSegments.size());
            std::move_backward(data.begin() + i, data.begin() + oldSize, data.end());
            for (size_t j = 0; j < newSegments.size(); j++) {
                data[i + j] = std::move(newSegments[j]);
            }
            // We want to increment by a total of newSegments.size() but we increment i at the end
            // of each loop body
            i += newSegments.size() - 1;
        }
        segmentStart = segmentEnd;
    }
}

} // namespace storage
} // namespace kuzu

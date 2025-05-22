#include "storage/table/column_chunk.h"

#include <algorithm>

#include "common/serializer/deserializer.h"
#include "common/vector/value_vector.h"
#include "main/client_context.h"
#include "storage/storage_utils.h"
#include "storage/table/column.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

ColumnChunk::ColumnChunk(MemoryManager& memoryManager, LogicalType&& dataType, uint64_t capacity,
    bool enableCompression, ResidencyState residencyState, bool initializeToZero)
    : enableCompression{enableCompression} {
    data = ColumnChunkFactory::createColumnChunkData(memoryManager, std::move(dataType),
        enableCompression, capacity, residencyState, true, initializeToZero);
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
}

ColumnChunk::ColumnChunk(MemoryManager& memoryManager, LogicalType&& dataType,
    bool enableCompression, ColumnChunkMetadata metadata)
    : enableCompression{enableCompression} {
    data = ColumnChunkFactory::createColumnChunkData(memoryManager, std::move(dataType),
        enableCompression, metadata, true, true);
}

ColumnChunk::ColumnChunk(bool enableCompression, std::unique_ptr<ColumnChunkData> data)
    : enableCompression{enableCompression}, data{std::move(data)} {}

void ColumnChunk::initializeScanState(ChunkState& state, const Column* column) const {
    data->initializeScanState(state, column);
}

void ColumnChunk::scan(const Transaction* transaction, const ChunkState& state, ValueVector& output,
    offset_t offsetInChunk, length_t length) const {
    // Check if there is deletions or insertions. If so, update selVector based on transaction.
    switch (getResidencyState()) {
    case ResidencyState::IN_MEMORY: {
        data->scan(output, offsetInChunk, length);
    } break;
    case ResidencyState::ON_DISK: {
        state.column->scan(&DUMMY_TRANSACTION, state, offsetInChunk, length, &output);
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
    ColumnChunk& output, row_idx_t startRow, row_idx_t numRows) const {
    if (numRows == INVALID_ROW_IDX) {
        numRows = getNumValues();
    }
    const auto numValuesBeforeScan = output.getNumValues();
    switch (const auto residencyState = getResidencyState()) {
    case ResidencyState::ON_DISK: {
        if (SCAN_RESIDENCY_STATE == residencyState) {
            chunkState.column->scan(transaction, chunkState, &output.getData(), startRow,
                startRow + numRows);
            scanCommittedUpdates(transaction, output.getData(), numValuesBeforeScan, startRow,
                numRows);
        }
    } break;
    case ResidencyState::IN_MEMORY: {
        if (SCAN_RESIDENCY_STATE == residencyState) {
            output.getData().append(data.get(), startRow, numRows);
            scanCommittedUpdates(transaction, output.getData(), numValuesBeforeScan, startRow,
                numRows);
        }
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

template void ColumnChunk::scanCommitted<ResidencyState::ON_DISK>(const Transaction* transaction,
    ChunkState& chunkState, ColumnChunk& output, row_idx_t startRow, row_idx_t numRows) const;
template void ColumnChunk::scanCommitted<ResidencyState::IN_MEMORY>(const Transaction* transaction,
    ChunkState& chunkState, ColumnChunk& output, row_idx_t startRow, row_idx_t numRows) const;

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
        data->lookup(rowInChunk, output, posInOutputVector);
    } break;
    case ResidencyState::ON_DISK: {
        state.column->lookupValue(transaction, state, rowInChunk, &output, posInOutputVector);
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
    if (transaction->getID() == Transaction::DUMMY_TRANSACTION_ID) {
        data->write(&values, values.state->getSelVector().getSelectedPositions()[0], offsetInChunk);
        return;
    }
    data->updateStats(&values, values.state->getSelVector());
    if (!updateInfo) {
        updateInfo = std::make_unique<UpdateInfo>();
    }
    const auto vectorIdx = offsetInChunk / DEFAULT_VECTOR_CAPACITY;
    const auto rowIdxInVector = offsetInChunk % DEFAULT_VECTOR_CAPACITY;
    const auto vectorUpdateInfo = updateInfo->update(data->getMemoryManager(), transaction,
        vectorIdx, rowIdxInVector, values);
    transaction->pushVectorUpdateInfo(*updateInfo, vectorIdx, *vectorUpdateInfo);
}

MergedColumnChunkStats ColumnChunk::getMergedColumnChunkStats(
    const transaction::Transaction* transaction) const {
    auto baseStats = data->getMergedColumnChunkStats();
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
    data->serialize(serializer);
}

std::unique_ptr<ColumnChunk> ColumnChunk::deserialize(MemoryManager& memoryManager,
    Deserializer& deSer) {
    std::string key;
    bool enableCompression = false;
    deSer.validateDebuggingInfo(key, "enable_compression");
    deSer.deserializeValue<bool>(enableCompression);
    auto data = ColumnChunkData::deserialize(memoryManager, deSer);
    return std::make_unique<ColumnChunk>(enableCompression, std::move(data));
}

row_idx_t ColumnChunk::getNumUpdatedRows(const Transaction* transaction) const {
    return updateInfo ? updateInfo->getNumUpdatedRows(transaction) : 0;
}

std::pair<std::unique_ptr<ColumnChunk>, std::unique_ptr<ColumnChunk>> ColumnChunk::scanUpdates(
    const Transaction* transaction) const {
    auto numUpdatedRows = getNumUpdatedRows(transaction);
    auto& mm = *transaction->getClientContext()->getMemoryManager();
    // TODO(Guodong): Actually for row idx in a column chunk, UINT32 should be enough.
    auto updatedRows = std::make_unique<ColumnChunk>(mm, LogicalType::UINT64(), numUpdatedRows,
        false, ResidencyState::IN_MEMORY);
    auto updatedData = std::make_unique<ColumnChunk>(mm, getDataType().copy(), numUpdatedRows,
        false, ResidencyState::IN_MEMORY);
    const auto numUpdateVectors = updateInfo->getNumVectors();
    row_idx_t numAppendedRows = 0;
    for (auto vectorIdx = 0u; vectorIdx < numUpdateVectors; vectorIdx++) {
        const auto vectorInfo = updateInfo->getVectorInfo(transaction, vectorIdx);
        if (!vectorInfo) {
            continue;
        }
        const row_idx_t startRowIdx = vectorIdx * DEFAULT_VECTOR_CAPACITY;
        for (auto rowIdx = 0u; rowIdx < vectorInfo->numRowsUpdated; rowIdx++) {
            updatedRows->getData().setValue<row_idx_t>(
                vectorInfo->rowsInVector[rowIdx] + startRowIdx, numAppendedRows++);
        }
        updatedData->getData().append(vectorInfo->data.get(), 0, vectorInfo->numRowsUpdated);
        KU_ASSERT(updatedData->getData().getNumValues() == updatedRows->getData().getNumValues());
    }
    return {std::move(updatedRows), std::move(updatedData)};
}

void ColumnChunk::reclaimStorage(FileHandle& dataFH) {
    data->reclaimStorage(dataFH);
}

} // namespace storage
} // namespace kuzu

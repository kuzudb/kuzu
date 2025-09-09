#include "storage/table/column_chunk.h"

#include "common/serializer/deserializer.h"
#include "common/serializer/serializer.h"
#include "common/vector/value_vector.h"
#include "storage/table/column.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

ColumnChunk::ColumnChunk(MemoryManager& mm, LogicalType&& dataType, uint64_t capacity,
    bool enableCompression, ResidencyState residencyState, bool initializeToZero)
    : enableCompression{enableCompression} {
    data = ColumnChunkFactory::createColumnChunkData(mm, std::move(dataType), enableCompression,
        capacity, residencyState, true, initializeToZero);
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
}

ColumnChunk::ColumnChunk(MemoryManager& mm, LogicalType&& dataType, bool enableCompression,
    ColumnChunkMetadata metadata)
    : enableCompression{enableCompression} {
    data = ColumnChunkFactory::createColumnChunkData(mm, std::move(dataType), enableCompression,
        metadata, true, true);
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
        state.column->scan(state, offsetInChunk, length, &output);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    updateInfo.scan(transaction, output, offsetInChunk, length);
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
            chunkState.column->scan(chunkState, &output.getData(), startRow, startRow + numRows);
            updateInfo.scanCommitted(transaction, output.getData(), numValuesBeforeScan, startRow,
                numRows);
        }
    } break;
    case ResidencyState::IN_MEMORY: {
        if (SCAN_RESIDENCY_STATE == residencyState) {
            output.getData().append(data.get(), startRow, numRows);
            updateInfo.scanCommitted(transaction, output.getData(), numValuesBeforeScan, startRow,
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
    return updateInfo.hasUpdates(transaction, startRow, numRows);
}

void ColumnChunk::lookup(const Transaction* transaction, const ChunkState& state,
    offset_t rowInChunk, ValueVector& output, sel_t posInOutputVector) const {
    switch (getResidencyState()) {
    case ResidencyState::IN_MEMORY: {
        data->lookup(rowInChunk, output, posInOutputVector);
    } break;
    case ResidencyState::ON_DISK: {
        state.column->lookupValue(state, rowInChunk, &output, posInOutputVector);
    } break;
    }
    updateInfo.lookup(transaction, rowInChunk, output, posInOutputVector);
}

void ColumnChunk::update(const Transaction* transaction, offset_t offsetInChunk,
    const ValueVector& values) {
    if (transaction->getType() == TransactionType::DUMMY) {
        data->write(&values, values.state->getSelVector().getSelectedPositions()[0], offsetInChunk);
        return;
    }
    data->updateStats(&values, values.state->getSelVector());
    const auto vectorIdx = offsetInChunk / DEFAULT_VECTOR_CAPACITY;
    const auto rowIdxInVector = offsetInChunk % DEFAULT_VECTOR_CAPACITY;
    auto& vectorUpdateInfo =
        updateInfo.update(data->getMemoryManager(), transaction, vectorIdx, rowIdxInVector, values);
    transaction->pushVectorUpdateInfo(updateInfo, vectorIdx, vectorUpdateInfo,
        transaction->getID());
}

MergedColumnChunkStats ColumnChunk::getMergedColumnChunkStats() const {
    KU_ASSERT(!updateInfo.isSet());
    auto baseStats = data->getMergedColumnChunkStats();
    return baseStats;
}

void ColumnChunk::serialize(Serializer& serializer) const {
    serializer.writeDebuggingInfo("enable_compression");
    serializer.write<bool>(enableCompression);
    data->serialize(serializer);
}

std::unique_ptr<ColumnChunk> ColumnChunk::deserialize(MemoryManager& mm, Deserializer& deSer) {
    std::string key;
    bool enableCompression = false;
    deSer.validateDebuggingInfo(key, "enable_compression");
    deSer.deserializeValue<bool>(enableCompression);
    auto data = ColumnChunkData::deserialize(mm, deSer);
    return std::make_unique<ColumnChunk>(enableCompression, std::move(data));
}

row_idx_t ColumnChunk::getNumUpdatedRows(const Transaction* transaction) const {
    return updateInfo.getNumUpdatedRows(transaction);
}

void ColumnChunk::reclaimStorage(PageAllocator& pageAllocator) const {
    data->reclaimStorage(pageAllocator);
}

} // namespace storage
} // namespace kuzu

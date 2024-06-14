#include "storage/store/column_chunk.h"

#include "common/vector/value_vector.h"
#include "storage/store/column.h"
#include "transaction/transaction.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

ColumnChunk::ColumnChunk(const LogicalType& dataType, uint64_t capacity, bool enableCompression,
    ResidencyState residencyState)
    : residencyState{residencyState}, dataType{dataType}, enableCompression{enableCompression} {
    data = ColumnChunkFactory::createColumnChunkData(dataType, enableCompression, capacity,
        residencyState);
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
}

ColumnChunk::ColumnChunk(const LogicalType& dataType, bool enableCompression,
    ColumnChunkMetadata metadata)
    : residencyState{ResidencyState::ON_DISK}, dataType{dataType},
      enableCompression{enableCompression} {
    data = ColumnChunkFactory::createColumnChunkData(dataType, enableCompression, metadata, true);
}

ColumnChunk::ColumnChunk(bool enableCompression, std::unique_ptr<ColumnChunkData> data)
    : residencyState{data->getResidencyState()}, dataType{data->getDataType()},
      enableCompression{enableCompression}, data{std::move(data)} {}

void ColumnChunk::initializeScanState(ChunkState& state) const {
    data->initializeScanState(state);
}

void ColumnChunk::scan(Transaction* transaction, ChunkState& state, ValueVector& nodeID,
    ValueVector& output, offset_t offsetInChunk, length_t length) const {
    // Check if there is deletions or insertions. If so, update selVector based on transaction.
    switch (residencyState) {
    case ResidencyState::TEMPORARY:
    case ResidencyState::IN_MEMORY: {
        data->scan(output, offsetInChunk, length);
    } break;
    case ResidencyState::ON_DISK: {
        state.column->scan(&DUMMY_READ_TRANSACTION, state, offsetInChunk, length, &nodeID, &output);
    }
    }
    if (updateInfo) {
        auto [startVectorIdx, startOffsetInVector] =
            StorageUtils::getQuotientRemainder(offsetInChunk, DEFAULT_VECTOR_CAPACITY);
        auto [endVectorIdx, endOffsetInVector] =
            StorageUtils::getQuotientRemainder(offsetInChunk + length, DEFAULT_VECTOR_CAPACITY);
        idx_t idx = startVectorIdx;
        sel_t posInVector = 0u;
        while (idx < endVectorIdx) {
            if (auto vectorInfo = updateInfo->getVectorInfo(transaction, idx)) {
                const auto startOffset = idx == startVectorIdx ? startOffsetInVector : 0;
                const auto endOffset =
                    idx == endVectorIdx ? endOffsetInVector : DEFAULT_VECTOR_CAPACITY;
                for (auto i = 0u; i < vectorInfo->numRowsUpdated; i++) {
                    if (vectorInfo->rowsInVector[i] > startOffset) {
                        data->lookup(i, output,
                            posInVector + vectorInfo->rowsInVector[i] - startOffset);
                    }
                }
                posInVector += endOffset - startOffset;
            }
            idx++;
        }
    }
}

void ColumnChunk::scanInMemCommitted(ColumnChunkData& output) const {
    KU_ASSERT(residencyState != ResidencyState::ON_DISK);
    output.append(data.get(), 0, data->getNumValues());
    const auto numVectors = data->getNumValues() / DEFAULT_VECTOR_CAPACITY;
    if (updateInfo) {
        const auto dummyTransaction =
            Transaction{TransactionType::READ_ONLY, 0, Transaction::START_TRANSACTION_ID - 1};
        for (auto vectorIdx = 0u; vectorIdx < numVectors; vectorIdx++) {
            const auto vectorInfo = updateInfo->getVectorInfo(&dummyTransaction, vectorIdx);
            for (auto i = 0u; i < vectorInfo->numRowsUpdated; i++) {
                data->write(vectorInfo->data.get(), i,
                    vectorIdx * DEFAULT_VECTOR_CAPACITY + vectorInfo->rowsInVector[i], 1);
            }
        }
    }
}

template<ResidencyState SCAN_RESIDENCY_STATE>
void ColumnChunk::scanCommitted(Transaction* transaction, ChunkState& chunkState,
    ColumnChunk& output) const {
    auto numValuesBeforeScan = output.getNumValues();
    switch (residencyState) {
    case ResidencyState::ON_DISK: {
        if (SCAN_RESIDENCY_STATE == residencyState) {
            chunkState.column->scan(transaction, chunkState, &output.getData());
            scanCommittedUpdates(transaction, output.getData(), numValuesBeforeScan);
        }
    } break;
    case ResidencyState::IN_MEMORY:
    case ResidencyState::TEMPORARY: {
        if (SCAN_RESIDENCY_STATE == residencyState) {
            output.getData().append(data.get(), 0, data->getNumValues());
            scanCommittedUpdates(transaction, output.getData(), numValuesBeforeScan);
        }
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

template void ColumnChunk::scanCommitted<ResidencyState::ON_DISK>(Transaction* transaction,
    ChunkState& chunkState, ColumnChunk& output) const;
template void ColumnChunk::scanCommitted<ResidencyState::IN_MEMORY>(Transaction* transaction,
    ChunkState& chunkState, ColumnChunk& output) const;
template void ColumnChunk::scanCommitted<ResidencyState::TEMPORARY>(Transaction* transaction,
    ChunkState& chunkState, ColumnChunk& output) const;

void ColumnChunk::scanCommittedUpdates(Transaction* transaction, ColumnChunkData& output,
    offset_t startOffsetInOutput) const {
    if (!updateInfo) {
        return;
    }
    const auto numVectors = getNumValues() / DEFAULT_VECTOR_CAPACITY;
    for (auto vectorIdx = 0u; vectorIdx < numVectors; vectorIdx++) {
        if (const auto vectorInfo = updateInfo->getVectorInfo(transaction, vectorIdx)) {
            for (auto i = 0u; i < vectorInfo->numRowsUpdated; i++) {
                output.write(vectorInfo->data.get(), i,
                    startOffsetInOutput + vectorIdx * DEFAULT_VECTOR_CAPACITY +
                        vectorInfo->rowsInVector[i],
                    1);
            }
        }
    }
}

void ColumnChunk::lookup(Transaction* transaction, offset_t offsetInChunk, ValueVector& output,
    sel_t posInOutputVector) const {
    // TODO: Implement this. Handle updates.
}

void ColumnChunk::update(Transaction* transaction, offset_t offsetInChunk,
    const ValueVector& values) {
    // TODO(Guodong): Pass in MemoryManager into ColumnChunk.
    ValueVector originalVector(dataType, nullptr);
    data->lookup(offsetInChunk, originalVector, 0);
    if (!updateInfo) {
        updateInfo = std::make_unique<UpdateInfo>();
    }
    const auto vectorIdx = offsetInChunk / DEFAULT_VECTOR_CAPACITY;
    const auto rowIdxInVector = offsetInChunk % DEFAULT_VECTOR_CAPACITY;
    const auto vectorUpdateInfo =
        updateInfo->update(transaction, vectorIdx, rowIdxInVector, values);
    transaction->pushVectorUpdateInfo(*updateInfo, vectorIdx, *vectorUpdateInfo);
}

void ColumnChunk::serialize(Serializer& serializer) const {
    serializer.write<bool>(enableCompression);
    data->serialize(serializer);
}

std::unique_ptr<ColumnChunk> ColumnChunk::deserialize(Deserializer& deSer) {
    bool enableCompression;
    deSer.deserializeValue<bool>(enableCompression);
    auto data = ColumnChunkData::deserialize(deSer);
    return std::make_unique<ColumnChunk>(enableCompression, std::move(data));
}

} // namespace storage
} // namespace kuzu

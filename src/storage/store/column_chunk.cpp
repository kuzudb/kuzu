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
    data = ColumnChunkFactory::createColumnChunkData(dataType, enableCompression, metadata);
}

ColumnChunk::ColumnChunk(bool enableCompression, std::unique_ptr<ColumnChunkData> data)
    : residencyState{data->getResidencyState()}, dataType{data->getDataType()},
      enableCompression{enableCompression}, data{std::move(data)} {}

void ColumnChunk::initializeScanState(ChunkState& state) const {
    data->initializeScanState(state);
}

// 100, 2148
// 0-100 == 0-2048
// 1-0 == 2-100
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
    // TODO:
    // Check if there are updates visible to the transaction. Merge updates if so.
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

} // namespace storage
} // namespace kuzu

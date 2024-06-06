#include "storage/store/column_chunk.h"

#include "common/vector/value_vector.h"
#include <transaction/transaction.h>

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

ColumnChunk::ColumnChunk(const LogicalType& dataType, uint64_t capacity, bool enableCompression,
    ResidencyState type)
    : dataType{dataType}, enableCompression{enableCompression} {
    data = ColumnChunkFactory::createColumnChunkData(dataType, enableCompression, capacity, type);
    KU_ASSERT(type == ResidencyState::TEMPORARY || type == ResidencyState::IN_MEMORY);
}

ColumnChunk::ColumnChunk(const LogicalType& dataType, bool enableCompression,
    ColumnChunkMetadata metadata)
    : dataType{dataType}, enableCompression{enableCompression} {
    data = std::make_unique<ColumnChunkData>(dataType, enableCompression, std::move(metadata));
}

ColumnChunk::ColumnChunk(bool enableCompression, std::unique_ptr<ColumnChunkData> data)
    : dataType{data->getDataType()}, enableCompression{enableCompression}, data{std::move(data)} {}

void ColumnChunk::initializeScanState(ChunkState& state) const {
    data->initializeScanState(state);
}

void ColumnChunk::scan(Transaction* transaction, ValueVector& output, offset_t offsetInChunk,
    length_t length) const {
    // TODO: Implement this. Handle updates.
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

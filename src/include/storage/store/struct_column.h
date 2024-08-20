#pragma once

#include "storage/store/column.h"

namespace kuzu {
namespace storage {

class StructColumn final : public Column {
public:
    StructColumn(std::string name, common::LogicalType dataType, BMFileHandle* dataFH,
        BufferManager* bufferManager, ShadowFile* shadowFile, bool enableCompression);

    static std::unique_ptr<ColumnChunkData> flushChunkData(const ColumnChunkData& chunk,
        BMFileHandle& dataFH);

    void scan(transaction::Transaction* transaction, const ChunkState& state,
        ColumnChunkData* columnChunk, common::offset_t startOffset = 0,
        common::offset_t endOffset = common::INVALID_OFFSET) override;
    void scan(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector) override;

    Column* getChild(common::idx_t childIdx) const {
        KU_ASSERT(childIdx < childColumns.size());
        return childColumns[childIdx].get();
    }
    void write(ColumnChunkData& persistentChunk, const ChunkState& state,
        common::offset_t offsetInChunk, ColumnChunkData* data, common::offset_t dataOffset,
        common::length_t numValues) override;

    void checkpointColumnChunk(ColumnCheckpointState& checkpointState) override;

protected:
    void scanInternal(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInChunk, common::row_idx_t numValuesToScan,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector) override;

    void lookupInternal(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t nodeOffset, common::ValueVector* resultVector,
        uint32_t posInVector) override;

private:
    std::vector<std::unique_ptr<Column>> childColumns;
};

} // namespace storage
} // namespace kuzu

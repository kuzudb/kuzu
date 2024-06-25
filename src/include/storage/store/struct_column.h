#pragma once

#include "storage/store/column.h"

namespace kuzu {
namespace storage {

class StructColumn final : public Column {
public:
    StructColumn(std::string name, common::LogicalType dataType, BMFileHandle* dataFH,
        BufferManager* bufferManager, WAL* wal, bool enableCompression);

    static std::unique_ptr<ColumnChunkData> flushChunkData(const ColumnChunkData& chunk,
        BMFileHandle& dataFH);

    void scan(transaction::Transaction* transaction, const ChunkState& state,
        ColumnChunkData* columnChunk, common::offset_t startOffset = 0,
        common::offset_t endOffset = common::INVALID_OFFSET) override;
    void scan(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector) override;

    void append(ColumnChunkData* columnChunk, ChunkState& state) override;

    Column* getChild(common::idx_t childIdx) const {
        KU_ASSERT(childIdx < childColumns.size());
        return childColumns[childIdx].get();
    }
    void write(ChunkState& state, common::offset_t offsetInChunk,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override;
    void write(ChunkState& state, common::offset_t offsetInChunk, ColumnChunkData* data,
        common::offset_t dataOffset, common::length_t numValues) override;

    // void prepareCommitForExistingChunk(transaction::Transaction* transaction, ChunkState& state,
    // const ChunkDataCollection& localInsertChunks, const offset_to_row_idx_t& insertInfo,
    // const ChunkDataCollection& localUpdateChunks, const offset_to_row_idx_t& updateInfo,
    // const offset_set_t& deleteInfo) override;
    void prepareCommitForExistingChunk(transaction::Transaction* transaction, ChunkState& state,
        const std::vector<common::offset_t>& dstOffsets, ColumnChunkData* chunk,
        common::offset_t startSrcOffset) override;

protected:
    void scanInternal(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInChunk, common::row_idx_t numValuesToScan,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector) override;
    void lookupInternal(transaction::Transaction* transaction, ChunkState& state,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector) override;

private:
    // static ChunkDataCollection getStructChildChunkCollection(
    // const ChunkDataCollection& chunkCollection, common::idx_t childIdx);

private:
    std::vector<std::unique_ptr<Column>> childColumns;
};

} // namespace storage
} // namespace kuzu

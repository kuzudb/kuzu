#pragma once

#include "storage/store/column.h"

namespace kuzu {
namespace storage {

class StructColumn final : public Column {
public:
    StructColumn(std::string name, common::LogicalType dataType,
        const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
        BufferManager* bufferManager, WAL* wal, transaction::Transaction* transaction,
        RWPropertyStats propertyStatistics, bool enableCompression);

    void initChunkState(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, ChunkState& columnReadState) override;
    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        ColumnChunk* columnChunk, common::offset_t startOffset = 0,
        common::offset_t endOffset = common::INVALID_OFFSET) override;
    void scan(transaction::Transaction* transaction, ChunkState& readState,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector) override;

    void append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) override;

    void checkpointInMemory() override;
    void rollbackInMemory() override;
    void prepareCommit() override;

    inline Column* getChild(common::vector_idx_t childIdx) {
        KU_ASSERT(childIdx < childColumns.size());
        return childColumns[childIdx].get();
    }
    void write(ChunkState& state, common::offset_t offsetInChunk,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override;
    void write(ChunkState& state, common::offset_t offsetInChunk, ColumnChunk* data,
        common::offset_t dataOffset, common::length_t numValues) override;

    void prepareCommitForExistingChunk(transaction::Transaction* transaction, ChunkState& state,
        const ChunkCollection& localInsertChunks, const offset_to_row_idx_t& insertInfo,
        const ChunkCollection& localUpdateChunks, const offset_to_row_idx_t& updateInfo,
        const offset_set_t& deleteInfo) override;
    void prepareCommitForExistingChunk(transaction::Transaction* transaction, ChunkState& state,
        const std::vector<common::offset_t>& dstOffsets, ColumnChunk* chunk,
        common::offset_t startSrcOffset) override;

protected:
    void scanInternal(transaction::Transaction* transaction, ChunkState& readState,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector) override;
    void lookupInternal(transaction::Transaction* transaction, ChunkState& readState,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector) override;

private:
    static ChunkCollection getStructChildChunkCollection(const ChunkCollection& chunkCollection,
        common::vector_idx_t childIdx);

private:
    std::vector<std::unique_ptr<Column>> childColumns;
};

} // namespace storage
} // namespace kuzu

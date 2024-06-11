#pragma once

#include "storage/store/dictionary_column.h"

namespace kuzu {
namespace storage {

class StringColumn final : public Column {
public:
    enum class ChildStateIndex : common::idx_t { DATA = 0, OFFSET = 1, INDEX = 2 };
    static constexpr size_t CHILD_STATE_COUNT = 3;

public:
    StringColumn(std::string name, common::LogicalType dataType,
        const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH,
        DiskArrayCollection& metadataDAC, BufferManager* bufferManager, WAL* wal,
        transaction::Transaction* transaction, bool enableCompression);

    void initChunkState(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, ChunkState& chunkState) override;

    void scan(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector = 0) override;
    void scan(transaction::Transaction* transaction, const ChunkState& state,
        ColumnChunkData* columnChunk, common::offset_t startOffset = 0,
        common::offset_t endOffset = common::INVALID_OFFSET) override;

    void append(ColumnChunkData* columnChunk, ChunkState& state) override;

    void writeValue(ChunkState& state, common::offset_t offsetInChunk,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override;

    void write(ChunkState& state, common::offset_t offsetInChunk, ColumnChunkData* data,
        common::offset_t dataOffset, common::length_t numValues) override;

    void prepareCommit() override;
    void checkpointInMemory() override;
    void rollbackInMemory() override;

    void prepareCommitForExistingChunk(transaction::Transaction* transaction, ChunkState& state,
        const ChunkCollection& localInsertChunks, const offset_to_row_idx_t& insertInfo,
        const ChunkCollection& localUpdateChunks, const offset_to_row_idx_t& updateInfo,
        const offset_set_t& deleteInfo) override;
    void prepareCommitForExistingChunk(transaction::Transaction* transaction, ChunkState& state,
        const std::vector<common::offset_t>& dstOffsets, ColumnChunkData* chunk,
        common::offset_t startSrcOffset) override;

    const DictionaryColumn& getDictionary() const { return dictionary; }

    static ChunkState& getChildState(ChunkState& state, ChildStateIndex child);
    static const ChunkState& getChildState(const ChunkState& state, ChildStateIndex child);

protected:
    void scanInternal(transaction::Transaction* transaction, const ChunkState& state,
        common::idx_t vectorIdx, common::row_idx_t numValuesToScan,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector) override;
    void scanUnfiltered(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInChunk, common::offset_t numValuesToRead,
        common::ValueVector* resultVector, common::sel_t startPosInVector = 0);
    void scanFiltered(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInChunk, const common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector);

    void lookupInternal(transaction::Transaction* transaction, ChunkState& state,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector) override;

private:
    bool canCommitInPlace(const ChunkState& state, const ChunkCollection& localInsertChunk,
        const offset_to_row_idx_t& insertInfo, const ChunkCollection& localUpdateChunk,
        const offset_to_row_idx_t& updateInfo) override;
    bool canCommitInPlace(const ChunkState& state, const std::vector<common::offset_t>& dstOffsets,
        ColumnChunkData* chunk, common::offset_t srcOffset) override;

    bool canIndexCommitInPlace(const ChunkState& dataState, uint64_t numStrings,
        common::offset_t maxOffset);

    void updateStateMetadataNumValues(ChunkState& state, size_t numValues) override;

private:
    // Main column stores indices of values in the dictionary
    DictionaryColumn dictionary;

    std::unique_ptr<Column> indexColumn;
};

} // namespace storage
} // namespace kuzu

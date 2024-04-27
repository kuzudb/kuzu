#pragma once

#include "storage/store/dictionary_column.h"

namespace kuzu {
namespace storage {

class StringColumn final : public Column {
public:
    StringColumn(std::string name, common::LogicalType dataType,
        const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
        BufferManager* bufferManager, WAL* wal, transaction::Transaction* transaction,
        RWPropertyStats propertyStatistics, bool enableCompression);

    void initChunkState(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, ChunkState& columnReadState) override;

    void scan(transaction::Transaction* transaction, ChunkState& readState,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector = 0) override;
    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        ColumnChunk* columnChunk, common::offset_t startOffset = 0,
        common::offset_t endOffset = common::INVALID_OFFSET) override;

    void append(ColumnChunk* columnChunk, common::node_group_idx_t nodeGroupIdx) override;

    void writeValue(ChunkState& state, common::offset_t offsetInChunk,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override;

    void write(ChunkState& state, common::offset_t offsetInChunk, ColumnChunk* data,
        common::offset_t dataOffset, common::length_t numValues) override;

    void prepareCommit() override;
    void checkpointInMemory() override;
    void rollbackInMemory() override;

    inline const DictionaryColumn& getDictionary() const { return dictionary; }

protected:
    void scanInternal(transaction::Transaction* transaction, ChunkState& readState,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector) override;
    void scanUnfiltered(transaction::Transaction* transaction, ChunkState& readState,
        common::offset_t startOffsetInChunk, common::offset_t numValuesToRead,
        common::ValueVector* resultVector, common::sel_t startPosInVector = 0);
    void scanFiltered(transaction::Transaction* transaction, ChunkState& readState,
        common::offset_t startOffsetInChunk, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector);

    void lookupInternal(transaction::Transaction* transaction, ChunkState& readState,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector) override;

private:
    bool canCommitInPlace(const ChunkState& state, const ChunkCollection& localInsertChunk,
        const offset_to_row_idx_t& insertInfo, const ChunkCollection& localUpdateChunk,
        const offset_to_row_idx_t& updateInfo) override;
    bool canCommitInPlace(const ChunkState& state, const std::vector<common::offset_t>& dstOffsets,
        ColumnChunk* chunk, common::offset_t srcOffset) override;

    bool canIndexCommitInPlace(const Column::ChunkState& dataState, uint64_t numStrings,
        common::offset_t maxOffset);

private:
    // Main column stores indices of values in the dictionary
    DictionaryColumn dictionary;
};

} // namespace storage
} // namespace kuzu

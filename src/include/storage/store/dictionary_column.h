#pragma once

#include "dictionary_chunk.h"
#include "storage/store/column.h"

namespace kuzu {
namespace storage {

class DictionaryColumn {
public:
    static constexpr common::vector_idx_t DATA_COLUMN_CHILD_READ_STATE_IDX = 0;
    static constexpr common::vector_idx_t OFFSET_COLUMN_CHILD_READ_STATE_IDX = 1;

    DictionaryColumn(const std::string& name, const MetadataDAHInfo& metaDAHeaderInfo,
        BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        transaction::Transaction* transaction, RWPropertyStats stats, bool enableCompression);

    void initChunkState(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, Column::ChunkState& columnReadState);

    void append(common::node_group_idx_t nodeGroupIdx, const DictionaryChunk& dictChunk);

    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        DictionaryChunk& dictChunk);
    // Offsets to scan should be a sorted list of pairs mapping the index of the entry in the string
    // dictionary (as read from the index column) to the output index in the result vector to store
    // the string.
    void scan(transaction::Transaction* transaction, const Column::ChunkState& offsetState,
        const Column::ChunkState& dataState,
        std::vector<std::pair<DictionaryChunk::string_index_t, uint64_t>>& offsetsToScan,
        common::ValueVector* resultVector, const ColumnChunkMetadata& indexMeta);

    DictionaryChunk::string_index_t append(Column::ChunkState& state, std::string_view val);

    bool canCommitInPlace(const Column::ChunkState& state, uint64_t numNewStrings,
        uint64_t totalStringLengthToAdd);

    void prepareCommit();
    void checkpointInMemory();
    void rollbackInMemory();

    inline Column* getDataColumn() const { return dataColumn.get(); }
    inline Column* getOffsetColumn() const { return offsetColumn.get(); }

private:
    void scanOffsets(transaction::Transaction* transaction, const Column::ChunkState& state,
        DictionaryChunk::string_offset_t* offsets, uint64_t index, uint64_t numValues,
        uint64_t dataSize);
    void scanValueToVector(transaction::Transaction* transaction,
        const Column::ChunkState& dataState, uint64_t startOffset, uint64_t endOffset,
        common::ValueVector* resultVector, uint64_t offsetInVector);

    bool canDataCommitInPlace(const Column::ChunkState& dataState, uint64_t totalStringLengthToAdd);
    bool canOffsetCommitInPlace(const Column::ChunkState& offsetState,
        const Column::ChunkState& dataState, uint64_t numNewStrings,
        uint64_t totalStringLengthToAdd);

private:
    // The offset column stores the offsets for each index, and the data column stores the data in
    // order. Values are never removed from the dictionary during in-place updates, only appended to
    // the end.
    std::unique_ptr<Column> dataColumn;
    std::unique_ptr<Column> offsetColumn;
};

} // namespace storage
} // namespace kuzu

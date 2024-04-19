#pragma once

#include "column.h"

// List is a nested data type which is stored as three chunks:
// 1. Offset column (type: INT64). Using offset to partition the data column into multiple lists.
// 2. Size column. Stores the size of each list.
// 3. Data column. Stores the actual data of the list.
// Similar to other data types, nulls are stored in the null column.
// Example layout for list of INT64:
// Four lists: [4,7,8,12], null, [2, 3], []
// Offset column: [4, 4, 6, 6]
// Size column: [4, 0, 2, 0]
// data column: [4, 7, 8, 12, 2, 3]
// When updating the data, we first append the data to the data column, and then update the offset
// and size accordingly. Besides offset column, we introduce an extra size column here to enable
// in-place updates of a list column. In a list column chunk, offsets of lists are not always sorted
// after updates. This is good for writes, but it introduces extra overheads for scans, as lists can
// be scattered, and scans have to be broken into multiple small reads. To achieve a balance between
// reads and writes, during updates, we rewrite the whole list column chunk in ascending order
// when the offsets are not sorted in ascending order and the size of data column chunk is larger
// than half of its capacity.

namespace kuzu {
namespace storage {

struct ListOffsetSizeInfo {
    common::offset_t numTotal;
    std::unique_ptr<ColumnChunk> offsetColumnChunk;
    std::unique_ptr<ColumnChunk> sizeColumnChunk;

    ListOffsetSizeInfo(common::offset_t numTotal, std::unique_ptr<ColumnChunk> offsetColumnChunk,
        std::unique_ptr<ColumnChunk> sizeColumnChunk)
        : numTotal{numTotal}, offsetColumnChunk{std::move(offsetColumnChunk)},
          sizeColumnChunk{std::move(sizeColumnChunk)} {}

    common::list_size_t getListSize(uint64_t pos) const;
    common::offset_t getListEndOffset(uint64_t pos) const;
    common::offset_t getListStartOffset(uint64_t pos) const;

    bool isOffsetSortedAscending(uint64_t startPos, uint64_t endPos) const;
};

class ListColumn final : public Column {
    friend class ListLocalColumn;

    static constexpr common::vector_idx_t SIZE_COLUMN_CHILD_READ_STATE_IDX = 0;
    static constexpr common::vector_idx_t DATA_COLUMN_CHILD_READ_STATE_IDX = 1;

public:
    ListColumn(std::string name, common::LogicalType dataType,
        const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
        BufferManager* bufferManager, WAL* wal, transaction::Transaction* transaction,
        RWPropertyStats propertyStatistics, bool enableCompression);

    void initReadState(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t startOffsetInChunk, ReadState& columnReadState) override;
    void scan(transaction::Transaction* transaction, ReadState& readState,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector = 0) override;

    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        ColumnChunk* columnChunk, common::offset_t startOffset = 0,
        common::offset_t endOffset = common::INVALID_OFFSET) override;

    inline Column* getDataColumn() { return dataColumn.get(); }

protected:
    void scanInternal(transaction::Transaction* transaction, ReadState& readState,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector) override;

    void lookupValue(transaction::Transaction* transaction, ReadState& readState,
        common::offset_t nodeOffset, common::ValueVector* resultVector,
        uint32_t posInVector) override;

    void append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) override;

private:
    void scanUnfiltered(transaction::Transaction* transaction, ReadState& readState,
        common::ValueVector* resultVector, const ListOffsetSizeInfo& listOffsetInfoInStorage);
    void scanFiltered(transaction::Transaction* transaction, ReadState& readState,
        common::ValueVector* offsetVector, const ListOffsetSizeInfo& listOffsetInfoInStorage);

    void prepareCommit() override;
    void checkpointInMemory() override;
    void rollbackInMemory() override;

    common::offset_t readOffset(transaction::Transaction* transaction, const ReadState& readState,
        common::offset_t offsetInNodeGroup);
    common::list_size_t readSize(transaction::Transaction* transaction, const ReadState& readState,
        common::offset_t offsetInNodeGroup);

    ListOffsetSizeInfo getListOffsetSizeInfo(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::offset_t startOffsetInNodeGroup,
        common::offset_t endOffsetInNodeGroup);

    void prepareCommitForChunk(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, const ChunkCollection& localInsertChunks,
        const offset_to_row_idx_t& insertInfo, const ChunkCollection& localUpdateChunks,
        const offset_to_row_idx_t& updateInfo, const offset_set_t& deleteInfo) override;
    void prepareCommitForChunk(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, const std::vector<common::offset_t>& dstOffsets,
        ColumnChunk* chunk, common::offset_t startSrcOffset) override;

    void prepareCommitForOffsetChunk(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, const std::vector<common::offset_t>& dstOffsets,
        ColumnChunk* chunk, common::offset_t startSrcOffset);

    void commitOffsetColumnChunkOutOfPlace(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, const std::vector<common::offset_t>& dstOffsets,
        ColumnChunk* chunk, common::offset_t startSrcOffset);

    void commitOffsetColumnChunkInPlace(common::node_group_idx_t nodeGroupIdx,
        const std::vector<common::offset_t>& dstOffsets, ColumnChunk* chunk,
        common::offset_t srcOffset);

private:
    std::unique_ptr<Column> sizeColumn;
    std::unique_ptr<Column> dataColumn;
};

} // namespace storage
} // namespace kuzu

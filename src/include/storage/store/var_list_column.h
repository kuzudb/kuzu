#pragma once

#include "column.h"
#include "var_list_column_chunk.h"

// List is a nested data type which is stored as two chunks:
// 1. Offset column (type: INT64). Using offset to partition the data column into multiple lists.
// 2. Data column. Stores the actual data of the list.
// Similar to other data types, nulls are stored in the null column.
// Example layout for list of INT64:
// Four lists: [4,7,8,12], null, [2, 3], []
// Offset column: [4, 4, 6, 6]
// data column: [4, 7, 8, 12, 2, 3]
// When reading the data, we firstly read the offset column and utilize the offset column to find
// the data column partitions.
// 1st list(offset 4): Since it is the first element, the start offset is a constant 0, and the end
// offset is 4. Its data is stored at position 0-4 in the data column.
// 2nd list(offset 4): By reading the null column, we know that the 2nd list is null. So we don't
// need to read from the data column.
// 3rd list(offset 6): The start offset is 4(by looking up the offset for the 2nd list), and the end
// offset is 6. Its data is stored at position 4-6 in the data column.
// 4th list(offset 6): The start offset is 6(by looking up the offset for the 3rd list), and the end
// offset is 6. Its data is stored at position 6-6 in the data column (empty list).

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

    uint32_t getListLength(uint64_t pos) const;
    common::offset_t getListEndOffset(uint64_t pos) const;
    common::offset_t getListStartOffset(uint64_t pos) const;

    bool isOffsetSortedAscending(uint64_t startPos, uint64_t endPos) const;
};

class VarListColumn : public Column {
    friend class VarListLocalColumn;

public:
    VarListColumn(std::string name, common::LogicalType dataType,
        const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
        BufferManager* bufferManager, WAL* wal, transaction::Transaction* transaction,
        RWPropertyStats propertyStatistics, bool enableCompression);

    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector = 0) final;

    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        ColumnChunk* columnChunk, common::offset_t startOffset = 0,
        common::offset_t endOffset = common::INVALID_OFFSET) final;

    inline Column* getDataColumn() { return dataColumn.get(); }

protected:
    void scanInternal(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;

    void lookupValue(transaction::Transaction* transaction, common::offset_t nodeOffset,
        common::ValueVector* resultVector, uint32_t posInVector) final;

    void append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) override;

private:
    void scanUnfiltered(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::ValueVector* resultVector,
        const ListOffsetSizeInfo& listOffsetInfoInStorage);
    void scanFiltered(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::ValueVector* offsetVector, const ListOffsetSizeInfo& listOffsetInfoInStorage);

    inline bool canCommitInPlace(transaction::Transaction*, common::node_group_idx_t,
        const ChunkCollection&, const offset_to_row_idx_t&, const ChunkCollection&,
        const offset_to_row_idx_t&) override {
        // Always perform out-of-place commit for VAR_LIST columns.
        return false;
    }
    inline bool canCommitInPlace(transaction::Transaction* /*transaction*/,
        common::node_group_idx_t /*nodeGroupIdx*/,
        const std::vector<common::offset_t>& /*dstOffsets*/, ColumnChunk* /*chunk*/,
        common::offset_t /*startOffset*/) override {
        // Always perform out-of-place commit for VAR_LIST columns.
        return false;
    }

    void checkpointInMemory() final;
    void rollbackInMemory() final;

    common::offset_t readOffset(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::offset_t offsetInNodeGroup);

    uint32_t readSize(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t offsetInNodeGroup);

    ListOffsetSizeInfo getListOffsetSizeInfo(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::offset_t startOffsetInNodeGroup,
        common::offset_t endOffsetInNodeGroup);

private:
    std::unique_ptr<Column> sizeColumn;
    std::unique_ptr<Column> dataColumn;
    std::unique_ptr<VarListDataColumnChunk> tmpDataColumnChunk;
};

} // namespace storage
} // namespace kuzu

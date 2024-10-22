#pragma once

#include "column.h"
#include "storage/buffer_manager/memory_manager.h"

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
    std::unique_ptr<ColumnChunkData> offsetColumnChunk;
    std::unique_ptr<ColumnChunkData> sizeColumnChunk;

    ListOffsetSizeInfo(common::offset_t numTotal,
        std::unique_ptr<ColumnChunkData> offsetColumnChunk,
        std::unique_ptr<ColumnChunkData> sizeColumnChunk)
        : numTotal{numTotal}, offsetColumnChunk{std::move(offsetColumnChunk)},
          sizeColumnChunk{std::move(sizeColumnChunk)} {}

    common::list_size_t getListSize(uint64_t pos) const;
    common::offset_t getListEndOffset(uint64_t pos) const;
    common::offset_t getListStartOffset(uint64_t pos) const;

    bool isOffsetSortedAscending(uint64_t startPos, uint64_t endPos) const;
};

class ListColumn final : public Column {
    static constexpr common::idx_t SIZE_COLUMN_CHILD_READ_STATE_IDX = 0;
    static constexpr common::idx_t DATA_COLUMN_CHILD_READ_STATE_IDX = 1;
    static constexpr common::idx_t OFFSET_COLUMN_CHILD_READ_STATE_IDX = 2;
    static constexpr size_t CHILD_COLUMN_COUNT = 3;

public:
    ListColumn(std::string name, common::LogicalType dataType, FileHandle* dataFH,
        MemoryManager* mm, ShadowFile* shadowFile, bool enableCompression);

    static std::unique_ptr<ColumnChunkData> flushChunkData(const ColumnChunkData& chunk,
        FileHandle& dataFH);

    void scan(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector = 0) const override;
    void scan(transaction::Transaction* transaction, const ChunkState& state,
        ColumnChunkData* columnChunk, common::offset_t startOffset = 0,
        common::offset_t endOffset = common::INVALID_OFFSET) const override;

    Column* getOffsetColumn() const { return offsetColumn.get(); }
    Column* getSizeColumn() const { return sizeColumn.get(); }
    Column* getDataColumn() const { return dataColumn.get(); }

    void checkpointColumnChunk(ColumnCheckpointState& checkpointState) override;

protected:
    void scanInternal(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInChunk, common::row_idx_t numValuesToScan,
        common::ValueVector* resultVector) const override;

    void lookupInternal(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t nodeOffset, common::ValueVector* resultVector,
        uint32_t posInVector) const override;

private:
    void scanUnfiltered(transaction::Transaction* transaction, const ChunkState& state,
        common::ValueVector* resultVector, uint64_t numValuesToScan,
        const ListOffsetSizeInfo& listOffsetInfoInStorage) const;
    void scanFiltered(transaction::Transaction* transaction, const ChunkState& state,
        common::ValueVector* offsetVector, const ListOffsetSizeInfo& listOffsetInfoInStorage) const;

    common::offset_t readOffset(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t offsetInNodeGroup) const;
    common::list_size_t readSize(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t offsetInNodeGroup) const;

    ListOffsetSizeInfo getListOffsetSizeInfo(transaction::Transaction* transaction,
        const ChunkState& state, common::offset_t startOffsetInNodeGroup,
        common::offset_t endOffsetInNodeGroup) const;

private:
    std::unique_ptr<Column> offsetColumn;
    std::unique_ptr<Column> sizeColumn;
    std::unique_ptr<Column> dataColumn;
};

} // namespace storage
} // namespace kuzu

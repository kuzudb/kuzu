#pragma once

#include "storage/store/column.h"

namespace kuzu {
namespace storage {

class StringColumn final : public Column {
public:
    using string_offset_t = uint64_t;
    using string_index_t = uint32_t;
    StringColumn(std::unique_ptr<common::LogicalType> dataType,
        const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
        BufferManager* bufferManager, WAL* wal, transaction::Transaction* transaction,
        RWPropertyStats propertyStatistics, bool enableCompression);

    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector = 0) override;
    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        ColumnChunk* columnChunk) override;

    void append(ColumnChunk* columnChunk, common::node_group_idx_t nodeGroupIdx) override;

    void writeValue(const ColumnChunkMetadata& chunkMeta, common::node_group_idx_t nodeGroupIdx,
        common::offset_t offsetInChunk, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom) override;

    inline Column* getDataColumn() { return dataColumn.get(); }
    inline Column* getOffsetColumn() { return offsetColumn.get(); }

    void checkpointInMemory() override;
    void rollbackInMemory() override;

protected:
    void scanInternal(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) override;
    void scanUnfiltered(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::offset_t startOffsetInGroup,
        common::offset_t endOffsetInGroup, common::ValueVector* resultVector,
        common::sel_t startPosInVector = 0);
    void scanFiltered(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t startOffsetInGroup, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector);

    void lookupInternal(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) override;

    void scanValueToVector(transaction::Transaction* transaction, const ReadState& dataState,
        uint64_t startOffset, uint64_t endOffset, common::ValueVector* resultVector,
        uint64_t offsetInVector);
    void scanOffsets(transaction::Transaction* transaction, const ReadState& state,
        uint64_t* offsets, uint64_t index, uint64_t numValues, uint64_t dataSize);
    // Offsets to scan should be a sorted list of pairs mapping the index of the entry in the string
    // dictionary (as read from the index column) to the output index in the result vector to store
    // the string.
    void scanValuesToVector(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx,
        std::vector<std::pair<string_index_t, uint64_t>>& offsetsToScan,
        common::ValueVector* resultVector, const ReadState& indexState);

private:
    bool canCommitInPlace(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, LocalVectorCollection* localChunk,
        const offset_to_row_idx_t& insertInfo, const offset_to_row_idx_t& updateInfo) override;

private:
    // Main column stores indices of values in the dictionary
    // The offset column stores the offsets for each index, and the data column stores the data in
    // order. Values are never removed from the dictionary during in-place updates, only appended to
    // the end.
    std::unique_ptr<Column> dataColumn;
    std::unique_ptr<Column> offsetColumn;
};

} // namespace storage
} // namespace kuzu

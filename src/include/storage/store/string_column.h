#pragma once

#include "storage/store/column.h"

namespace kuzu {
namespace storage {

class StringColumn : public Column {
public:
    using string_offset_t = uint64_t;
    using string_index_t = uint32_t;
    StringColumn(std::unique_ptr<common::LogicalType> dataType,
        const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
        BufferManager* bufferManager, WAL* wal, transaction::Transaction* transaction,
        RWPropertyStats propertyStatistics);

    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector = 0) final;
    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        ColumnChunk* columnChunk) final;

    void append(ColumnChunk* columnChunk, common::node_group_idx_t nodeGroupIdx) final;

    void writeValue(const ColumnChunkMetadata& chunkMeta, common::offset_t nodeOffset,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) final;

    inline Column* getDataColumn() { return dataColumn.get(); }
    inline Column* getOffsetColumn() { return offsetColumn.get(); }

    void checkpointInMemory() final;
    void rollbackInMemory() final;

protected:
    void scanInternal(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;
    void scanUnfiltered(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::offset_t startOffsetInGroup,
        common::offset_t endOffsetInGroup, common::ValueVector* resultVector,
        uint64_t startPosInVector = 0);
    void scanFiltered(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t startOffsetInGroup, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector);

    void lookupInternal(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;

    void scanValueToVector(transaction::Transaction* transaction, const ReadState& dataState,
        uint64_t startOffset, uint64_t endOffset, common::ValueVector* resultVector,
        uint64_t offsetInVector);
    void scanOffsets(transaction::Transaction* transaction, const ReadState& state,
        uint64_t* offsets, uint64_t index, uint64_t dataSize);

private:
    void readStringValueFromOvf(transaction::Transaction* transaction, common::ku_string_t& kuStr,
        common::ValueVector* resultVector, common::page_idx_t overflowPageIdx);
    bool canCommitInPlace(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, LocalVectorCollection* localChunk) final;

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

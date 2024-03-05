#pragma once

#include "column.h"

namespace kuzu {
namespace storage {

struct ListOffsetInfoInStorage2 {
    common::offset_t prevNodeListOffset;
    std::vector<std::unique_ptr<common::ValueVector>> offsetVectors;

    ListOffsetInfoInStorage2(common::offset_t prevNodeListOffset,
        std::vector<std::unique_ptr<common::ValueVector>> offsetVectors)
        : prevNodeListOffset{prevNodeListOffset}, offsetVectors{std::move(offsetVectors)} {}

    common::offset_t getListOffset(uint64_t nodePos) const;

    inline uint64_t getListLength(uint64_t nodePos) const {
        KU_ASSERT(getListOffset(nodePos + 1) >= getListOffset(nodePos));
        return getListOffset(nodePos + 1) - getListOffset(nodePos);
    }
};

class ArrayColumn : public Column {
    friend class VarListLocalColumn;

public:
    ArrayColumn(std::string name, common::LogicalType dataType,
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
    inline common::offset_t readListOffsetInStorage(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::offset_t offsetInNodeGroup) {
        return offsetInNodeGroup == 0 ?
                   0 :
                   readOffset(transaction, nodeGroupIdx, offsetInNodeGroup - 1);
    }

    void scanUnfiltered(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::ValueVector* resultVector,
        const ListOffsetInfoInStorage2& listOffsetInfoInStorage);
    void scanFiltered(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::ValueVector* offsetVector, const ListOffsetInfoInStorage2& listOffsetInfoInStorage);

    inline bool canCommitInPlace(transaction::Transaction* /*transaction*/,
        common::node_group_idx_t /*nodeGroupIdx*/, LocalVectorCollection* /*localChunk*/,
        const offset_to_row_idx_t& /*insertInfo*/,
        const offset_to_row_idx_t& /*updateInfo*/) override {
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
    ListOffsetInfoInStorage2 getListOffsetInfoInStorage(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::offset_t startOffsetInNodeGroup,
        common::offset_t endOffsetInNodeGroup,
        const std::shared_ptr<common::DataChunkState>& state);

private:
    std::unique_ptr<Column> dataColumn;
};

} // namespace storage
} // namespace kuzu

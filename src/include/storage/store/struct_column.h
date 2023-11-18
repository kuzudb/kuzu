#pragma once

#include "storage/store/column.h"

namespace kuzu {
namespace storage {

class StructColumn final : public Column {
public:
    StructColumn(std::unique_ptr<common::LogicalType> dataType,
        const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
        BufferManager* bufferManager, WAL* wal, transaction::Transaction* transaction,
        RWPropertyStats propertyStatistics, bool enableCompression);

    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        ColumnChunk* columnChunk) override;
    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector) override;

    void append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) override;

    void checkpointInMemory() override;
    void rollbackInMemory() override;

    inline Column* getChild(common::vector_idx_t childIdx) {
        KU_ASSERT(childIdx < childColumns.size());
        return childColumns[childIdx].get();
    }
    void write(common::node_group_idx_t nodeGroupIdx, common::offset_t offsetInChunk,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override;
    void setNull(common::node_group_idx_t nodeGroupIdx, common::offset_t offsetInChunk) override;

    void prepareCommitForChunk(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, LocalVectorCollection* localColumnChunk,
        const offset_to_row_idx_t& insertInfo, const offset_to_row_idx_t& updateInfo,
        const offset_set_t& deleteInfo) override;

protected:
    void scanInternal(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) override;
    void lookupInternal(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) override;

private:
    std::vector<std::unique_ptr<Column>> childColumns;
};

} // namespace storage
} // namespace kuzu

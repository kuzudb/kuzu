#pragma once

#include "storage/stats/table_statistics.h"
#include "storage/store/node_column.h"

namespace kuzu {
namespace storage {

class StructNodeColumn : public NodeColumn {
public:
    StructNodeColumn(common::LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
        BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        transaction::Transaction* transaction, RWPropertyStats propertyStatistics,
        bool enableCompression);

    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector) final;

    void append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) final;

    void checkpointInMemory() final;
    void rollbackInMemory() final;

    inline NodeColumn* getChild(common::vector_idx_t childIdx) {
        assert(childIdx < childColumns.size());
        return childColumns[childIdx].get();
    }
    void write(common::offset_t nodeOffset, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom) final;

protected:
    void scanInternal(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;
    void lookupInternal(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;

private:
    std::vector<std::unique_ptr<NodeColumn>> childColumns;
};

} // namespace storage
} // namespace kuzu

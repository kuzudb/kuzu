#pragma once

#include "storage/store/column.h"

namespace kuzu {
namespace storage {

class StringColumn : public Column {
public:
    StringColumn(common::LogicalType dataType, const MetadataDAHInfo& metaDAHeaderInfo,
        BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        transaction::Transaction* transaction, RWPropertyStats propertyStatistics);

    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector = 0) final;
    void scan(common::node_group_idx_t nodeGroupIdx, ColumnChunk* columnChunk) final;

    void append(ColumnChunk* columnChunk, common::node_group_idx_t nodeGroupIdx) final;

    void writeValue(common::offset_t nodeOffset, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom) final;

    inline InMemDiskArray<OverflowColumnChunkMetadata>* getOverflowMetadataDA() {
        return overflowMetadataDA.get();
    }

    void checkpointInMemory() final;
    void rollbackInMemory() final;

protected:
    void scanInternal(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;
    void lookupInternal(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;

private:
    void readStringValueFromOvf(transaction::Transaction* transaction, common::ku_string_t& kuStr,
        common::ValueVector* resultVector, common::page_idx_t overflowPageIdx);

private:
    std::unique_ptr<InMemDiskArray<OverflowColumnChunkMetadata>> overflowMetadataDA;
};

} // namespace storage
} // namespace kuzu

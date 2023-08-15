#pragma once

#include "node_column.h"

namespace kuzu {
namespace storage {

struct StringNodeColumnFunc {
    static void writeStringValuesToPage(
        uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector, uint32_t posInVector);
};

class StringNodeColumn : public NodeColumn {
public:
    StringNodeColumn(common::LogicalType dataType, const catalog::MetadataDAHInfo& metaDAHeaderInfo,
        BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        transaction::Transaction* transaction);

    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector = 0) final;

    common::page_idx_t append(ColumnChunk* columnChunk, common::page_idx_t startPageIdx,
        common::node_group_idx_t nodeGroupIdx) final;

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
    std::unique_ptr<InMemDiskArray<ColumnChunkMetadata>> overflowMetadataDA;
};

} // namespace storage
} // namespace kuzu

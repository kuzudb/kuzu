#pragma once

#include "storage/store/dictionary_column.h"

namespace kuzu {
namespace storage {

class StringColumn final : public Column {
public:
    StringColumn(std::string name, common::LogicalType dataType,
        const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
        BufferManager* bufferManager, WAL* wal, transaction::Transaction* transaction,
        RWPropertyStats propertyStatistics, bool enableCompression);

    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector = 0) override;
    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        ColumnChunk* columnChunk, common::offset_t startOffset = 0,
        common::offset_t endOffset = common::INVALID_OFFSET) override;

    void append(ColumnChunk* columnChunk, common::node_group_idx_t nodeGroupIdx) override;

    void writeValue(const ColumnChunkMetadata& chunkMeta, common::node_group_idx_t nodeGroupIdx,
        common::offset_t offsetInChunk, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom) override;

    void write(common::node_group_idx_t nodeGroupIdx, common::offset_t offsetInChunk,
        ColumnChunk* data, common::offset_t dataOffset, common::length_t numValues) override;

    void checkpointInMemory() override;
    void rollbackInMemory() override;

    inline const DictionaryColumn& getDictionary() const { return dictionary; }

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

private:
    bool canCommitInPlace(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, LocalVectorCollection* localChunk,
        const offset_to_row_idx_t& insertInfo, const offset_to_row_idx_t& updateInfo) override;
    bool canCommitInPlace(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, const std::vector<common::offset_t>& dstOffsets,
        ColumnChunk* chunk, common::offset_t srcOffset) override;

    bool canIndexCommitInPlace(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, uint64_t numStrings, common::offset_t maxOffset);

private:
    // Main column stores indices of values in the dictionary
    DictionaryColumn dictionary;
};

} // namespace storage
} // namespace kuzu

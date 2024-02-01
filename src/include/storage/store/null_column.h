#pragma once

#include "storage/store/column.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

class NullColumn final : public Column {
    friend StructColumn;

public:
    // Page size must be aligned to 8 byte chunks for the 64-bit NullMask algorithms to work
    // without the possibility of memory errors from reading/writing off the end of a page.
    static_assert(PageUtils::getNumElementsInAPage(1, false /*requireNullColumn*/) % 8 == 0);

    NullColumn(std::string name, page_idx_t metaDAHPageIdx, BMFileHandle* dataFH,
        BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal, Transaction* transaction,
        RWPropertyStats propertyStatistics, bool enableCompression);

    void scan(
        Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) override;
    void scan(transaction::Transaction* transaction, node_group_idx_t nodeGroupIdx,
        offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
        uint64_t offsetInVector) override;
    void scan(transaction::Transaction* transaction, node_group_idx_t nodeGroupIdx,
        ColumnChunk* columnChunk, common::offset_t startOffset = 0,
        common::offset_t endOffset = common::INVALID_OFFSET) override;

    void lookup(
        Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) override;

    void append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) override;

    bool isNull(transaction::Transaction* transaction, node_group_idx_t nodeGroupIdx,
        offset_t offsetInChunk);
    void setNull(node_group_idx_t nodeGroupIdx, offset_t offsetInChunk, uint64_t value = true);

    void write(node_group_idx_t nodeGroupIdx, offset_t offsetInChunk,
        ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override;
    void write(common::node_group_idx_t nodeGroupIdx, common::offset_t offsetInChunk,
        ColumnChunk* data, common::offset_t dataOffset, common::length_t numValues) override;

    bool canCommitInPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
        LocalVectorCollection* localChunk, const offset_to_row_idx_t& insertInfo,
        const offset_to_row_idx_t& updateInfo) override;
    bool canCommitInPlace(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, const std::vector<common::offset_t>& dstOffsets,
        ColumnChunk* chunk, common::offset_t srcOffset) override;
    void commitLocalChunkInPlace(Transaction* /*transaction*/, node_group_idx_t nodeGroupIdx,
        LocalVectorCollection* localChunk, const offset_to_row_idx_t& insertInfo,
        const offset_to_row_idx_t& updateInfo, const offset_set_t& deleteInfo) override;

private:
    std::unique_ptr<ColumnChunk> getEmptyChunkForCommit(uint64_t /*capacity*/) override {
        return ColumnChunkFactory::createNullColumnChunk(enableCompression);
    }
};

} // namespace storage
} // namespace kuzu

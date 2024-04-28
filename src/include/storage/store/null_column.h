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

    void scan(Transaction* transaction, ChunkState& readState, ValueVector* nodeIDVector,
        ValueVector* resultVector) override;
    void scan(transaction::Transaction* transaction, ChunkState& readState,
        offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
        uint64_t offsetInVector) override;
    void scan(transaction::Transaction* transaction, node_group_idx_t nodeGroupIdx,
        ColumnChunk* columnChunk, common::offset_t startOffset = 0,
        common::offset_t endOffset = common::INVALID_OFFSET) override;

    void lookup(Transaction* transaction, ChunkState& readState, ValueVector* nodeIDVector,
        ValueVector* resultVector) override;

    void append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) override;

    bool isNull(Transaction* transaction, const ChunkState& state, offset_t offsetInChunk);
    void setNull(ChunkState& state, offset_t offsetInChunk, uint64_t value = true);

    void write(ChunkState& state, offset_t offsetInChunk, ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom) override;
    void write(ChunkState& state, common::offset_t offsetInChunk, ColumnChunk* data,
        common::offset_t dataOffset, common::length_t numValues) override;

    void commitLocalChunkInPlace(ChunkState& state, const ChunkCollection& localInsertChunk,
        const offset_to_row_idx_t& insertInfo, const ChunkCollection& localUpdateChunk,
        const offset_to_row_idx_t& updateInfo, const offset_set_t& deleteInfo) override;

private:
    std::unique_ptr<ColumnChunk> getEmptyChunkForCommit(uint64_t capacity) override {
        return ColumnChunkFactory::createNullColumnChunk(enableCompression, capacity);
    }
};

} // namespace storage
} // namespace kuzu

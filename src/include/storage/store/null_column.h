#pragma once

#include "storage/store/column.h"

namespace kuzu {
namespace storage {

class NullColumn final : public Column {
    friend StructColumn;

public:
    // Page size must be aligned to 8 byte chunks for the 64-bit NullMask algorithms to work
    // without the possibility of memory errors from reading/writing off the end of a page.
    static_assert(PageUtils::getNumElementsInAPage(1, false /*requireNullColumn*/) % 8 == 0);

    NullColumn(std::string name, common::page_idx_t metaDAHIdx, BMFileHandle* dataFH,
        DiskArrayCollection& metadataDAC, BufferManager* bufferManager, WAL* wal,
        transaction::Transaction* transaction, bool enableCompression);

    void scan(transaction::Transaction* transaction, const ChunkState& state,
        common::idx_t vectorIdx, common::row_idx_t numValuesToScan,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector) override;
    void scan(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector) override;
    void scan(transaction::Transaction* transaction, const ChunkState& state,
        ColumnChunkData* columnChunk, common::offset_t startOffset = 0,
        common::offset_t endOffset = common::INVALID_OFFSET) override;

    void lookup(transaction::Transaction* transaction, ChunkState& readState,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector) override;

    void append(ColumnChunkData* columnChunk, ChunkState& state) override;

    bool isNull(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t offsetInChunk);
    void setNull(ChunkState& state, common::offset_t offsetInChunk, uint64_t value = true);

    void write(ChunkState& state, common::offset_t offsetInChunk,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override;
    void write(ChunkState& state, common::offset_t offsetInChunk, ColumnChunkData* data,
        common::offset_t dataOffset, common::length_t numValues) override;

    void commitLocalChunkInPlace(ChunkState& state, const ChunkCollection& localInsertChunk,
        const offset_to_row_idx_t& insertInfo, const ChunkCollection& localUpdateChunk,
        const offset_to_row_idx_t& updateInfo, const offset_set_t& deleteInfo) override;

private:
    std::unique_ptr<ColumnChunkData> getEmptyChunkForCommit(uint64_t capacity) override {
        return ColumnChunkFactory::createNullChunkData(enableCompression, capacity);
    }
};

} // namespace storage
} // namespace kuzu

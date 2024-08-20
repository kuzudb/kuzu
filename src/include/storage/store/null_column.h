#pragma once

#include "storage/storage_utils.h"
#include "storage/store/column.h"

namespace kuzu {
namespace storage {

class NullColumn final : public Column {
    friend StructColumn;

public:
    // Page size must be aligned to 8 byte chunks for the 64-bit NullMask algorithms to work
    // without the possibility of memory errors from reading/writing off the end of a page.
    static_assert(PageUtils::getNumElementsInAPage(1, false /*requireNullColumn*/) % 8 == 0);

    NullColumn(const std::string& name, BMFileHandle* dataFH, BufferManager* bufferManager,
        ShadowFile* shadowFile, bool enableCompression);

    void scan(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInChunk, common::row_idx_t numValuesToScan,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector) override;
    void scan(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector) override;
    void scan(transaction::Transaction* transaction, const ChunkState& state,
        ColumnChunkData* columnChunk, common::offset_t startOffset = 0,
        common::offset_t endOffset = common::INVALID_OFFSET) override;

    void write(ColumnChunkData& persistentChunk, const ChunkState& state,
        common::offset_t offsetInChunk, ColumnChunkData* data, common::offset_t dataOffset,
        common::length_t numValues) override;
};

} // namespace storage
} // namespace kuzu

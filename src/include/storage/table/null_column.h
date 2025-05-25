#pragma once

#include "storage/storage_utils.h"
#include "storage/table/column.h"

namespace kuzu {
namespace storage {

static constexpr uint32_t getNumElementsInAPage(uint32_t elementSize, bool hasNull) {
    KU_ASSERT(elementSize > 0);
    auto numBytesPerNullEntry = common::NullMask::NUM_BITS_PER_NULL_ENTRY >> 3;
    auto numNullEntries =
        hasNull ? static_cast<uint32_t>(ceil(
                      static_cast<double>(common::KUZU_PAGE_SIZE) /
                      static_cast<double>((static_cast<uint64_t>(elementSize)
                                              << common::NullMask::NUM_BITS_PER_NULL_ENTRY_LOG2) +
                                          numBytesPerNullEntry))) :
                  0;
    return (common::KUZU_PAGE_SIZE - (numNullEntries * numBytesPerNullEntry)) / elementSize;
}
// Page size must be aligned to 8 byte chunks for the 64-bit NullMask algorithms to work
// without the possibility of memory errors from reading/writing off the end of a page.
static_assert(getNumElementsInAPage(1, false /*requireNullColumn*/) % 8 == 0);

class NullColumn final : public Column {
    friend StructColumn;

public:
    NullColumn(const std::string& name, FileHandle* dataFH, MemoryManager* mm,
        ShadowFile* shadowFile, bool enableCompression);

    void scan(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInChunk, common::row_idx_t numValuesToScan,
        common::ValueVector* resultVector) const override;
    void scan(const transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector) const override;
    void scan(const transaction::Transaction* transaction, const ChunkState& state,
        ColumnChunkData* columnChunk, common::offset_t startOffset = 0,
        common::offset_t endOffset = common::INVALID_OFFSET) const override;

    void write(ColumnChunkData& persistentChunk, ChunkState& state, common::offset_t offsetInChunk,
        ColumnChunkData* data, common::offset_t dataOffset, common::length_t numValues) override;
};

} // namespace storage
} // namespace kuzu

#pragma once

#include "common/types/types.h"
#include "storage/store/column.h"

namespace kuzu {
namespace storage {
class MemoryManager;

class StructColumn final : public Column {
public:
    StructColumn(std::string name, common::LogicalType dataType, FileHandle* dataFH,
        MemoryManager* mm, ShadowFile* shadowFile, bool enableCompression);

    static std::unique_ptr<ColumnChunkData> flushChunkData(const ColumnChunkData& chunk,
        FileHandle& dataFH);

    Column* getChild(common::idx_t childIdx) const {
        KU_ASSERT(childIdx < childColumns.size());
        return childColumns[childIdx].get();
    }
    void writeInternal(ColumnChunkData& persistentChunk, SegmentState& state,
        common::offset_t offsetInSegment, const ColumnChunkData& data, common::offset_t dataOffset,
        common::length_t numValues) const override;

    void checkpointSegment(ColumnCheckpointState&& checkpointState) const override;

protected:
    void scanInternal(const transaction::Transaction* transaction, const SegmentState& state,
        common::offset_t startOffsetInSegment, common::row_idx_t numValuesToScan,
        ColumnChunkData* resultChunk) const override;
    void scanInternal(const transaction::Transaction* transaction, const SegmentState& state,
        common::offset_t startOffsetInSegment, common::row_idx_t numValuesToScan,
        common::ValueVector* resultVector, common::offset_t offsetInResult) const override;

    void lookupInternal(const transaction::Transaction* transaction, const SegmentState& state,
        common::offset_t offsetInSegment, common::ValueVector* resultVector,
        uint32_t posInVector) const override;

private:
    std::vector<std::unique_ptr<Column>> childColumns;
};

} // namespace storage
} // namespace kuzu

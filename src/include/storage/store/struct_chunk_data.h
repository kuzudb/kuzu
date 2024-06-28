#pragma once

#include "common/data_chunk/sel_vector.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "storage/store/column_chunk_data.h"

namespace kuzu {
namespace storage {

class StructChunkData final : public ColumnChunkData {
public:
    StructChunkData(common::LogicalType dataType, uint64_t capacity, bool enableCompression,
        bool inMemory);

    ColumnChunkData* getChild(common::idx_t childIdx) {
        KU_ASSERT(childIdx < childChunks.size());
        return childChunks[childIdx].get();
    }

    void finalize() override;

protected:
    void append(ColumnChunkData* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) override;
    void append(common::ValueVector* vector, const common::SelectionVector& selVector) override;

    void lookup(common::offset_t offsetInChunk, common::ValueVector& output,
        common::sel_t posInOutputVector) const override;

    void write(common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk) override;
    void write(ColumnChunkData* chunk, ColumnChunkData* dstOffsets,
        common::RelMultiplicity multiplicity) override;
    void write(ColumnChunkData* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;
    void copy(ColumnChunkData* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;

    void resize(uint64_t newCapacity) override;

    void resetToEmpty() override;

    bool numValuesSanityCheck() const override;

    void setNumValues(uint64_t numValues) override {
        ColumnChunkData::setNumValues(numValues);
        for (auto& childChunk : childChunks) {
            childChunk->setNumValues(numValues);
        }
    }

private:
    std::vector<std::unique_ptr<ColumnChunkData>> childChunks;
};

} // namespace storage
} // namespace kuzu

#pragma once

#include "common/data_chunk/sel_vector.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

class StructColumnChunk final : public ColumnChunk {
public:
    StructColumnChunk(common::LogicalType dataType, uint64_t capacity, bool enableCompression,
        bool inMemory);

    inline ColumnChunk* getChild(common::vector_idx_t childIdx) {
        KU_ASSERT(childIdx < childChunks.size());
        return childChunks[childIdx].get();
    }

    void finalize() override;

protected:
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) final;
    void append(common::ValueVector* vector, const common::SelectionVector& selVector) final;

    void lookup(common::offset_t offsetInChunk, common::ValueVector& output,
        common::sel_t posInOutputVector) const override;

    void write(common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk) override;
    void write(ColumnChunk* chunk, ColumnChunk* dstOffsets,
        common::RelMultiplicity multiplicity) override;
    void write(ColumnChunk* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;
    void copy(ColumnChunk* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;

    void resize(uint64_t newCapacity) override;

    void resetToEmpty() override;

    bool numValuesSanityCheck() const override;

    void setNumValues(uint64_t numValues) override {
        ColumnChunk::setNumValues(numValues);
        for (auto& childChunk : childChunks) {
            childChunk->setNumValues(numValues);
        }
    }

private:
    std::vector<std::unique_ptr<ColumnChunk>> childChunks;
};

} // namespace storage
} // namespace kuzu

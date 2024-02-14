#pragma once

#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

class StructColumnChunk : public ColumnChunk {
public:
    StructColumnChunk(common::LogicalType dataType, uint64_t capacity, bool enableCompression);

    inline ColumnChunk* getChild(common::vector_idx_t childIdx) {
        KU_ASSERT(childIdx < childChunks.size());
        return childChunks[childIdx].get();
    }

    void finalize() final;

protected:
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) final;
    void append(common::ValueVector* vector) final;

    void write(common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk) final;
    void write(common::ValueVector* valueVector, common::ValueVector* offsetInChunkVector,
        bool isCSR) final;
    void write(ColumnChunk* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;
    void copy(ColumnChunk* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;

    void resize(uint64_t newCapacity) final;

    void resetToEmpty() final;

    bool numValuesSanityCheck() const override;

private:
    std::vector<std::unique_ptr<ColumnChunk>> childChunks;
};

} // namespace storage
} // namespace kuzu

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

protected:
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend) final;
    void append(common::ValueVector* vector, common::offset_t startPosInChunk) final;

    void write(common::ValueVector* vector, common::offset_t startOffsetInChunk) final;
    void write(common::ValueVector* valueVector, common::ValueVector* offsetInChunkVector) final;

    void resize(uint64_t newCapacity) final;

private:
    std::vector<std::unique_ptr<ColumnChunk>> childChunks;
};

} // namespace storage
} // namespace kuzu

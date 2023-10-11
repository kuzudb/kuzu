#pragma once

#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

class StructColumnChunk : public ColumnChunk {
public:
    StructColumnChunk(common::LogicalType dataType, bool enableCompression);

    inline ColumnChunk* getChild(common::vector_idx_t childIdx) {
        assert(childIdx < childChunks.size());
        return childChunks[childIdx].get();
    }

protected:
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend) final;
    void append(common::ValueVector* vector, common::offset_t startPosInChunk) final;

    void resize(uint64_t newCapacity) final;

private:
    void write(const common::Value& val, uint64_t posToWrite) final;

private:
    std::vector<std::unique_ptr<ColumnChunk>> childChunks;
};

} // namespace storage
} // namespace kuzu

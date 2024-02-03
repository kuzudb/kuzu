#pragma once

#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

class StructColumnChunk : public ColumnChunk {
public:
    StructColumnChunk(
        std::unique_ptr<common::LogicalType> dataType, uint64_t capacity, bool enableCompression);

    inline ColumnChunk* getChild(common::vector_idx_t childIdx) {
        KU_ASSERT(childIdx < childChunks.size());
        return childChunks[childIdx].get();
    }

    void finalize() override;

    template<typename T>
    T getValue(common::offset_t /*pos*/) const {
        KU_UNREACHABLE;
    }

protected:
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) override;
    void append(common::ValueVector* vector) override;

    void write(common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk) override;
    void write(common::ValueVector* valueVector, common::ValueVector* offsetInChunkVector,
        bool isCSR) override;
    void write(ColumnChunk* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;
    void copy(ColumnChunk* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;

    void resize(uint64_t newCapacity) override;

    void resetToEmpty() override;

    void setAllNull() override;

    void setNumValues(uint64_t numValues_) override;

    void setNull(common::offset_t offset, bool isNull) override;

    bool numValuesSanityCheck() const override;

    std::vector<std::unique_ptr<ColumnChunk>> childChunks;
};

} // namespace storage
} // namespace kuzu

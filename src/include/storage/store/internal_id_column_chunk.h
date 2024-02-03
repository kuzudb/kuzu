#pragma once

#include "storage/store/struct_column_chunk.h"

namespace kuzu {
namespace storage {

class InternalIDColumnChunk final : public StructColumnChunk {
public:
    InternalIDColumnChunk(
        uint64_t capacity, std::unique_ptr<common::LogicalType> dataType, bool enableCompression);
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) override;
    void append(common::ValueVector* vector) override;

protected:
    void write(common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk) override;
    void write(common::ValueVector* valueVector, common::ValueVector* offsetInChunkVector,
        bool isCSR) override;
};

} // namespace storage
} // namespace kuzu

#pragma once

#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

class StructColumnChunk : public ColumnChunk {
public:
    StructColumnChunk(common::LogicalType dataType,
        std::unique_ptr<common::CSVReaderConfig> csvReaderConfig, bool enableCompression);

protected:
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend) final;
    void append(common::ValueVector* vector, common::offset_t startPosInChunk) final;

private:
    void write(const common::Value& val, uint64_t posToWrite) final;
};

} // namespace storage
} // namespace kuzu

#pragma once

#include "storage/copier/column_chunk.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

struct VarListDataColumnChunk {
    std::unique_ptr<ColumnChunk> dataChunk;
    uint64_t numValuesInDataChunk;
    uint64_t capacityInDataChunk;

    VarListDataColumnChunk(std::unique_ptr<ColumnChunk> dataChunk)
        : dataChunk{std::move(dataChunk)}, numValuesInDataChunk{0},
          capacityInDataChunk{StorageConstants::NODE_GROUP_SIZE} {}

    void reset();
};

class VarListColumnChunk : public ColumnChunk {
public:
    VarListColumnChunk(LogicalType dataType, CopyDescription* copyDescription);

    inline ColumnChunk* getDataColumnChunk() const {
        return varListDataColumnChunk.dataChunk.get();
    }

    void setValueFromString(const char* value, uint64_t length, uint64_t pos);

    void resetToEmpty() final;

private:
    inline common::page_idx_t getNumPages() const final {
        return varListDataColumnChunk.dataChunk->getNumPages() + ColumnChunk::getNumPages();
    }

    void append(arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) override;

    void copyVarListFromArrowString(
        arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend);

    void copyVarListFromArrowList(
        arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend);

    void write(const common::Value& listVal, uint64_t posToWrite) override;

    void resizeDataChunk(uint64_t numValues);

private:
    VarListDataColumnChunk varListDataColumnChunk;
};

} // namespace storage
} // namespace kuzu

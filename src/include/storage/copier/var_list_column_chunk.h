#pragma once

#include "arrow/array/array_nested.h"
#include "storage/copier/column_chunk.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

struct VarListDataColumnChunk {
    std::unique_ptr<ColumnChunk> dataChunk;
    uint64_t numValuesInDataChunk;
    uint64_t capacityInDataChunk;

    explicit VarListDataColumnChunk(std::unique_ptr<ColumnChunk> dataChunk)
        : dataChunk{std::move(dataChunk)}, numValuesInDataChunk{0},
          capacityInDataChunk{StorageConstants::NODE_GROUP_SIZE} {}

    void reset();

    void resize(uint64_t numValues);
};

class VarListColumnChunk : public ColumnChunk {
public:
    VarListColumnChunk(LogicalType dataType, CopyDescription* copyDescription);

    inline ColumnChunk* getDataColumnChunk() const {
        return varListDataColumnChunk.dataChunk.get();
    }

    void setValueFromString(const char* value, uint64_t length, uint64_t pos);

    void resetToEmpty() final;

    void append(common::ValueVector* vector, common::offset_t startPosInChunk) final;

private:
    inline common::page_idx_t getNumPages() const final {
        return varListDataColumnChunk.dataChunk->getNumPages() + ColumnChunk::getNumPages();
    }

    void append(arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) override;

    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend) final;

    void copyVarListFromArrowString(
        arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend);

    template<typename T>
    void copyVarListFromArrowList(
        arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
        auto listArray = (T*)array;
        auto dataChunkOffsetToAppend = varListDataColumnChunk.numValuesInDataChunk;
        for (auto i = 0u; i < numValuesToAppend; i++) {
            nullChunk->setNull(i + startPosInChunk, listArray->IsNull(i));
            auto length = listArray->value_length(i);
            varListDataColumnChunk.numValuesInDataChunk += length;
            setValue(varListDataColumnChunk.numValuesInDataChunk, i + startPosInChunk);
        }
        auto startOffset = listArray->value_offset(startPosInChunk);
        auto endOffset = listArray->value_offset(startPosInChunk + numValuesToAppend);
        varListDataColumnChunk.resize(varListDataColumnChunk.numValuesInDataChunk);
        varListDataColumnChunk.dataChunk->append(
            listArray->values().get(), dataChunkOffsetToAppend, endOffset - startOffset);
    }

    void write(const common::Value& listVal, uint64_t posToWrite) override;

private:
    VarListDataColumnChunk varListDataColumnChunk;

    inline uint64_t getListLen(common::offset_t offset) const {
        return getListOffset(offset + 1) - getListOffset(offset);
    }

    inline offset_t getListOffset(common::offset_t offset) const {
        return offset == 0 ? 0 : getValue<uint64_t>(offset - 1);
    }
};

} // namespace storage
} // namespace kuzu

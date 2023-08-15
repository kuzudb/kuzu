#pragma once

#include "storage/copier/column_chunk.h"

namespace kuzu {
namespace storage {

struct StructFieldIdxAndValue {
    StructFieldIdxAndValue(common::struct_field_idx_t fieldIdx, std::string fieldValue)
        : fieldIdx{fieldIdx}, fieldValue{std::move(fieldValue)} {}

    common::struct_field_idx_t fieldIdx;
    std::string fieldValue;
};

class StructColumnChunk : public ColumnChunk {
public:
    StructColumnChunk(common::LogicalType dataType, common::CopyDescription* copyDescription);

protected:
    void append(
        arrow::Array* array, common::offset_t startPosInChunk, uint32_t numValuesToAppend) final;
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        common::offset_t startPosInChunk, uint32_t numValuesToAppend) final;
    void append(common::ValueVector* vector, common::offset_t startPosInChunk) final;

private:
    // TODO(Guodong): These methods are duplicated from `InMemStructColumnChunk`, which will be
    // merged later.
    void setStructFields(const char* value, uint64_t length, uint64_t pos);
    void setValueToStructField(common::offset_t pos, const std::string& structFieldValue,
        common::struct_field_idx_t structFiledIdx);
    std::vector<StructFieldIdxAndValue> parseStructFieldNameAndValues(
        common::LogicalType& type, const std::string& structString);
    static std::string parseStructFieldName(const std::string& structString, uint64_t& curPos);
    std::string parseStructFieldValue(const std::string& structString, uint64_t& curPos);
    void write(const common::Value& val, uint64_t posToWrite) final;
    void copyStructFromArrowStruct(
        arrow::Array* array, common::offset_t startPosInChunk, uint32_t numValuesToAppend);
    template<typename ARROW_TYPE>
    void copyStructFromArrowString(
        arrow::Array* array, common::offset_t startPosInChunk, uint32_t numValuesToAppend);
};

} // namespace storage
} // namespace kuzu

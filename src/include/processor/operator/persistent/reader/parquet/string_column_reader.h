#pragma once

#include "column_reader.h"
#include "processor/operator/persistent/reader/parquet/templated_column_reader.h"

namespace kuzu {
namespace processor {

struct StringParquetValueConversion {
    static common::ku_string_t dictRead(ByteBuffer& dict, uint32_t& offset, ColumnReader& reader);

    static common::ku_string_t plainRead(ByteBuffer& plainData, ColumnReader& reader);

    static void plainSkip(ByteBuffer& plainData, ColumnReader& reader);
};

class StringColumnReader
    : public TemplatedColumnReader<common::ku_string_t, StringParquetValueConversion> {
public:
    static constexpr const common::PhysicalTypeID TYPE = common::PhysicalTypeID::STRING;

public:
    StringColumnReader(ParquetReader& reader, std::unique_ptr<common::LogicalType> type,
        const kuzu_parquet::format::SchemaElement& schema, uint64_t schemaIdx, uint64_t maxDefine,
        uint64_t maxRepeat);

    std::unique_ptr<common::ku_string_t[]> dictStrs;
    uint64_t fixedWidthStringLength;

public:
    void dictionary(
        std::shared_ptr<ResizeableBuffer> dictionary_data, uint64_t numEntries) override;
    static uint32_t verifyString(const char* strData, uint32_t strLen, const bool isVarchar);
    uint32_t verifyString(const char* strData, uint32_t strLen);
};

} // namespace processor
} // namespace kuzu

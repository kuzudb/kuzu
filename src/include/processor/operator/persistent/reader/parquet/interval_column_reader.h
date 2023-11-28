#pragma once

#include "common/types/interval_t.h"
#include "templated_column_reader.h"

namespace kuzu {
namespace processor {

struct IntervalValueConversion {
    static inline common::interval_t dictRead(
        ByteBuffer& dict, uint32_t& offset, ColumnReader& /*reader*/) {
        return (reinterpret_cast<common::interval_t*>(dict.ptr))[offset];
    }

    static common::interval_t readParquetInterval(const char* input);

    static common::interval_t plainRead(ByteBuffer& plainData, ColumnReader& reader);

    static inline void plainSkip(ByteBuffer& plain_data, ColumnReader& /*reader*/) {
        plain_data.inc(common::ParquetConstants::PARQUET_INTERVAL_SIZE);
    }
};

class IntervalColumnReader
    : public TemplatedColumnReader<common::interval_t, IntervalValueConversion> {

public:
    IntervalColumnReader(ParquetReader& reader, std::unique_ptr<common::LogicalType> type,
        const kuzu_parquet::format::SchemaElement& schema, uint64_t fileIdx, uint64_t maxDefine,
        uint64_t maxRepeat)
        : TemplatedColumnReader<common::interval_t, IntervalValueConversion>(
              reader, std::move(type), schema, fileIdx, maxDefine, maxRepeat){};

protected:
    void dictionary(
        const std::shared_ptr<ResizeableBuffer>& dictionary_data, uint64_t num_entries) override;
};

} // namespace processor
} // namespace kuzu

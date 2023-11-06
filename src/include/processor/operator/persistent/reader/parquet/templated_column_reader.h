#pragma once

#include "column_reader.h"
#include "resizable_buffer.h"

namespace kuzu {
namespace processor {

template<class VALUE_TYPE>
struct TemplatedParquetValueConversion {
    static VALUE_TYPE dictRead(ByteBuffer& dict, uint32_t& offset, ColumnReader& /*reader*/) {
        assert(offset < dict.len / sizeof(VALUE_TYPE));
        return ((VALUE_TYPE*)dict.ptr)[offset];
    }

    static VALUE_TYPE plainRead(ByteBuffer& plainData, ColumnReader& /*reader*/) {
        return plainData.read<VALUE_TYPE>();
    }

    static void plainSkip(ByteBuffer& plainData, ColumnReader& /*reader*/) {
        plainData.inc(sizeof(VALUE_TYPE));
    }
};

template<class VALUE_TYPE, class VALUE_CONVERSION>
class TemplatedColumnReader : public ColumnReader {
public:
    static constexpr const common::PhysicalTypeID TYPE = common::PhysicalTypeID::ANY;

public:
    TemplatedColumnReader(ParquetReader& reader, std::unique_ptr<common::LogicalType> type,
        const kuzu_parquet::format::SchemaElement& schema, uint64_t schemaIdx, uint64_t maxDefine,
        uint64_t maxRepeat)
        : ColumnReader(reader, std::move(type), schema, schemaIdx, maxDefine, maxRepeat){};

    std::shared_ptr<ResizeableBuffer> dict;

public:
    void allocateDict(uint64_t size) {
        if (!dict) {
            dict = std::make_shared<ResizeableBuffer>(size);
        } else {
            dict->resize(size);
        }
    }

    void offsets(uint32_t* offsets, uint8_t* defines, uint64_t numValues, parquet_filter_t& filter,
        uint64_t resultOffset, common::ValueVector* result) override {
        uint64_t offsetIdx = 0;
        for (auto rowIdx = 0; rowIdx < numValues; rowIdx++) {
            if (hasDefines() && defines[rowIdx + resultOffset] != maxDefine) {
                result->setNull(rowIdx + resultOffset, true);
                continue;
            }
            result->setNull(rowIdx + resultOffset, false);
            if (filter[rowIdx + resultOffset]) {
                VALUE_TYPE val = VALUE_CONVERSION::dictRead(*dict, offsets[offsetIdx++], *this);
                result->setValue(rowIdx + resultOffset, val);
            } else {
                offsetIdx++;
            }
        }
    }

    void plain(std::shared_ptr<ByteBuffer> plainData, uint8_t* defines, uint64_t numValues,
        parquet_filter_t& filter, uint64_t resultOffset, common::ValueVector* result) override {
        plainTemplated<VALUE_TYPE, VALUE_CONVERSION>(
            std::move(plainData), defines, numValues, filter, resultOffset, result);
    }

    void dictionary(std::shared_ptr<ResizeableBuffer> data, uint64_t /*num_entries*/) override {
        dict = std::move(data);
    }
};

template<class PARQUET_PHYSICAL_TYPE, class DUCKDB_PHYSICAL_TYPE,
    DUCKDB_PHYSICAL_TYPE (*FUNC)(const PARQUET_PHYSICAL_TYPE& input)>
struct CallbackParquetValueConversion {
    static DUCKDB_PHYSICAL_TYPE dictRead(ByteBuffer& dict, uint32_t& offset, ColumnReader& reader) {
        return TemplatedParquetValueConversion<DUCKDB_PHYSICAL_TYPE>::dictRead(
            dict, offset, reader);
    }

    static DUCKDB_PHYSICAL_TYPE plainRead(ByteBuffer& plainData, ColumnReader& /*reader*/) {
        return FUNC(plainData.read<PARQUET_PHYSICAL_TYPE>());
    }

    static void plainSkip(ByteBuffer& plainData, ColumnReader& /*reader*/) {
        plainData.inc(sizeof(PARQUET_PHYSICAL_TYPE));
    }
};

} // namespace processor
} // namespace kuzu

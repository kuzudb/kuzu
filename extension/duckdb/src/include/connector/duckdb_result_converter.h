#pragma once

#include "common/data_chunk/data_chunk.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
// Supress warnings from duckdb.hpp
#undef ARROW_FLAG_DICTIONARY_ORDERED
#include "duckdb.hpp"
#pragma GCC diagnostic pop
#include <optional>

namespace kuzu {
namespace duckdb_extension {

using duckdb_conversion_func_t = std::function<void(duckdb::Vector& duckDBVector,
    common::ValueVector& result, uint64_t numValues)>;

struct DuckDBResultConverter {
    std::vector<duckdb_conversion_func_t> conversionFunctions;

    explicit DuckDBResultConverter(const std::vector<common::LogicalType>& types);
    void convertDuckDBResultToVector(duckdb::DataChunk& duckDBResult, common::DataChunk& result,
        std::optional<std::vector<bool>> columnSkips = std::nullopt) const;

    static void getDuckDBVectorConversionFunc(common::PhysicalTypeID physicalTypeID,
        duckdb_conversion_func_t& conversion_func);
};

} // namespace duckdb_extension
} // namespace kuzu

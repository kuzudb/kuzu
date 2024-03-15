#pragma once

#include "common/vector/value_vector.h"
#include "function/scalar_function.h"
#include "function/table/bind_data.h"
#include "function/table_functions.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
// Supress warnings from duckdb.hpp
#undef ARROW_FLAG_DICTIONARY_ORDERED
#include "duckdb.hpp"
#pragma GCC diagnostic pop

namespace kuzu {
namespace duckdb_scanner {

using duckdb_conversion_func_t = std::function<void(
    duckdb::Vector& duckDBVector, common::ValueVector& result, uint64_t numValues)>;

struct DuckDBScanBindData : public function::TableFuncBindData {
    explicit DuckDBScanBindData(std::string query, std::string dbPath,
        std::vector<common::LogicalType> columnTypes, std::vector<std::string> columnNames);

    std::unique_ptr<TableFuncBindData> copy() const override;

    std::string query;
    std::string dbPath;
    std::vector<duckdb_conversion_func_t> conversionFunctions;
};

struct DuckDBScanSharedState : public function::TableFuncSharedState {
    explicit DuckDBScanSharedState(std::unique_ptr<duckdb::QueryResult> queryResult);

    std::unique_ptr<duckdb::QueryResult> queryResult;
};

function::TableFunction getScanFunction(std::string dbPath);

} // namespace duckdb_scanner
} // namespace kuzu

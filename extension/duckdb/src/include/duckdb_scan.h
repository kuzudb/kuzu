#pragma once

#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "function/table/bind_data.h"
#include "function/table/scan_functions.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
// Supress warnings from duckdb.hpp
#undef ARROW_FLAG_DICTIONARY_ORDERED
#include "duckdb.hpp"
#pragma GCC diagnostic pop

namespace kuzu {
namespace duckdb_extension {

class DuckDBConnector;

using duckdb_conversion_func_t = std::function<void(duckdb::Vector& duckDBVector,
    common::ValueVector& result, uint64_t numValues)>;
using init_duckdb_conn_t = std::function<std::pair<duckdb::DuckDB, duckdb::Connection>()>;

struct DuckDBScanBindData : public function::TableFuncBindData {
    std::string query;
    std::vector<duckdb_conversion_func_t> conversionFunctions;
    const DuckDBConnector& connector;

    DuckDBScanBindData(std::string query, std::vector<common::LogicalType> columnTypes,
        std::vector<std::string> columnNames, const DuckDBConnector& connector);
    DuckDBScanBindData(const DuckDBScanBindData& other)
        : function::TableFuncBindData{other}, query{other.query},
          conversionFunctions{other.conversionFunctions}, connector{other.connector} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<DuckDBScanBindData>(*this);
    }
};

struct DuckDBScanSharedState : public function::BaseScanSharedStateWithNumRows {
    explicit DuckDBScanSharedState(std::unique_ptr<duckdb::MaterializedQueryResult> queryResult);

    std::unique_ptr<duckdb::MaterializedQueryResult> queryResult;
};

void getDuckDBVectorConversionFunc(common::PhysicalTypeID physicalTypeID,
    duckdb_conversion_func_t& conversion_func);

function::TableFunction getScanFunction(DuckDBScanBindData bindData);

} // namespace duckdb_extension
} // namespace kuzu

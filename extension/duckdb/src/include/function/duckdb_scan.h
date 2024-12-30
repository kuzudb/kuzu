#pragma once

#include "common/types/types.h"
#include "connector/duckdb_result_converter.h"
#include "function/table/bind_data.h"
#include "function/table/scan_functions.h"

namespace kuzu {
namespace duckdb_extension {

class DuckDBConnector;

using init_duckdb_conn_t = std::function<std::pair<duckdb::DuckDB, duckdb::Connection>()>;

struct DuckDBScanBindData final : function::TableFuncBindData {
    std::string query;
    std::vector<common::LogicalType> columnTypes;
    std::vector<std::string> columnNames;
    DuckDBResultConverter converter;
    const DuckDBConnector& connector;

    DuckDBScanBindData(std::string query, std::vector<common::LogicalType> columnTypes,
        std::vector<std::string> columnNames, const DuckDBConnector& connector);
    DuckDBScanBindData(const DuckDBScanBindData& other)
        : TableFuncBindData{other}, query{other.query}, columnTypes{copyVector(other.columnTypes)},
          columnNames{other.columnNames}, converter{other.converter}, connector{other.connector} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<DuckDBScanBindData>(*this);
    }
};

struct DuckDBScanSharedState final : function::BaseScanSharedStateWithNumRows {
    explicit DuckDBScanSharedState(std::unique_ptr<duckdb::MaterializedQueryResult> queryResult);

    std::unique_ptr<duckdb::MaterializedQueryResult> queryResult;
};

function::TableFunction getScanFunction(DuckDBScanBindData bindData);

} // namespace duckdb_extension
} // namespace kuzu

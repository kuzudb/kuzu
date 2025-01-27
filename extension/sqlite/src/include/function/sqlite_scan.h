#pragma once

#include "function/duckdb_scan.h"

namespace kuzu {
namespace sqlite_extension {

struct SqliteScanBindData : public duckdb_extension::DuckDBScanBindData {
    SqliteScanBindData(std::string query, const std::vector<common::LogicalType>& columnTypes,
        std::vector<std::string> columnNames, const duckdb_extension::DuckDBConnector& connector)
        : duckdb_extension::DuckDBScanBindData{std::move(query), columnTypes, columnNames,
              connector} {}

    SqliteScanBindData(const SqliteScanBindData& other)
        : duckdb_extension::DuckDBScanBindData{other} {}

    std::string getQuery(const main::ClientContext& context) const override;

    std::vector<common::LogicalType> getColumnTypes(
        const main::ClientContext& context) const override;

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<SqliteScanBindData>(*this);
    }
};

} // namespace sqlite_extension
} // namespace kuzu

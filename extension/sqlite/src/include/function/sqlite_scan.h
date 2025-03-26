#pragma once

#include "function/duckdb_scan.h"

namespace kuzu {
namespace sqlite_extension {

class SQLiteTableScanInfo : public duckdb_extension::DuckDBTableScanInfo {
public:
    SQLiteTableScanInfo(std::string templateQuery, std::vector<common::LogicalType> columnTypes,
        std::vector<std::string> columnNames, const duckdb_extension::DuckDBConnector& connector)
        : DuckDBTableScanInfo{std::move(templateQuery), std::move(columnTypes),
              std::move(columnNames), connector} {}

    SQLiteTableScanInfo(const SQLiteTableScanInfo& other) = default;

    std::string getTemplateQuery(const main::ClientContext& context) const override;

    std::vector<common::LogicalType> getColumnTypes(
        const main::ClientContext& context) const override;
};

} // namespace sqlite_extension
} // namespace kuzu

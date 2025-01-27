#pragma once

#include "connector/duckdb_connector.h"

namespace kuzu {
namespace sqlite_extension {

class SqliteConnector final : public duckdb_extension::DuckDBConnector {
public:
    void connect(const std::string& dbPath, const std::string& catalogName,
        const std::string& schemaName, main::ClientContext* context) override;

    std::shared_ptr<duckdb_extension::DuckDBScanBindData> getScanBindData(std::string query,
        const std::vector<common::LogicalType>& columnTypes,
        const std::vector<std::string>& columnNames) const override;
};

} // namespace sqlite_extension
} // namespace kuzu

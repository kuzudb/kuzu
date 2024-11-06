#pragma once

#include "connector/duckdb_connector.h"

namespace kuzu {
namespace sqlite_extension {

class SqliteConnector : public duckdb_extension::DuckDBConnector {
public:
    void connect(const std::string& dbPath, const std::string& catalogName,
        main::ClientContext* context) override;
};

} // namespace sqlite_extension
} // namespace kuzu

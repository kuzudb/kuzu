#pragma once

#include "duckdb_connector.h"

namespace kuzu {
namespace duckdb_extension {

class HTTPDuckDBConnector : public DuckDBConnector {
public:
    void connect(const std::string& dbPath, const std::string& catalogName,
        const std::string& schemaName, main::ClientContext* context) override;
};

class S3DuckDBConnector : public DuckDBConnector {
public:
    void connect(const std::string& dbPath, const std::string& catalogName,
        const std::string& schemaName, main::ClientContext* context) override;
};

} // namespace duckdb_extension
} // namespace kuzu

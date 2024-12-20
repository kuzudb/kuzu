#pragma once

#include "connector/duckdb_connector.h"

namespace kuzu {
namespace iceberg_extension {

class IcebergConnector : public duckdb_extension::DuckDBConnector {
public:
    void connect(const std::string& dbPath, const std::string& catalogName,
        const std::string& schemaName, main::ClientContext* context) override;
};

} // namespace iceberg_extension
} // namespace kuzu

#pragma once

#include "connector/duckdb_connector.h"

namespace kuzu {
namespace delta_extension {

class DeltaConnector : public duckdb_extension::DuckDBConnector {
public:
    void connect(const std::string& dbPath, const std::string& catalogName,
        const std::string& schemaName, main::ClientContext* context) override;
};

} // namespace delta_extension
} // namespace kuzu

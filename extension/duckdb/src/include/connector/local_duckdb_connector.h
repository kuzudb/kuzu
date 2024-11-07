#pragma once

#include "connector/duckdb_connector.h"

namespace kuzu {
namespace duckdb_extension {

class LocalDuckDBConnector : public DuckDBConnector {
public:
    void connect(const std::string& dbPath, const std::string& catalogName,
        main::ClientContext* context) override;
};

} // namespace duckdb_extension
} // namespace kuzu

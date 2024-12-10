#pragma once

#include "connector/duckdb_connector.h"

namespace kuzu {
namespace delta_extension {

class DeltaConnector : public duckdb_extension::DuckDBConnector {
public:
    std::string format = "DELTA";
    void connect(const std::string& dbPath, const std::string& catalogName,
        main::ClientContext* context) override;
};

} // namespace delta_extension
} // namespace kuzu

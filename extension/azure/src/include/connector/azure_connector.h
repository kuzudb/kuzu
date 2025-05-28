#pragma once

#include "connector/duckdb_connector.h"

namespace kuzu {
namespace azure_extension {

class AzureConnector : public duckdb_extension::DuckDBConnector {
public:
    void connect(const std::string& dbPath, const std::string& catalogName,
        const std::string& schemaName, main::ClientContext* context) override;

    void initRemoteAzureSecrets(main::ClientContext* context) const;
};

} // namespace azure_extension
} // namespace kuzu

#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
// Supress warnings from duckdb.hpp
#undef ARROW_FLAG_DICTIONARY_ORDERED
#include "duckdb.hpp"
#pragma GCC diagnostic pop

#include "connector/duckdb_secret_manager.h"
#include "function/duckdb_scan.h"
#include "s3fs_config.h"

namespace kuzu {
namespace main {
class ClientContext;
}

namespace duckdb_extension {

enum class DuckDBConnectionType : uint8_t {
    LOCAL = 0,
    HTTP = 1,
    S3 = 2,
};

class DuckDBConnector {
public:
    virtual ~DuckDBConnector() = default;

    virtual void connect(const std::string& dbPath, const std::string& catalogName,
        const std::string& schemaName, main::ClientContext* context) = 0;

    virtual std::shared_ptr<DuckDBTableScanInfo> getTableScanInfo(std::string query,
        std::vector<common::LogicalType> columnTypes, std::vector<std::string> columnNames) const {
        return std::make_shared<DuckDBTableScanInfo>(std::move(query), std::move(columnTypes),
            std::move(columnNames), *this);
    }

    std::unique_ptr<duckdb::MaterializedQueryResult> executeQuery(std::string query) const;

    void initRemoteFSSecrets(main::ClientContext* context) const {
        for (auto& fsConfig : httpfs_extension::S3FileSystemConfig::getAvailableConfigs()) {
            executeQuery(DuckDBSecretManager::getRemoteS3FSSecret(context, fsConfig));
        }
    }

protected:
    std::unique_ptr<duckdb::DuckDB> instance;
    std::unique_ptr<duckdb::Connection> connection;
};

} // namespace duckdb_extension
} // namespace kuzu

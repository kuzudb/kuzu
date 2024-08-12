#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
// Supress warnings from duckdb.hpp
#undef ARROW_FLAG_DICTIONARY_ORDERED
#include "duckdb.hpp"
#pragma GCC diagnostic pop

#include "common/assert.h"

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
        main::ClientContext* context) = 0;

    std::unique_ptr<duckdb::MaterializedQueryResult> executeQuery(std::string query) const;

protected:
    std::unique_ptr<duckdb::DuckDB> instance;
    std::unique_ptr<duckdb::Connection> connection;
};

class LocalDuckDBConnector : public DuckDBConnector {
public:
    void connect(const std::string& dbPath, const std::string& catalogName,
        main::ClientContext* context) override;
};

class HTTPDuckDBConnector : public DuckDBConnector {
public:
    void connect(const std::string& dbPath, const std::string& catalogName,
        main::ClientContext* context) override;
};

class S3DuckDBConnector : public DuckDBConnector {
public:
    void connect(const std::string& dbPath, const std::string& catalogName,
        main::ClientContext* context) override;
};

class DuckDBConnectorFactory {
public:
    static std::unique_ptr<DuckDBConnector> getDuckDBConnector(const std::string& dbPath);
};

} // namespace duckdb_extension
} // namespace kuzu

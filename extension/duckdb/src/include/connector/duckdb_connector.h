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
        const std::string& schemaName, main::ClientContext* context) = 0;

    std::unique_ptr<duckdb::MaterializedQueryResult> executeQuery(std::string query) const;

protected:
    std::unique_ptr<duckdb::DuckDB> instance;
    std::unique_ptr<duckdb::Connection> connection;
};

} // namespace duckdb_extension
} // namespace kuzu

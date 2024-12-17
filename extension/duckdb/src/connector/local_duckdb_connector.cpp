#include "connector/local_duckdb_connector.h"

#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"
#include "main/client_context.h"

namespace kuzu {
namespace duckdb_extension {

void LocalDuckDBConnector::connect(const std::string& dbPath, const std::string& /*catalogName*/,
    const std::string& /*schemaName*/, main::ClientContext* context) {
    if (!context->getVFSUnsafe()->fileOrPathExists(dbPath, context)) {
        throw common::RuntimeException{
            common::stringFormat("Given duckdb database path {} does not exist.", dbPath)};
    }
    duckdb::DBConfig dbConfig{true /* isReadOnly */};
    instance = std::make_unique<duckdb::DuckDB>(dbPath, &dbConfig);
    connection = std::make_unique<duckdb::Connection>(*instance);
}

} // namespace duckdb_extension
} // namespace kuzu

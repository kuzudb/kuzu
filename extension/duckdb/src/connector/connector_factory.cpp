#include "connector/connector_factory.h"

#include "connector/duckdb_connector.h"
#include "connector/local_duckdb_connector.h"
#include "connector/remote_duckdb_connector.h"

namespace kuzu {
namespace duckdb_extension {

static DuckDBConnectionType getConnectionType(const std::string& path) {
    if (path.rfind("https://", 0) == 0 || path.rfind("http://", 0) == 0) {
        return DuckDBConnectionType::HTTP;
    } else if (path.rfind("s3://", 0) == 0) {
        return DuckDBConnectionType::S3;
    } else {
        return DuckDBConnectionType::LOCAL;
    }
}

std::unique_ptr<DuckDBConnector> DuckDBConnectorFactory::getDuckDBConnector(
    const std::string& dbPath) {
    auto type = getConnectionType(dbPath);
    switch (type) {
    case DuckDBConnectionType::LOCAL:
        return std::make_unique<LocalDuckDBConnector>();
    case DuckDBConnectionType::HTTP:
        return std::make_unique<HTTPDuckDBConnector>();
    case DuckDBConnectionType::S3:
        return std::make_unique<S3DuckDBConnector>();
    default:
        KU_UNREACHABLE;
    }
}

} // namespace duckdb_extension
} // namespace kuzu

#include "connector/duckdb_connector.h"

#include "common/exception/runtime.h"
#include "main/client_context.h"

namespace kuzu {
namespace duckdb_extension {

std::unique_ptr<duckdb::MaterializedQueryResult> DuckDBConnector::executeQuery(
    std::string query) const {
    KU_ASSERT(instance != nullptr && connection != nullptr);
    auto result = connection->Query(query);
    if (result->HasError()) {
        throw common::Exception{result->GetError()};
    }
    return result;
}

} // namespace duckdb_extension
} // namespace kuzu

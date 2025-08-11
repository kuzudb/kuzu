#include "connector/duckdb_connector.h"

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

std::unique_ptr<duckdb::StreamQueryResult> DuckDBConnector::executeStreamQuery(
    std::string query) const {
    KU_ASSERT(instance != nullptr && connection != nullptr);
    auto result = connection->SendQuery(query);
    if (result->HasError()) {
        throw common::Exception{result->GetError()};
    }
    return duckdb::unique_ptr_cast<duckdb::QueryResult, duckdb::StreamQueryResult>(
        std::move(result));
}

} // namespace duckdb_extension
} // namespace kuzu

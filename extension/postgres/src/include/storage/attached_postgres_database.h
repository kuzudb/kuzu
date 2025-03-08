#pragma once

#include "connector/postgres_connector.h"
#include "storage/attached_duckdb_database.h"

namespace kuzu {
namespace postgres_extension {

class AttachedPostgresDatabase final : public duckdb_extension::AttachedDuckDBDatabase {
public:
    AttachedPostgresDatabase(std::string dbName, std::string dbType,
        std::unique_ptr<extension::CatalogExtension> catalog,
        std::unique_ptr<PostgresConnector> connector, std::string attachedCatalogNameInDuckDB)
        : duckdb_extension::AttachedDuckDBDatabase{std::move(dbName), std::move(dbType),
              std::move(catalog), std::move(connector)},
          attachedCatalogNameInDuckDB{std::move(attachedCatalogNameInDuckDB)} {}

    std::unique_ptr<duckdb::MaterializedQueryResult> executeQuery(const std::string& query) const {
        return connector->executeQuery(query);
    }

public:
    std::string getAttachedCatalogNameInDuckDB() const { return attachedCatalogNameInDuckDB; }

private:
    std::string attachedCatalogNameInDuckDB;
};

} // namespace postgres_extension
} // namespace kuzu

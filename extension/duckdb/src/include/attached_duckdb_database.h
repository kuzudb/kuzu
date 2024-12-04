#pragma once

#include "connector/duckdb_connector.h"
#include "main/attached_database.h"

namespace kuzu {
namespace duckdb_extension {

class DuckDBConnector;

class AttachedDuckDBDatabase final : public main::AttachedDatabase {
public:
    AttachedDuckDBDatabase(std::string dbName, std::string dbType,
        std::unique_ptr<extension::CatalogExtension> catalog,
        std::unique_ptr<DuckDBConnector> connector)
        : main::AttachedDatabase{std::move(dbName), std::move(dbType), std::move(catalog)},
          connector{std::move(connector)} {}

private:
    std::unique_ptr<DuckDBConnector> connector;
};

} // namespace duckdb_extension
} // namespace kuzu

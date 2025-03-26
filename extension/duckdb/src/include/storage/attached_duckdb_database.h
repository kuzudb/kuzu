#pragma once

#include "connector/duckdb_connector.h"
#include "main/attached_database.h"

namespace kuzu {
namespace duckdb_extension {

class AttachedDuckDBDatabase : public main::AttachedDatabase {
public:
    AttachedDuckDBDatabase(std::string dbName, std::string dbType,
        std::unique_ptr<extension::CatalogExtension> catalog,
        std::unique_ptr<DuckDBConnector> connector)
        : main::AttachedDatabase{std::move(dbName), std::move(dbType), std::move(catalog)},
          connector{std::move(connector)} {}

    const DuckDBConnector& getConnector() const { return *connector; }

protected:
    std::unique_ptr<DuckDBConnector> connector;
};

} // namespace duckdb_extension
} // namespace kuzu

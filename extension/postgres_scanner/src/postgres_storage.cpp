#include "postgres_storage.h"

#include <regex>

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "duckdb_type_converter.h"
#include "postgres_catalog.h"

namespace kuzu {
namespace postgres_scanner {

std::string extractDBName(const std::string& connectionInfo) {
    std::regex pattern("dbname=([^ ]+)");
    std::smatch match;
    if (std::regex_search(connectionInfo, match, pattern)) {
        return match.str(1);
    }
    throw common::RuntimeException{"Invalid postgresql connection string."};
}

std::unique_ptr<main::AttachedDatabase> attachPostgres(std::string dbName, std::string dbPath,
    main::ClientContext* clientContext) {
    auto catalogName = extractDBName(dbPath);
    if (dbName == "") {
        dbName = catalogName;
    }
    auto postgresCatalog = std::make_unique<PostgresCatalogContent>();
    postgresCatalog->init(dbPath, catalogName, clientContext);
    return std::make_unique<main::AttachedDatabase>(dbName, std::move(postgresCatalog));
}

PostgresStorageExtension::PostgresStorageExtension() : StorageExtension{attachPostgres} {}

bool PostgresStorageExtension::canHandleDB(std::string dbType) const {
    common::StringUtils::toUpper(dbType);
    return dbType == "POSTGRES";
}

} // namespace postgres_scanner
} // namespace kuzu

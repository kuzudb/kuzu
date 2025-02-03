#include "storage/postgres_storage.h"

#include <regex>

#include "catalog/duckdb_catalog.h"
#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "connector/postgres_connector.h"
#include "extension/extension.h"
#include "function/clear_cache.h"
#include "storage/attached_duckdb_database.h"
#include "storage/duckdb_storage.h"

namespace kuzu {
namespace postgres_extension {

std::string extractDBName(const std::string& connectionInfo) {
    std::regex pattern("dbname=([^ ]+)");
    std::smatch match;
    if (std::regex_search(connectionInfo, match, pattern)) {
        return match.str(1);
    }
    throw common::RuntimeException{"Invalid postgresql connection string."};
}

std::unique_ptr<main::AttachedDatabase> attachPostgres(std::string dbName, std::string dbPath,
    main::ClientContext* clientContext, const binder::AttachOption& attachOption) {
    auto catalogName = extractDBName(dbPath);
    if (dbName == "") {
        dbName = catalogName;
    }
    auto schemaName = duckdb_extension::DuckDBCatalog::bindSchemaName(attachOption,
        PostgresStorageExtension::DEFAULT_SCHEMA_NAME);
    auto connector = std::make_unique<PostgresConnector>();
    connector->connect(dbPath, catalogName, schemaName, clientContext);
    auto catalog = std::make_unique<duckdb_extension::DuckDBCatalog>(dbPath, catalogName,
        schemaName, clientContext, *connector, attachOption);
    catalog->init();
    return std::make_unique<duckdb_extension::AttachedDuckDBDatabase>(dbName,
        PostgresStorageExtension::DB_TYPE, std::move(catalog), std::move(connector));
}

PostgresStorageExtension::PostgresStorageExtension(main::Database* database)
    : StorageExtension{attachPostgres} {
    extension::ExtensionUtils::addStandaloneTableFunc<duckdb_extension::ClearCacheFunction>(
        *database);
}

bool PostgresStorageExtension::canHandleDB(std::string dbType_) const {
    common::StringUtils::toUpper(dbType_);
    return dbType_ == DB_TYPE;
}

} // namespace postgres_extension
} // namespace kuzu

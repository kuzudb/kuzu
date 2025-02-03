#include "storage/sqlite_storage.h"

#include <filesystem>

#include "catalog/duckdb_catalog.h"
#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "connector/sqlite_connector.h"
#include "extension/extension.h"
#include "function/clear_cache.h"
#include "storage/attached_duckdb_database.h"

namespace kuzu {
namespace sqlite_extension {

static std::string getCatalogNameFromPath(const std::string& dbPath) {
    std::filesystem::path path(dbPath);
    return path.stem().string();
}

std::unique_ptr<main::AttachedDatabase> attachSqlite(std::string dbName, std::string dbPath,
    main::ClientContext* clientContext, const binder::AttachOption& attachOption) {
    auto catalogName = getCatalogNameFromPath(dbPath);
    if (dbName == "") {
        dbName = catalogName;
    }
    auto connector = std::make_unique<SqliteConnector>();
    connector->connect(dbPath, catalogName, SqliteStorageExtension::DEFAULT_SCHEMA_NAME,
        clientContext);
    auto catalog = std::make_unique<duckdb_extension::DuckDBCatalog>(dbPath, catalogName,
        SqliteStorageExtension::DEFAULT_SCHEMA_NAME, clientContext, *connector, attachOption);
    catalog->init();
    return std::make_unique<duckdb_extension::AttachedDuckDBDatabase>(dbName,
        SqliteStorageExtension::DB_TYPE, std::move(catalog), std::move(connector));
}

SqliteStorageExtension::SqliteStorageExtension(main::Database* database)
    : StorageExtension{attachSqlite} {
    extension::ExtensionUtils::addStandaloneTableFunc<duckdb_extension::ClearCacheFunction>(
        *database);
}

bool SqliteStorageExtension::canHandleDB(std::string dbType_) const {
    common::StringUtils::toUpper(dbType_);
    return dbType_ == DB_TYPE;
}

} // namespace sqlite_extension
} // namespace kuzu

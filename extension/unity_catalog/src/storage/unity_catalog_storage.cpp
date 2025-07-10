#include "storage/unity_catalog_storage.h"

#include "catalog/duckdb_catalog.h"
#include "common/string_utils.h"
#include "connector/unity_catalog_connector.h"
#include "extension/extension.h"
#include "function/clear_cache.h"
#include "storage/attached_duckdb_database.h"

namespace kuzu {
namespace unity_catalog_extension {

std::unique_ptr<main::AttachedDatabase> attachUnityCatalog(std::string dbName, std::string dbPath,
    main::ClientContext* clientContext, const binder::AttachOption& attachOption) {
    if (dbName == "") {
        dbName = dbPath;
    }
    auto connector = std::make_unique<UnityCatalogConnector>();
    connector->connect(dbPath, dbName, UnityCatalogStorageExtension::DEFAULT_SCHEMA_NAME,
        clientContext);
    auto catalog = std::make_unique<duckdb_extension::DuckDBCatalog>(dbPath, dbPath,
        UnityCatalogStorageExtension::DEFAULT_SCHEMA_NAME, clientContext, *connector, attachOption);
    catalog->init();
    return std::make_unique<duckdb_extension::AttachedDuckDBDatabase>(dbName,
        UnityCatalogStorageExtension::DB_TYPE, std::move(catalog), std::move(connector));
}

UnityCatalogStorageExtension::UnityCatalogStorageExtension(main::Database& database)
    : StorageExtension{attachUnityCatalog} {
    extension::ExtensionUtils::addStandaloneTableFunc<duckdb_extension::ClearCacheFunction>(
        database);
}

bool UnityCatalogStorageExtension::canHandleDB(std::string dbType_) const {
    common::StringUtils::toUpper(dbType_);
    return dbType_ == DB_TYPE;
}

} // namespace unity_catalog_extension
} // namespace kuzu

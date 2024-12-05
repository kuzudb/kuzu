#include "storage/duckdb_storage.h"

#include <filesystem>

#include "catalog/duckdb_catalog.h"
#include "common/file_system/virtual_file_system.h"
#include "common/string_utils.h"
#include "connector/connector_factory.h"
#include "extension/extension.h"
#include "function/clear_cache.h"
#include "storage/attached_duckdb_database.h"

namespace kuzu {
namespace duckdb_extension {

static std::string getCatalogNameFromPath(const std::string& dbPath) {
    std::filesystem::path path(dbPath);
    return path.stem().string();
}

std::unique_ptr<main::AttachedDatabase> attachDuckDB(std::string dbName, std::string dbPath,
    main::ClientContext* clientContext, const binder::AttachOption& attachOption) {
    auto catalogName = getCatalogNameFromPath(dbPath);
    if (dbName == "") {
        dbName = catalogName;
    }
    auto connector = DuckDBConnectorFactory::getDuckDBConnector(dbPath);
    connector->connect(dbPath, catalogName, clientContext);
    auto duckdbCatalog = std::make_unique<DuckDBCatalog>(std::move(dbPath), std::move(catalogName),
        DuckDBStorageExtension::DEFAULT_SCHEMA_NAME, clientContext, *connector, attachOption);
    duckdbCatalog->init();
    return std::make_unique<AttachedDuckDBDatabase>(dbName, DuckDBStorageExtension::DB_TYPE,
        std::move(duckdbCatalog), std::move(connector));
}

DuckDBStorageExtension::DuckDBStorageExtension(main::Database* database)
    : StorageExtension{attachDuckDB} {
    auto duckDBClearCacheFunction = std::make_unique<ClearCacheFunction>();
    extension::ExtensionUtils::registerTableFunction(*database,
        std::move(duckDBClearCacheFunction));
}

bool DuckDBStorageExtension::canHandleDB(std::string dbType_) const {
    common::StringUtils::toUpper(dbType_);
    return dbType_ == DB_TYPE;
}

} // namespace duckdb_extension
} // namespace kuzu

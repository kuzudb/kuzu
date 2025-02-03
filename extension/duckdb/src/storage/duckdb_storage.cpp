#include "storage/duckdb_storage.h"

#include <filesystem>

#include "binder/bound_attach_info.h"
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
    auto schemaName =
        DuckDBCatalog::bindSchemaName(attachOption, DuckDBStorageExtension::DEFAULT_SCHEMA_NAME);
    auto connector = DuckDBConnectorFactory::getDuckDBConnector(dbPath);
    connector->connect(dbPath, catalogName, schemaName, clientContext);

    auto duckdbCatalog = std::make_unique<DuckDBCatalog>(std::move(dbPath), std::move(catalogName),
        schemaName, clientContext, *connector, attachOption);
    duckdbCatalog->init();
    return std::make_unique<AttachedDuckDBDatabase>(dbName, DuckDBStorageExtension::DB_TYPE,
        std::move(duckdbCatalog), std::move(connector));
}

DuckDBStorageExtension::DuckDBStorageExtension(main::Database* db)
    : StorageExtension{attachDuckDB} {
    extension::ExtensionUtils::addStandaloneTableFunc<ClearCacheFunction>(*db);
}

bool DuckDBStorageExtension::canHandleDB(std::string dbType_) const {
    common::StringUtils::toUpper(dbType_);
    return dbType_ == DB_TYPE;
}

} // namespace duckdb_extension
} // namespace kuzu

#include "duckdb_storage.h"

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/exception/binder.h"
#include "duckdb_catalog.h"

namespace kuzu {
namespace duckdb_scanner {

std::unique_ptr<main::AttachedDatabase> attachDuckDB(std::string dbName, std::string dbPath,
    main::ClientContext* clientContext) {
    if (dbName == "") {
        if (dbPath.find('.') != std::string::npos) {
            auto fileNamePos = dbPath.find_last_of('/') + 1;
            dbName = dbPath.substr(fileNamePos, dbPath.find_last_of('.') - fileNamePos);
        } else {
            dbName = dbPath;
        }
    }
    auto duckdbCatalog = std::make_unique<DuckDBCatalogContent>();
    duckdbCatalog->init(dbPath, dbName, clientContext);
    return std::make_unique<main::AttachedDatabase>(dbName, std::move(duckdbCatalog));
}

DuckDBStorageExtension::DuckDBStorageExtension() : StorageExtension{attachDuckDB} {}

bool DuckDBStorageExtension::canHandleDB(std::string dbType) const {
    common::StringUtils::toUpper(dbType);
    return dbType == "DUCKDB";
}

} // namespace duckdb_scanner
} // namespace kuzu

#include "duckdb_storage.h"

#include <filesystem>

#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"
#include "common/string_utils.h"
#include "duckdb_catalog.h"

namespace kuzu {
namespace duckdb_scanner {

static std::string getCatalogNameFromPath(const std::string& dbPath) {
    std::filesystem::path path(dbPath);
    return path.stem().string();
}

std::unique_ptr<main::AttachedDatabase> attachDuckDB(std::string dbName, std::string dbPath,
    main::ClientContext* clientContext) {
    auto catalogName = getCatalogNameFromPath(dbPath);
    if (dbName == "") {
        dbName = catalogName;
    }
    auto duckdbCatalog = std::make_unique<DuckDBCatalogContent>();
    auto vfs = std::make_unique<common::VirtualFileSystem>();
    if (!vfs->fileOrPathExists(dbPath)) {
        throw common::RuntimeException{
            common::stringFormat("'{}' is not a valid duckdb database path.", dbPath)};
    }
    duckdbCatalog->init(dbPath, catalogName, clientContext);
    return std::make_unique<main::AttachedDatabase>(dbName, std::move(duckdbCatalog));
}

DuckDBStorageExtension::DuckDBStorageExtension() : StorageExtension{attachDuckDB} {}

bool DuckDBStorageExtension::canHandleDB(std::string dbType) const {
    common::StringUtils::toUpper(dbType);
    return dbType == "DUCKDB";
}

} // namespace duckdb_scanner
} // namespace kuzu

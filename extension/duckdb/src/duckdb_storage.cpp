#include "duckdb_storage.h"

#include <filesystem>

#include "common/exception/runtime.h"
#include "common/file_system/virtual_file_system.h"
#include "common/string_utils.h"
#include "duckdb_catalog.h"
#include "duckdb_functions.h"
#include "extension/extension.h"

namespace kuzu {
namespace duckdb_extension {

static std::string getCatalogNameFromPath(const std::string& dbPath) {
    std::filesystem::path path(dbPath);
    return path.stem().string();
}

static void validateDuckDBPathExistence(const std::string& dbPath) {
    auto vfs = std::make_unique<common::VirtualFileSystem>();
    if (!vfs->fileOrPathExists(dbPath)) {
        throw common::RuntimeException{
            common::stringFormat("'{}' is not a valid duckdb database path.", dbPath)};
    }
}

std::unique_ptr<main::AttachedDatabase> attachDuckDB(std::string dbName, std::string dbPath,
    main::ClientContext* clientContext) {
    auto catalogName = getCatalogNameFromPath(dbPath);
    if (dbName == "") {
        dbName = catalogName;
    }
    validateDuckDBPathExistence(dbPath);
    auto duckdbCatalog = std::make_unique<DuckDBCatalog>(dbPath, catalogName, clientContext);
    duckdbCatalog->init();
    return std::make_unique<main::AttachedDatabase>(dbName, DuckDBStorageExtension::dbType,
        std::move(duckdbCatalog));
}

DuckDBStorageExtension::DuckDBStorageExtension(main::Database* database)
    : StorageExtension{attachDuckDB} {
    auto duckDBClearCacheFunction = std::make_unique<DuckDBClearCacheFunction>();
    extension::ExtensionUtils::registerTableFunction(*database,
        std::move(duckDBClearCacheFunction));
}

bool DuckDBStorageExtension::canHandleDB(std::string dbType_) const {
    common::StringUtils::toUpper(dbType_);
    return dbType_ == dbType;
}

} // namespace duckdb_extension
} // namespace kuzu

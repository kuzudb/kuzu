#include "sqlite_storage.h"

#include <filesystem>
#include <regex>

#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "duckdb_functions.h"
#include "extension/extension.h"
#include "sqlite_catalog.h"

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
    auto sqliteCatalog =
        std::make_unique<SqliteCatalog>(dbPath, catalogName, clientContext, attachOption);
    sqliteCatalog->init();
    return std::make_unique<main::AttachedDatabase>(dbName, SqliteStorageExtension::DB_TYPE,
        std::move(sqliteCatalog));
}

SqliteStorageExtension::SqliteStorageExtension(main::Database* database)
    : StorageExtension{attachSqlite} {
    auto clearCacheFunction = std::make_unique<duckdb_extension::ClearCacheFunction>();
    extension::ExtensionUtils::registerTableFunction(*database, std::move(clearCacheFunction));
}

bool SqliteStorageExtension::canHandleDB(std::string dbType_) const {
    common::StringUtils::toUpper(dbType_);
    return dbType_ == DB_TYPE;
}

} // namespace sqlite_extension
} // namespace kuzu

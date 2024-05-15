#include "postgres_storage.h"

#include <regex>

#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "duckdb_functions.h"
#include "extension/extension.h"
#include "postgres_catalog.h"

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
    auto postgresCatalog =
        std::make_unique<PostgresCatalog>(dbPath, catalogName, clientContext, attachOption);
    postgresCatalog->init();
    return std::make_unique<main::AttachedDatabase>(dbName, PostgresStorageExtension::DB_TYPE,
        std::move(postgresCatalog));
}

PostgresStorageExtension::PostgresStorageExtension(main::Database* database)
    : StorageExtension{attachPostgres} {
    auto clearCacheFunction = std::make_unique<duckdb_extension::ClearCacheFunction>();
    extension::ExtensionUtils::registerTableFunction(*database, std::move(clearCacheFunction));
}

bool PostgresStorageExtension::canHandleDB(std::string dbType_) const {
    common::StringUtils::toUpper(dbType_);
    return dbType_ == DB_TYPE;
}

} // namespace postgres_extension
} // namespace kuzu

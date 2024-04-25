#include "postgres_storage.h"

#include <regex>

#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/exception/runtime.h"
#include "common/string_utils.h"
#include "extension/extension.h"
#include "postgres_catalog.h"
#include "postgres_functions.h"

namespace kuzu {
namespace postgres_scanner {

std::string extractDBName(const std::string& connectionInfo) {
    std::regex pattern("dbname=([^ ]+)");
    std::smatch match;
    if (std::regex_search(connectionInfo, match, pattern)) {
        return match.str(1);
    }
    throw common::RuntimeException{"Invalid postgresql connection string."};
}

std::unique_ptr<main::AttachedDatabase> attachPostgres(std::string dbName, std::string dbPath,
    main::ClientContext* clientContext) {
    auto catalogName = extractDBName(dbPath);
    if (dbName == "") {
        dbName = catalogName;
    }
    auto postgresCatalog = std::make_unique<PostgresCatalog>(dbPath, catalogName, clientContext);
    postgresCatalog->init();
    return std::make_unique<main::AttachedDatabase>(dbName, PostgresStorageExtension::dbType,
        std::move(postgresCatalog));
}

PostgresStorageExtension::PostgresStorageExtension(main::Database* database)
    : StorageExtension{attachPostgres} {
    auto postgresClearCacheFunction = std::make_unique<PostgresClearCacheFunction>();
    extension::ExtensionUtils::registerTableFunction(*database,
        std::move(postgresClearCacheFunction));
}

bool PostgresStorageExtension::canHandleDB(std::string dbType_) const {
    common::StringUtils::toUpper(dbType_);
    return dbType_ == dbType;
}

} // namespace postgres_scanner
} // namespace kuzu

#include "duckdb_storage.h"

#include "duckdb_scan.h"

namespace kuzu {
namespace duckdb_scanner {

std::unique_ptr<main::AttachedDatabase> attachDuckDB(std::string dbName, std::string dbPath) {
    if (dbName == "") {
        if (dbPath.find('.') != std::string::npos) {
            auto fileNamePos = dbPath.find_last_of('/') + 1;
            dbName = dbPath.substr(fileNamePos, dbPath.find_last_of('.') - fileNamePos);
        } else {
            dbName = dbPath;
        }
    }
    return std::make_unique<main::AttachedDatabase>(dbName, getScanFunction(dbPath));
}

DuckDBStorageExtension::DuckDBStorageExtension() : StorageExtension{attachDuckDB} {}

bool DuckDBStorageExtension::canHandleDB(std::string dbType) const {
    common::StringUtils::toUpper(dbType);
    if (dbType == "DUCKDB") {
        return true;
    }
    return false;
}

} // namespace duckdb_scanner
} // namespace kuzu

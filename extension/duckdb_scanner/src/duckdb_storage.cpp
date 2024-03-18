#include "duckdb_storage.h"

#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog_entry/table_catalog_entry.h"
#include "common/exception/binder.h"
#include "duckdb_catalog.h"
#include "duckdb_scan.h"
#include "duckdb_type_converter.h"

namespace kuzu {
namespace duckdb_scanner {

static bool getTableInfo(duckdb::Connection& con, std::string tableName,
    std::vector<common::LogicalType>& columnTypes, std::vector<std::string>& columnNames) {
    auto result = con.Query(
        common::stringFormat("select data_type,column_name from information_schema.columns where "
                             "table_name = '{}' and table_schema='main';",
            tableName));
    if (result->RowCount() == 0) {
        return false;
    }
    columnTypes.reserve(result->RowCount());
    columnNames.reserve(result->RowCount());
    for (auto i = 0u; i < result->RowCount(); i++) {
        try {
            columnTypes.push_back(DuckDBTypeConverter::convertDuckDBType(
                result->GetValue(0, i).GetValue<std::string>()));
        } catch (common::BinderException& e) { return false; }
        columnNames.push_back(result->GetValue(1, i).GetValue<std::string>());
    }
    return true;
}

std::unique_ptr<binder::BoundCreateTableInfo> getCreateTableInfo(
    duckdb::Connection& con, std::string tableName, std::string dbPath) {
    std::vector<common::LogicalType> columnTypes;
    std::vector<std::string> columnNames;
    if (!getTableInfo(con, tableName, columnTypes, columnNames)) {
        return nullptr;
    }
    std::vector<binder::PropertyInfo> propertyInfos;
    for (auto i = 0u; i < columnNames.size(); i++) {
        auto propertyInfo = binder::PropertyInfo(columnNames[i], columnTypes[i]);
        propertyInfos.push_back(std::move(propertyInfo));
    }
    return std::make_unique<binder::BoundCreateTableInfo>(common::TableType::FOREIGN, tableName,
        std::make_unique<BoundExtraCreateDuckDBTableInfo>(
            std::move(dbPath), std::move(propertyInfos)));
}

std::unique_ptr<DuckDBCatalogContent> createCatalog(
    std::string dbPath, main::ClientContext* context) {
    duckdb::DuckDB db(dbPath);
    duckdb::Connection con(db);
    auto query = "select table_name from information_schema.tables where table_schema = 'main';";
    auto result = con.SendQuery(query);
    std::unique_ptr<duckdb::DataChunk> resultChunk;
    try {
        resultChunk = result->Fetch();
    } catch (std::exception& e) { return 0; }
    if (resultChunk == nullptr) {
        return 0;
    }
    auto tableNamesVector = std::make_unique<common::ValueVector>(
        common::LogicalTypeID::STRING, context->getMemoryManager());
    duckdb_conversion_func_t conversionFunc;
    getDuckDBVectorConversionFunc(common::PhysicalTypeID::STRING, conversionFunc);
    conversionFunc(resultChunk->data[0], *tableNamesVector, resultChunk->size());
    std::unique_ptr<DuckDBCatalogContent> catalogContent = std::make_unique<DuckDBCatalogContent>();
    for (auto i = 0u; i < resultChunk->size(); i++) {
        auto tableName = tableNamesVector->getValue<common::ku_string_t>(i).getAsString();
        auto createTableInfo = getCreateTableInfo(con, tableName, dbPath);
        if (createTableInfo != nullptr) {
            catalogContent->createForeignTable(*createTableInfo);
        }
    }
    return catalogContent;
}

std::unique_ptr<main::AttachedDatabase> attachDuckDB(
    std::string dbName, std::string dbPath, main::ClientContext* clientContext) {
    if (dbName == "") {
        if (dbPath.find('.') != std::string::npos) {
            auto fileNamePos = dbPath.find_last_of('/') + 1;
            dbName = dbPath.substr(fileNamePos, dbPath.find_last_of('.') - fileNamePos);
        } else {
            dbName = dbPath;
        }
    }
    return std::make_unique<main::AttachedDatabase>(dbName, createCatalog(dbPath, clientContext));
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

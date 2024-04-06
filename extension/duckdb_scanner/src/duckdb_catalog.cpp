#include "duckdb_catalog.h"

#include "common/exception/binder.h"
#include "duckdb_table_catalog_entry.h"
#include "duckdb_type_converter.h"

namespace kuzu {
namespace duckdb_scanner {

void DuckDBCatalogContent::init(const std::string& dbPath, const std::string& catalogName,
    main::ClientContext* context) {
    auto [db, con] = getConnection(dbPath);
    auto query = common::stringFormat(
        "select table_name from information_schema.tables where table_catalog = '{}' and "
        "table_schema = '{}';",
        catalogName, getDefaultSchemaName());
    auto result = con.Query(query);
    std::unique_ptr<duckdb::DataChunk> resultChunk;
    try {
        resultChunk = result->Fetch();
    } catch (std::exception& e) {
        throw common::BinderException(e.what());
    }
    if (resultChunk->size() == 0) {
        return;
    }
    common::ValueVector tableNamesVector{*common::LogicalType::STRING(),
        context->getMemoryManager()};
    duckdb_scanner::duckdb_conversion_func_t conversionFunc;
    duckdb_scanner::getDuckDBVectorConversionFunc(common::PhysicalTypeID::STRING, conversionFunc);
    conversionFunc(resultChunk->data[0], tableNamesVector, resultChunk->size());
    for (auto i = 0u; i < resultChunk->size(); i++) {
        auto tableName = tableNamesVector.getValue<common::ku_string_t>(i).getAsString();
        createForeignTable(con, tableName, dbPath, catalogName);
    }
}

static std::string getQuery(const binder::BoundCreateTableInfo& info) {
    auto extraInfo = common::ku_dynamic_cast<binder::BoundExtraCreateCatalogEntryInfo*,
        BoundExtraCreateDuckDBTableInfo*>(info.extraInfo.get());
    return common::stringFormat("SELECT * FROM {}.{}.{}", extraInfo->catalogName,
        extraInfo->schemaName, info.tableName);
}

void DuckDBCatalogContent::createForeignTable(duckdb::Connection& con, const std::string& tableName,
    const std::string& dbPath, const std::string& catalogName) {
    auto tableID = assignNextTableID();
    auto info = bindCreateTableInfo(con, tableName, dbPath, catalogName);
    if (info == nullptr) {
        return;
    }
    auto extraInfo = common::ku_dynamic_cast<binder::BoundExtraCreateCatalogEntryInfo*,
        BoundExtraCreateDuckDBTableInfo*>(info->extraInfo.get());
    std::vector<common::LogicalType> columnTypes;
    std::vector<std::string> columnNames;
    for (auto& propertyInfo : extraInfo->propertyInfos) {
        columnNames.push_back(propertyInfo.name);
        columnTypes.push_back(propertyInfo.type);
    }
    DuckDBScanBindData bindData(getQuery(*info), std::move(columnTypes), std::move(columnNames),
        std::bind(&DuckDBCatalogContent::getConnection, this, dbPath));
    auto tableEntry = std::make_unique<catalog::DuckDBTableCatalogEntry>(info->tableName, tableID,
        getScanFunction(std::move(bindData)));
    for (auto& propertyInfo : extraInfo->propertyInfos) {
        tableEntry->addProperty(propertyInfo.name, propertyInfo.type.copy());
    }
    tables->createEntry(std::move(tableEntry));
}

static bool getTableInfo(duckdb::Connection& con, const std::string& tableName,
    const std::string& schemaName, const std::string& catalogName,
    std::vector<common::LogicalType>& columnTypes, std::vector<std::string>& columnNames) {
    auto query =
        common::stringFormat("select data_type,column_name from information_schema.columns where "
                             "table_name = '{}' and table_schema = '{}' and table_catalog = '{}';",
            tableName, schemaName, catalogName);
    auto result = con.Query(query);
    if (result->RowCount() == 0) {
        return false;
    }
    columnTypes.reserve(result->RowCount());
    columnNames.reserve(result->RowCount());
    for (auto i = 0u; i < result->RowCount(); i++) {
        try {
            columnTypes.push_back(DuckDBTypeConverter::convertDuckDBType(
                result->GetValue(0, i).GetValue<std::string>()));
        } catch (common::BinderException& e) {
            return false;
        }
        columnNames.push_back(result->GetValue(1, i).GetValue<std::string>());
    }
    return true;
}

bool DuckDBCatalogContent::bindPropertyInfos(duckdb::Connection& con, const std::string& tableName,
    const std::string& catalogName, std::vector<binder::PropertyInfo>& propertyInfos) {
    std::vector<common::LogicalType> columnTypes;
    std::vector<std::string> columnNames;
    if (!getTableInfo(con, tableName, getDefaultSchemaName(), catalogName, columnTypes,
            columnNames)) {
        return false;
    }
    for (auto i = 0u; i < columnNames.size(); i++) {
        auto propertyInfo = binder::PropertyInfo(columnNames[i], columnTypes[i]);
        propertyInfos.push_back(std::move(propertyInfo));
    }
    return true;
}

std::unique_ptr<binder::BoundCreateTableInfo> DuckDBCatalogContent::bindCreateTableInfo(
    duckdb::Connection& con, const std::string& tableName, const std::string& dbPath,
    const std::string& catalogName) {
    std::vector<binder::PropertyInfo> propertyInfos;
    if (!bindPropertyInfos(con, tableName, catalogName, propertyInfos)) {
        return nullptr;
    }
    return std::make_unique<binder::BoundCreateTableInfo>(common::TableType::FOREIGN, tableName,
        std::make_unique<duckdb_scanner::BoundExtraCreateDuckDBTableInfo>(dbPath, catalogName,
            getDefaultSchemaName(), std::move(propertyInfos)));
}

std::string DuckDBCatalogContent::getDefaultSchemaName() const {
    return "main";
}

std::pair<duckdb::DuckDB, duckdb::Connection> DuckDBCatalogContent::getConnection(
    const std::string& dbPath) const {
    duckdb::DuckDB db(dbPath);
    duckdb::Connection con(db);
    return std::make_pair(std::move(db), std::move(con));
}

} // namespace duckdb_scanner
} // namespace kuzu

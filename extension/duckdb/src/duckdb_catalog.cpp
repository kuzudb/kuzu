#include "duckdb_catalog.h"

#include "binder/bound_attach_info.h"
#include "common/exception/binder.h"
#include "common/exception/runtime.h"
#include "duckdb_scan.h"
#include "duckdb_storage.h"
#include "duckdb_table_catalog_entry.h"
#include "duckdb_type_converter.h"

namespace kuzu {
namespace duckdb_extension {

DuckDBCatalog::DuckDBCatalog(std::string dbPath, std::string catalogName,
    main::ClientContext* context, const binder::AttachOption& attachOption)
    : CatalogExtension{}, dbPath{std::move(dbPath)}, catalogName{std::move(catalogName)},
      tableNamesVector{common::LogicalType::STRING(), context->getMemoryManager()} {
    skipUnsupportedTable = DuckDBStorageExtension::SKIP_UNSUPPORTED_TABLE_DEFAULT_VAL;
    auto& options = attachOption.options;
    if (options.contains(DuckDBStorageExtension::SKIP_UNSUPPORTED_TABLE_KEY)) {
        auto val = options.at(DuckDBStorageExtension::SKIP_UNSUPPORTED_TABLE_KEY);
        if (val.getDataType().getLogicalTypeID() != common::LogicalTypeID::BOOL) {
            throw common::RuntimeException{common::stringFormat("Invalid option value for {}",
                DuckDBStorageExtension::SKIP_UNSUPPORTED_TABLE_KEY)};
        }
        skipUnsupportedTable = val.getValue<bool>();
    }
}

void DuckDBCatalog::init() {
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
    if (resultChunk == nullptr || resultChunk->size() == 0) {
        return;
    }
    duckdb_extension::duckdb_conversion_func_t conversionFunc;
    duckdb_extension::getDuckDBVectorConversionFunc(common::PhysicalTypeID::STRING, conversionFunc);
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

// TODO(Ziyi): Do u need to pass in `dbPath` and `catalogName` here? Why not use the private field?
void DuckDBCatalog::createForeignTable(duckdb::Connection& con, const std::string& tableName,
    const std::string& dbPath, const std::string& catalogName) {
    auto tableID = tables->assignNextOID();
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
        columnTypes.push_back(propertyInfo.type.copy());
    }
    DuckDBScanBindData bindData(getQuery(*info), std::move(columnTypes), std::move(columnNames),
        std::bind(&DuckDBCatalog::getConnection, this, dbPath));
    auto tableEntry = std::make_unique<catalog::DuckDBTableCatalogEntry>(tables.get(),
        info->tableName, tableID, getScanFunction(std::move(bindData)));
    for (auto& propertyInfo : extraInfo->propertyInfos) {
        tableEntry->addProperty(propertyInfo.name, propertyInfo.type.copy(),
            propertyInfo.defaultValue->copy());
    }
    tables->createEntry(&transaction::DUMMY_WRITE_TRANSACTION, std::move(tableEntry));
}

static bool getTableInfo(duckdb::Connection& con, const std::string& tableName,
    const std::string& schemaName, const std::string& catalogName,
    std::vector<common::LogicalType>& columnTypes, std::vector<std::string>& columnNames,
    bool skipUnsupportedTable) {
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
            if (skipUnsupportedTable) {
                return false;
            }
            throw;
        }
        columnNames.push_back(result->GetValue(1, i).GetValue<std::string>());
    }
    return true;
}

// TODO(Ziyi): Do u need to pass in `catalogName` here? Why not use the private field?
bool DuckDBCatalog::bindPropertyInfos(duckdb::Connection& con, const std::string& tableName,
    const std::string& catalogName, std::vector<binder::PropertyInfo>& propertyInfos) {
    std::vector<common::LogicalType> columnTypes;
    std::vector<std::string> columnNames;
    if (!getTableInfo(con, tableName, getDefaultSchemaName(), catalogName, columnTypes, columnNames,
            skipUnsupportedTable)) {
        return false;
    }
    for (auto i = 0u; i < columnNames.size(); i++) {
        auto propertyInfo = binder::PropertyInfo(columnNames[i], columnTypes[i].copy());
        propertyInfos.push_back(std::move(propertyInfo));
    }
    return true;
}

// TODO(Ziyi): Do u need to pass in `dbPath` and `catalogName` here? Why not use the private field?
std::unique_ptr<binder::BoundCreateTableInfo> DuckDBCatalog::bindCreateTableInfo(
    duckdb::Connection& con, const std::string& tableName, const std::string& dbPath,
    const std::string& catalogName) {
    std::vector<binder::PropertyInfo> propertyInfos;
    if (!bindPropertyInfos(con, tableName, catalogName, propertyInfos)) {
        return nullptr;
    }
    return std::make_unique<binder::BoundCreateTableInfo>(common::TableType::EXTERNAL, tableName,
        common::ConflictAction::ON_CONFLICT_THROW,
        std::make_unique<duckdb_extension::BoundExtraCreateDuckDBTableInfo>(dbPath, catalogName,
            getDefaultSchemaName(), std::move(propertyInfos)));
}

std::string DuckDBCatalog::getDefaultSchemaName() const {
    return "main";
}

// TODO(Ziyi): Do u need to pass in `dbPath` here? Why not use the private field?
std::pair<duckdb::DuckDB, duckdb::Connection> DuckDBCatalog::getConnection(
    const std::string& dbPath) const {
    try {
        duckdb::DuckDB db(dbPath);
        duckdb::Connection con(db);
        return std::make_pair(std::move(db), std::move(con));
    } catch (std::exception& e) {
        throw common::RuntimeException{e.what()};
    }
}

} // namespace duckdb_extension
} // namespace kuzu

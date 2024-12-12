#include "binder/bound_export_database.h"
#include "binder/query/bound_regular_query.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/file_system/virtual_file_system.h"
#include "common/string_utils.h"
#include "main/client_context.h"
#include "parser/parser.h"
#include "parser/port_db.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;
using namespace kuzu::transaction;
using namespace kuzu::storage;

namespace kuzu {
namespace binder {

static std::string getPrimaryKeyName(table_id_t tableId, const Catalog& catalog, Transaction* tx) {
    auto tableEntry = catalog.getTableCatalogEntry(tx, tableId);
    return tableEntry->constCast<NodeTableCatalogEntry>().getPrimaryKeyName();
}

static std::vector<ExportedTableData> getExportInfo(const Catalog& catalog, Transaction* tx,
    Binder* binder) {
    std::vector<ExportedTableData> exportData;
    for (auto tableEntry : catalog.getTableEntries(tx)) {
        ExportedTableData tableData;
        if (binder->bindExportTableData(tableData, *tableEntry, catalog, tx)) {
            exportData.push_back(std::move(tableData));
        }
    }
    return exportData;
}

FileTypeInfo getFileType(case_insensitive_map_t<common::Value>& options) {
    auto fileTypeInfo = FileTypeInfo{FileType::CSV, "CSV"};
    if (options.find("FORMAT") != options.end()) {
        auto value = options.at("FORMAT");
        if (value.getDataType().getLogicalTypeID() != LogicalTypeID::STRING) {
            throw BinderException("The type of format option must be a string.");
        }
        auto valueStr = value.getValue<std::string>();
        StringUtils::toUpper(valueStr);
        fileTypeInfo = FileTypeInfo{FileTypeUtils::fromString(valueStr), valueStr};
        options.erase("FORMAT");
    }
    return fileTypeInfo;
}

static void bindExportNodeTableDataQuery(const TableCatalogEntry& entry, std::string& exportQuery) {
    exportQuery = stringFormat("match (a:{}) return a.*", entry.getName());
}

static void bindExportRelTableDataQuery(const TableCatalogEntry& entry, std::string& exportQuery,
    const catalog::Catalog& catalog, transaction::Transaction* tx) {
    auto relTableEntry = entry.constPtrCast<RelTableCatalogEntry>();
    auto srcPrimaryKeyName = getPrimaryKeyName(relTableEntry->getSrcTableID(), catalog, tx);
    auto dstPrimaryKeyName = getPrimaryKeyName(relTableEntry->getDstTableID(), catalog, tx);
    auto srcName = catalog.getTableName(tx, relTableEntry->getSrcTableID());
    auto dstName = catalog.getTableName(tx, relTableEntry->getDstTableID());
    auto relName = relTableEntry->getName();
    exportQuery = stringFormat("match (a:{})-[r:{}]->(b:{}) return a.{},b.{},r.*;", srcName,
        relName, dstName, srcPrimaryKeyName, dstPrimaryKeyName);
}

static bool bindExportQuery(std::string& exportQuery, const TableCatalogEntry& entry,
    const Catalog& catalog, transaction::Transaction* tx) {
    switch (entry.getTableType()) {
    case common::TableType::NODE: {
        bindExportNodeTableDataQuery(entry, exportQuery);
    } break;
    case common::TableType::REL: {
        bindExportRelTableDataQuery(entry, exportQuery, catalog, tx);
    } break;
    default:
        return false;
    }
    return true;
}

bool Binder::bindExportTableData(ExportedTableData& tableData, const TableCatalogEntry& entry,
    const Catalog& catalog, transaction::Transaction* tx) {
    std::string exportQuery;
    tableData.tableName = entry.getName();
    if (!bindExportQuery(exportQuery, entry, catalog, tx)) {
        return false;
    }
    auto parsedStatement = Parser::parseQuery(exportQuery, clientContext);
    KU_ASSERT(parsedStatement.size() == 1);
    auto parsedQuery = parsedStatement[0]->constPtrCast<RegularQuery>();
    auto query = bindQuery(*parsedQuery);
    auto columns = query->getStatementResult()->getColumns();
    for (auto& column : columns) {
        auto columnName = column->hasAlias() ? column->getAlias() : column->toString();
        tableData.columnNames.push_back(columnName);
        tableData.columnTypes.push_back(column->getDataType().copy());
    }
    tableData.regularQuery = std::move(query);
    return true;
}

std::unique_ptr<BoundStatement> Binder::bindExportDatabaseClause(const Statement& statement) {
    auto& exportDB = statement.constCast<ExportDB>();
    auto boundFilePath =
        clientContext->getVFSUnsafe()->expandPath(clientContext, exportDB.getFilePath());
    auto exportData = getExportInfo(*clientContext->getCatalog(), clientContext->getTx(), this);
    auto parsedOptions = bindParsingOptions(exportDB.getParsingOptionsRef());
    auto fileTypeInfo = getFileType(parsedOptions);
    switch (fileTypeInfo.fileType) {
    case FileType::CSV:
    case FileType::PARQUET:
        break;
    default:
        throw BinderException("Export database currently only supports csv and parquet files.");
    }
    if (fileTypeInfo.fileType != FileType::CSV && parsedOptions.size() != 0) {
        throw BinderException{"Only export to csv can have options."};
    }
    return std::make_unique<BoundExportDatabase>(boundFilePath, fileTypeInfo, std::move(exportData),
        std::move(parsedOptions));
}

} // namespace binder
} // namespace kuzu

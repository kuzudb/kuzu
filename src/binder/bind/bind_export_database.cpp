#include "binder/bound_export_database.h"
#include "binder/query/bound_regular_query.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/index_catalog_entry.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/exception/binder.h"
#include "common/file_system/virtual_file_system.h"
#include "common/string_utils.h"
#include "main/client_context.h"
#include "parser/parser.h"
#include "parser/port_db.h"
#include "parser/query/regular_query.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;
using namespace kuzu::transaction;
using namespace kuzu::storage;

namespace kuzu {
namespace binder {

static std::vector<ExportedTableData> getExportInfo(const Catalog& catalog,
    main::ClientContext* context, Binder* binder) {
    auto transaction = context->getTransaction();
    std::vector<ExportedTableData> exportData;
    for (auto tableEntry : catalog.getTableEntries(transaction, false /*useInternal*/)) {
        ExportedTableData tableData;
        if (binder->bindExportTableData(tableData, *tableEntry, catalog, transaction)) {
            exportData.push_back(std::move(tableData));
        }
    }
    for (auto indexEntry : catalog.getIndexEntries(transaction)) {
        ExportedTableData tableData;
        auto tableToExport = indexEntry->getTableEntryToExport(context);
        if (tableToExport == nullptr) {
            continue;
        }
        binder->bindExportTableData(tableData, *tableToExport, catalog, transaction);
        exportData.push_back(std::move(tableData));
    }
    return exportData;
}

FileTypeInfo getFileType(case_insensitive_map_t<Value>& options) {
    auto fileTypeInfo = FileTypeInfo{FileType::PARQUET, "PARQUET"};
    if (options.contains("FORMAT")) {
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
    exportQuery = stringFormat("match (a:`{}`) return a.*", entry.getName());
}

static void bindExportRelTableDataQuery(const TableCatalogEntry& entry, std::string& exportQuery,
    const Catalog& catalog, const Transaction* transaction) {
    auto relTableEntry = entry.constPtrCast<RelTableCatalogEntry>();
    auto& srcTableEntry = catalog.getTableCatalogEntry(transaction, relTableEntry->getSrcTableID())
                              ->constCast<NodeTableCatalogEntry>();
    auto& dstTableEntry = catalog.getTableCatalogEntry(transaction, relTableEntry->getDstTableID())
                              ->constCast<NodeTableCatalogEntry>();
    exportQuery = stringFormat("match (a:`{}`)-[r:`{}`]->(b:`{}`) return a.{},b.{},r.*;",
        srcTableEntry.getName(), relTableEntry->getName(), dstTableEntry.getName(),
        srcTableEntry.getPrimaryKeyName(), dstTableEntry.getPrimaryKeyName());
}

static bool bindExportQuery(std::string& exportQuery, const TableCatalogEntry& entry,
    const Catalog& catalog, const Transaction* transaction) {
    switch (entry.getTableType()) {
    case TableType::NODE: {
        bindExportNodeTableDataQuery(entry, exportQuery);
    } break;
    case TableType::REL: {
        bindExportRelTableDataQuery(entry, exportQuery, catalog, transaction);
    } break;
    default:
        return false;
    }
    return true;
}

bool Binder::bindExportTableData(ExportedTableData& tableData, const TableCatalogEntry& entry,
    const Catalog& catalog, const Transaction* transaction) {
    std::string exportQuery;
    tableData.tableName = entry.getName();
    if (!bindExportQuery(exportQuery, entry, catalog, transaction)) {
        return false;
    }
    auto parsedStatement = Parser::parseQuery(exportQuery);
    KU_ASSERT(parsedStatement.size() == 1);
    auto parsedQuery = parsedStatement[0]->constPtrCast<RegularQuery>();
    clientContext->setUseInternalCatalogEntry(true /* useInternalCatalogEntry */);
    auto query = bindQuery(*parsedQuery);
    clientContext->setUseInternalCatalogEntry(false /* useInternalCatalogEntry */);
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
    auto exportData = getExportInfo(*clientContext->getCatalog(), clientContext, this);
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

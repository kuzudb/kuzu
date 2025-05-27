#include "binder/bound_export_database.h"
#include "binder/query/bound_regular_query.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/index_catalog_entry.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
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

void bindExportTableData(ExportedTableData& tableData, const std::string& query,
    main::ClientContext* context, Binder* binder) {
    auto parsedStatement = Parser::parseQuery(query);
    KU_ASSERT(parsedStatement.size() == 1);
    auto parsedQuery = parsedStatement[0]->constPtrCast<RegularQuery>();
    context->setUseInternalCatalogEntry(true /* useInternalCatalogEntry */);
    auto boundQuery = binder->bindQuery(*parsedQuery);
    context->setUseInternalCatalogEntry(false /* useInternalCatalogEntry */);
    auto columns = boundQuery->getStatementResult()->getColumns();
    for (auto& column : columns) {
        auto columnName = column->hasAlias() ? column->getAlias() : column->toString();
        tableData.columnNames.push_back(columnName);
        tableData.columnTypes.push_back(column->getDataType().copy());
    }
    tableData.regularQuery = std::move(boundQuery);
}

static std::string getExportNodeTableDataQuery(const TableCatalogEntry& entry) {
    return stringFormat("match (a:`{}`) return a.*", entry.getName());
}

static std::string getExportRelTableDataQuery(const TableCatalogEntry& relGroupEntry,
    const NodeTableCatalogEntry& srcEntry, const NodeTableCatalogEntry& dstEntry) {
    return stringFormat("match (a:`{}`)-[r:`{}`]->(b:`{}`) return a.{},b.{},r.*;",
        srcEntry.getName(), relGroupEntry.getName(), dstEntry.getName(),
        srcEntry.getPrimaryKeyName(), dstEntry.getPrimaryKeyName());
}

static std::vector<ExportedTableData> getExportInfo(const Catalog& catalog,
    main::ClientContext* context, Binder* binder, FileTypeInfo& fileTypeInfo) {
    auto transaction = context->getTransaction();
    std::vector<ExportedTableData> exportData;
    for (auto entry : catalog.getNodeTableEntries(transaction, false /*useInternal*/)) {
        ExportedTableData tableData;
        tableData.tableName = entry->getName();
        tableData.fileName =
            entry->getName() + "." + StringUtils::getLower(fileTypeInfo.fileTypeStr);
        auto query = getExportNodeTableDataQuery(*entry);
        bindExportTableData(tableData, query, context, binder);
        exportData.push_back(std::move(tableData));
    }
    for (auto entry : catalog.getRelGroupEntries(transaction, false /* useInternal */)) {
        auto& relGroupEntry = entry->constCast<RelGroupCatalogEntry>();
        for (auto& info : relGroupEntry.getRelEntryInfos()) {
            ExportedTableData tableData;
            auto srcTableID = info.nodePair.srcTableID;
            auto dstTableID = info.nodePair.dstTableID;
            auto& srcEntry = catalog.getTableCatalogEntry(transaction, srcTableID)
                                 ->constCast<NodeTableCatalogEntry>();
            auto& dstEntry = catalog.getTableCatalogEntry(transaction, dstTableID)
                                 ->constCast<NodeTableCatalogEntry>();
            tableData.tableName = entry->getName();
            tableData.fileName =
                stringFormat("{}_{}_{}.{}", relGroupEntry.getName(), srcEntry.getName(),
                    dstEntry.getName(), StringUtils::getLower(fileTypeInfo.fileTypeStr));
            auto query = getExportRelTableDataQuery(relGroupEntry, srcEntry, dstEntry);
            bindExportTableData(tableData, query, context, binder);
            exportData.push_back(std::move(tableData));
        }
    }

    for (auto indexEntry : catalog.getIndexEntries(transaction)) {
        // Export
        ExportedTableData tableData;
        auto entry = indexEntry->getTableEntryToExport(context);
        if (entry == nullptr) {
            continue;
        }
        KU_ASSERT(entry->getTableType() == TableType::NODE);
        tableData.tableName = entry->getName();
        tableData.fileName =
            entry->getName() + "." + StringUtils::getLower(fileTypeInfo.fileTypeStr);
        auto query = getExportNodeTableDataQuery(*entry);
        bindExportTableData(tableData, query, context, binder);
        exportData.push_back(std::move(tableData));
    }
    return exportData;
}

std::unique_ptr<BoundStatement> Binder::bindExportDatabaseClause(const Statement& statement) {
    auto& exportDB = statement.constCast<ExportDB>();
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
    auto exportData =
        getExportInfo(*clientContext->getCatalog(), clientContext, this, fileTypeInfo);
    auto boundFilePath =
        clientContext->getVFSUnsafe()->expandPath(clientContext, exportDB.getFilePath());
    return std::make_unique<BoundExportDatabase>(boundFilePath, fileTypeInfo, std::move(exportData),
        std::move(parsedOptions));
}

} // namespace binder
} // namespace kuzu

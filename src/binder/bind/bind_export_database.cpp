#include "binder/copy/bound_export_database.h"
#include "binder/query/bound_regular_query.h"
#include "common/exception/binder.h"
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
    auto primaryProperty =
        ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(tableEntry)->getPrimaryKey();
    return primaryProperty->getName();
}

static std::vector<ExportedTableData> getExportInfo(
    const Catalog& catalog, Transaction* tx, Binder* binder) {
    std::vector<ExportedTableData> exportData;
    for (auto& nodeTableEntry : catalog.getNodeTableEntries(tx)) {
        auto tableName = nodeTableEntry->getName();
        std::string selQuery = "match (a:" + tableName + ") return a.*";
        exportData.push_back(binder->extractExportData(selQuery, tableName));
    }
    for (auto& relTableEntry : catalog.getRelTableEntries(tx)) {
        auto srcPrimaryKeyName = getPrimaryKeyName(relTableEntry->getSrcTableID(), catalog, tx);
        auto dstPrimaryKeyName = getPrimaryKeyName(relTableEntry->getDstTableID(), catalog, tx);
        auto srcName = catalog.getTableName(tx, relTableEntry->getSrcTableID());
        auto dstName = catalog.getTableName(tx, relTableEntry->getDstTableID());
        auto relName = relTableEntry->getName();
        std::string selQuery = "match (a:" + srcName + ")-[r:" + relName + "]->(b:" + dstName +
                               ") return a." + srcPrimaryKeyName + ",b." + dstPrimaryKeyName +
                               ",r.*";
        exportData.push_back(binder->extractExportData(selQuery, relName));
    }
    return exportData;
}

FileType getFileType(std::unordered_map<std::string, common::Value>& options) {
    auto fileType = FileType::CSV;
    if (options.find("FORMAT") != options.end()) {
        auto value = options.at("FORMAT");
        if (value.getDataType()->getLogicalTypeID() != LogicalTypeID::STRING) {
            throw BinderException("The type of format option must be a string.");
        }
        auto valueStr = value.getValue<std::string>();
        StringUtils::toUpper(valueStr);
        fileType = FileTypeUtils::fromString(valueStr);
        options.erase("FORMAT");
    }
    return fileType;
}

ExportedTableData Binder::extractExportData(std::string selQuery, std::string tableName) {
    auto parsedStatement = Parser::parseQuery(selQuery);
    ExportedTableData exportedTableData;
    exportedTableData.tableName = tableName;
    KU_ASSERT(parsedStatement.size() == 1);
    auto parsedQuery =
        ku_dynamic_cast<const Statement*, const RegularQuery*>(parsedStatement[0].get());
    auto query = bindQuery(*parsedQuery);
    auto columns = query->getStatementResult()->getColumns();
    for (auto& column : columns) {
        auto columnName = column->hasAlias() ? column->getAlias() : column->toString();
        exportedTableData.columnNames.push_back(columnName);
        exportedTableData.columnTypes.push_back(column->getDataType());
    }
    exportedTableData.regularQuery = std::move(query);
    return exportedTableData;
}

std::unique_ptr<BoundStatement> Binder::bindExportDatabaseClause(const Statement& statement) {
    auto& exportDatabaseStatement = ku_dynamic_cast<const Statement&, const ExportDB&>(statement);
    auto boundFilePath = exportDatabaseStatement.getFilePath();
    auto exportData = getExportInfo(catalog, clientContext->getTx(), this);
    auto parsedOptions = bindParsingOptions(exportDatabaseStatement.getParsingOptionsRef());
    auto fileType = getFileType(parsedOptions);
    if (fileType != FileType::CSV && fileType != FileType::PARQUET) {
        throw BinderException("Export database currently only supports csv and parquet files.");
    }
    if (fileType != FileType::CSV && parsedOptions.size() != 0) {
        throw BinderException{"Only export to csv can have options."};
    }
    // try to create the directory, if it doesn't exist yet
    if (!vfs->fileOrPathExists(boundFilePath)) {
        vfs->createDir(boundFilePath);
    } else {
        throw BinderException(stringFormat("Directory {} already exists.", boundFilePath));
    }
    return std::make_unique<BoundExportDatabase>(
        boundFilePath, fileType, std::move(exportData), std::move(parsedOptions));
}
} // namespace binder
} // namespace kuzu

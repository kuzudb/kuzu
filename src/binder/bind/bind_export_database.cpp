#include "binder/copy/bound_export_database.h"
#include "binder/query/bound_regular_query.h"
#include "common/exception/binder.h"
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
    auto schema = catalog.getTableSchema(tx, tableId);
    auto primaryProperty = ku_dynamic_cast<TableSchema*, NodeTableSchema*>(schema)->getPrimaryKey();
    return primaryProperty->getName();
}

static std::vector<ExportedTableData> getExportInfo(
    const Catalog& catalog, Transaction* tx, Binder* binder) {
    std::vector<ExportedTableData> exportData;
    for (auto& schema : catalog.getNodeTableSchemas(tx)) {
        auto tableName = schema->getName();
        std::string selQuery = "match (a:" + tableName + ") return a.*";
        exportData.push_back(binder->extractExportData(selQuery, tableName));
    }
    for (auto& schema : catalog.getRelTableSchemas(tx)) {
        auto relSchema = ku_dynamic_cast<TableSchema*, RelTableSchema*>(schema);
        auto srcPrimaryKeyName = getPrimaryKeyName(relSchema->getSrcTableID(), catalog, tx);
        auto dstPrimaryKeyName = getPrimaryKeyName(relSchema->getDstTableID(), catalog, tx);
        auto srcName = catalog.getTableName(tx, relSchema->getSrcTableID());
        auto dstName = catalog.getTableName(tx, relSchema->getDstTableID());
        auto relName = schema->getName();
        std::string selQuery = "match (a:" + srcName + ")-[r:" + relName + "]->(b:" + dstName +
                               ") return a." + srcPrimaryKeyName + ",b." + dstPrimaryKeyName +
                               ",r.*";
        exportData.push_back(binder->extractExportData(selQuery, relName));
    }
    return exportData;
}

ExportedTableData Binder::extractExportData(std::string selQuery, std::string tableName) {
    auto parsedStatement = Parser::parseQuery(selQuery);
    ExportedTableData exportedTableData;
    exportedTableData.tableName = tableName;
    auto parsedQuery =
        ku_dynamic_cast<const Statement*, const RegularQuery*>(parsedStatement.get());
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
    // TODO(Jiamin): add more supported file types
    auto fileType = FileType::CSV;
    auto csvConfig = CSVReaderConfig::construct(
        bindParsingOptions(exportDatabaseStatement.getParsingOptionsRef()));
    // try to create the directory, if it doesn't exist yet
    if (!vfs->fileOrPathExists(boundFilePath)) {
        vfs->createDir(boundFilePath);
    } else {
        throw BinderException(stringFormat("Directory {} already exists.", boundFilePath));
    }
    return std::make_unique<BoundExportDatabase>(
        boundFilePath, fileType, std::move(exportData), csvConfig.option.copy());
}
} // namespace binder
} // namespace kuzu

#include "binder/copy/bound_export_database.h"
#include "binder/query/bound_regular_query.h"
#include "common/exception/binder.h"
#include "parser/db.h"
#include "parser/parser.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::catalog;
using namespace kuzu::transaction;
using namespace kuzu::storage;

namespace kuzu {
namespace binder {

std::string getPrimaryKeyName(table_id_t tableId, const Catalog& catalog) {
    auto schema = catalog.getTableSchema(&DUMMY_READ_TRANSACTION, tableId);
    auto primaryProperty = ku_dynamic_cast<TableSchema*, NodeTableSchema*>(schema)->getPrimaryKey();
    return primaryProperty->getName();
}

void Binder::extractExportData(
    std::string selQuery, std::string tableName, std::vector<ExportedTableData>& exportData) {
    std::unique_ptr<Statement> parsedStatement = Parser::parseQuery(selQuery);
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
    exportData.push_back(std::move(exportedTableData));
}

void Binder::getExportInfo(const Catalog& catalog, std::vector<ExportedTableData>& exportData) {
    for (auto& schema : catalog.getNodeTableSchemas(&DUMMY_READ_TRANSACTION)) {
        auto tableName = schema->getName();
        std::string selQuery = "match (a:" + tableName + ") return a.*";
        extractExportData(selQuery, tableName, exportData);
    }
    for (auto& schema : catalog.getRelTableSchemas(&DUMMY_READ_TRANSACTION)) {
        auto relSchema = ku_dynamic_cast<TableSchema*, RelTableSchema*>(schema);
        auto srcPrimaryKeyName = getPrimaryKeyName(relSchema->getSrcTableID(), catalog);
        auto dstPrimaryKeyName = getPrimaryKeyName(relSchema->getDstTableID(), catalog);
        auto srcName = catalog.getTableName(&DUMMY_READ_TRANSACTION, relSchema->getSrcTableID());
        auto dstName = catalog.getTableName(&DUMMY_READ_TRANSACTION, relSchema->getDstTableID());
        auto relName = schema->getName();
        std::string selQuery = "match (a:" + srcName + ")-[r:" + relName + "]->(b:" + dstName +
                               ") return a." + srcPrimaryKeyName + ",b." + dstPrimaryKeyName +
                               ",r.*";
        extractExportData(selQuery, relName, exportData);
    }
}

std::unique_ptr<BoundStatement> Binder::bindExportDatabaseClause(const Statement& statement) {
    auto& exportDatabaseStatement = ku_dynamic_cast<const Statement&, const ExportDB&>(statement);
    auto boundFilePath = exportDatabaseStatement.getFilePath();
    std::vector<ExportedTableData> exportData;
    getExportInfo(catalog, exportData);
    // TODO(Jiamin): add more supported file types
    auto fileType = exportDatabaseStatement.getFileType();
    if (fileType != FileType::CSV && fileType != FileType::PARQUET) {
        throw BinderException("Export database currently only supports csv and parquet files.");
    }
    if (fileType != FileType::CSV && exportDatabaseStatement.getParsingOptionsRef().size() != 0) {
        throw BinderException{"Only export to csv can have options."};
    }
    auto csvConfig = CSVReaderConfig::construct(
        bindParsingOptions(exportDatabaseStatement.getParsingOptionsRef()));
    // try to create the directory, if it doesn't exist yet
    if (!vfs->fileOrPathExists(boundFilePath)) {
        vfs->createDir(boundFilePath);
    } else {
        throw BinderException("incorrect directory!");
    }
    return std::make_unique<BoundExportDatabase>(
        boundFilePath, fileType, std::move(exportData), csvConfig.option.copy());
}
} // namespace binder
} // namespace kuzu

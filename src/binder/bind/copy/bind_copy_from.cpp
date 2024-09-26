#include "binder/binder.h"
#include "binder/copy/bound_copy_from.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/enums/table_type.h"
#include "common/exception/binder.h"
#include "common/string_format.h"
#include "main/client_context.h"
#include "parser/copy.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::parser;
using namespace kuzu::function;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCopyFromClause(const Statement& statement) {
    auto& copyStatement = ku_dynamic_cast<const CopyFrom&>(statement);
    auto tableName = copyStatement.getTableName();
    validateTableExist(tableName);
    // Bind to table schema.
    auto catalog = clientContext->getCatalog();
    auto tableID = catalog->getTableID(clientContext->getTx(), tableName);
    auto tableEntry = catalog->getTableCatalogEntry(clientContext->getTx(), tableID);
    switch (tableEntry->getTableType()) {
    case TableType::REL_GROUP: {
        throw BinderException(stringFormat("Cannot copy into {} table with type {}.", tableName,
            TableTypeUtils::toString(tableEntry->getTableType())));
    }
    default:
        break;
    }
    switch (tableEntry->getTableType()) {
    case TableType::NODE: {
        auto nodeTableEntry = tableEntry->ptrCast<NodeTableCatalogEntry>();
        return bindCopyNodeFrom(statement, nodeTableEntry);
    }
    case TableType::REL: {
        auto relTableEntry = tableEntry->ptrCast<RelTableCatalogEntry>();
        return bindCopyRelFrom(statement, relTableEntry);
    }
    case TableType::RDF: {
        auto rdfGraphEntry = tableEntry->ptrCast<RDFGraphCatalogEntry>();
        return bindCopyRdfFrom(statement, rdfGraphEntry);
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

static void bindExpectedNodeColumns(NodeTableCatalogEntry* nodeTableEntry,
    const std::vector<std::string>& inputColumnNames, std::vector<std::string>& columnNames,
    std::vector<LogicalType>& columnTypes);
static void bindExpectedRelColumns(RelTableCatalogEntry* relTableEntry,
    const std::vector<std::string>& inputColumnNames, std::vector<std::string>& columnNames,
    std::vector<LogicalType>& columnTypes, main::ClientContext* context);

static std::pair<ColumnEvaluateType, std::shared_ptr<Expression>> matchColumnExpression(
    const expression_vector& columns, const PropertyDefinition& property,
    ExpressionBinder& expressionBinder) {
    for (auto& column : columns) {
        if (property.getName() == column->toString()) {
            if (column->dataType == property.getType()) {
                return {ColumnEvaluateType::REFERENCE, column};
            } else {
                return {ColumnEvaluateType::CAST,
                    expressionBinder.forceCast(column, property.getType())};
            }
        }
    }
    return {ColumnEvaluateType::DEFAULT, expressionBinder.bindExpression(*property.defaultExpr)};
}

std::unique_ptr<BoundStatement> Binder::bindCopyNodeFrom(const Statement& statement,
    NodeTableCatalogEntry* nodeTableEntry) {
    auto& copyStatement = ku_dynamic_cast<const CopyFrom&>(statement);
    // Bind expected columns based on catalog information.
    std::vector<std::string> expectedColumnNames;
    std::vector<LogicalType> expectedColumnTypes;
    bindExpectedNodeColumns(nodeTableEntry, copyStatement.getColumnNames(), expectedColumnNames,
        expectedColumnTypes);
    auto boundSource = bindScanSource(copyStatement.getSource(),
        copyStatement.getParsingOptionsRef(), expectedColumnNames, expectedColumnTypes);
    expression_vector warningDataExprs = boundSource->getWarningColumns();
    if (boundSource->type == ScanSourceType::FILE) {
        auto& source = boundSource->constCast<BoundTableScanSource>();
        auto bindData = source.info.bindData->constPtrCast<ScanBindData>();
        if (copyStatement.byColumn() && bindData->config.fileTypeInfo.fileType != FileType::NPY) {
            throw BinderException(stringFormat("Copy by column with {} file type is not supported.",
                bindData->config.fileTypeInfo.fileTypeStr));
        }
    }
    expression_vector columns;
    std::vector<ColumnEvaluateType> evaluateTypes;
    for (auto& property : nodeTableEntry->getProperties()) {
        auto [evaluateType, column] =
            matchColumnExpression(boundSource->getColumns(), property, expressionBinder);
        columns.push_back(column);
        evaluateTypes.push_back(evaluateType);
    }
    columns.insert(columns.end(), warningDataExprs.begin(), warningDataExprs.end());
    // TODO(Guodong): Should remove this expression.
    auto offset = expressionBinder.createVariableExpression(LogicalType::INT64(),
        std::string(InternalKeyword::ANONYMOUS));
    auto boundCopyFromInfo = BoundCopyFromInfo(nodeTableEntry, std::move(boundSource),
        std::move(offset), std::move(columns), std::move(evaluateTypes), nullptr /* extraInfo */);
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCopyRelFrom(const parser::Statement& statement,
    RelTableCatalogEntry* relTableEntry) {
    auto& copyStatement = statement.constCast<CopyFrom>();
    if (copyStatement.byColumn()) {
        throw BinderException(
            stringFormat("Copy by column is not supported for relationship table."));
    }
    // Bind expected columns based on catalog information.
    std::vector<std::string> expectedColumnNames;
    std::vector<LogicalType> expectedColumnTypes;
    bindExpectedRelColumns(relTableEntry, copyStatement.getColumnNames(), expectedColumnNames,
        expectedColumnTypes, clientContext);
    auto boundSource = bindScanSource(copyStatement.getSource(),
        copyStatement.getParsingOptionsRef(), expectedColumnNames, expectedColumnTypes);
    expression_vector warningDataExprs = boundSource->getWarningColumns();
    auto columns = boundSource->getColumns();
    auto offset = expressionBinder.createVariableExpression(LogicalType::INT64(),
        std::string(InternalKeyword::ROW_OFFSET));
    auto srcTableID = relTableEntry->getSrcTableID();
    auto dstTableID = relTableEntry->getDstTableID();
    auto srcKey = columns[0];
    auto dstKey = columns[1];
    auto srcOffset = createVariable(std::string(InternalKeyword::SRC_OFFSET), LogicalType::INT64());
    auto dstOffset = createVariable(std::string(InternalKeyword::DST_OFFSET), LogicalType::INT64());
    expression_vector columnExprs{srcOffset, dstOffset, offset};
    std::vector<ColumnEvaluateType> evaluateTypes{ColumnEvaluateType::REFERENCE,
        ColumnEvaluateType::REFERENCE, ColumnEvaluateType::REFERENCE};
    auto& properties = relTableEntry->getProperties();
    for (auto i = 1u; i < properties.size(); ++i) { // skip internal ID
        auto& property = properties[i];
        auto [evaluateType, column] =
            matchColumnExpression(boundSource->getColumns(), property, expressionBinder);
        columnExprs.push_back(column);
        evaluateTypes.push_back(evaluateType);
    }
    columnExprs.insert(columnExprs.end(), warningDataExprs.begin(), warningDataExprs.end());
    auto srcLookUpInfo = IndexLookupInfo(srcTableID, srcOffset, srcKey, warningDataExprs);
    auto dstLookUpInfo = IndexLookupInfo(dstTableID, dstOffset, dstKey, warningDataExprs);
    auto lookupInfos = std::vector<IndexLookupInfo>{srcLookUpInfo, dstLookUpInfo};
    auto internalIDColumnIndices = std::vector<common::idx_t>{0, 1, 2};
    auto extraCopyRelInfo =
        std::make_unique<ExtraBoundCopyRelInfo>(internalIDColumnIndices, lookupInfos);
    auto boundCopyFromInfo = BoundCopyFromInfo(relTableEntry, boundSource->copy(), offset,
        std::move(columnExprs), std::move(evaluateTypes), std::move(extraCopyRelInfo));
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

static bool skipPropertyInFile(const PropertyDefinition& property) {
    if (property.getName() == InternalKeyword::ID) {
        return true;
    }
    return false;
}

static bool skipPropertyInSchema(const PropertyDefinition& property) {
    if (property.getType().getLogicalTypeID() == LogicalTypeID::SERIAL) {
        return true;
    }
    if (property.getName() == InternalKeyword::ID) {
        return true;
    }
    return false;
}

static void bindExpectedColumns(TableCatalogEntry* tableEntry,
    const std::vector<std::string>& inputColumnNames, std::vector<std::string>& columnNames,
    std::vector<common::LogicalType>& columnTypes) {
    if (!inputColumnNames.empty()) {
        std::unordered_set<std::string> inputColumnNamesSet;
        for (auto& columName : inputColumnNames) {
            if (inputColumnNamesSet.contains(columName)) {
                throw BinderException(
                    stringFormat("Detect duplicate column name {} during COPY.", columName));
            }
            inputColumnNamesSet.insert(columName);
        }
        // Search column data type for each input column.
        for (auto& columnName : inputColumnNames) {
            if (!tableEntry->containsProperty(columnName)) {
                throw BinderException(stringFormat("Table {} does not contain column {}.",
                    tableEntry->getName(), columnName));
            }
            auto& property = tableEntry->getProperty(columnName);
            if (skipPropertyInFile(property)) {
                continue;
            }
            columnNames.push_back(columnName);
            columnTypes.push_back(property.getType().copy());
        }
    } else {
        // No column specified. Fall back to schema columns.
        for (auto& property : tableEntry->getProperties()) {
            if (skipPropertyInSchema(property)) {
                continue;
            }
            columnNames.push_back(property.getName());
            columnTypes.push_back(property.getType().copy());
        }
    }
}

void bindExpectedNodeColumns(NodeTableCatalogEntry* nodeTableEntry,
    const std::vector<std::string>& inputColumnNames, std::vector<std::string>& columnNames,
    std::vector<LogicalType>& columnTypes) {
    KU_ASSERT(columnNames.empty() && columnTypes.empty());
    bindExpectedColumns(nodeTableEntry, inputColumnNames, columnNames, columnTypes);
}

void bindExpectedRelColumns(RelTableCatalogEntry* relTableEntry,
    const std::vector<std::string>& inputColumnNames, std::vector<std::string>& columnNames,
    std::vector<LogicalType>& columnTypes, main::ClientContext* context) {
    KU_ASSERT(columnNames.empty() && columnTypes.empty());
    auto catalog = context->getCatalog();
    auto transaction = context->getTx();
    auto srcTable = catalog->getTableCatalogEntry(transaction, relTableEntry->getSrcTableID())
                        ->ptrCast<NodeTableCatalogEntry>();
    auto dstTable = catalog->getTableCatalogEntry(transaction, relTableEntry->getDstTableID())
                        ->ptrCast<NodeTableCatalogEntry>();
    columnNames.push_back("from");
    columnNames.push_back("to");
    auto srcPKColumnType = srcTable->getPrimaryKeyDefinition().getType().copy();
    if (srcPKColumnType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
        srcPKColumnType = LogicalType::INT64();
    }
    auto dstPKColumnType = dstTable->getPrimaryKeyDefinition().getType().copy();
    if (dstPKColumnType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
        dstPKColumnType = LogicalType::INT64();
    }
    columnTypes.push_back(std::move(srcPKColumnType));
    columnTypes.push_back(std::move(dstPKColumnType));
    bindExpectedColumns(relTableEntry, inputColumnNames, columnNames, columnTypes);
}

} // namespace binder
} // namespace kuzu

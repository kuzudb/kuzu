#include "binder/binder.h"
#include "binder/copy/bound_copy_from.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
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
    auto catalog = clientContext->getCatalog();
    auto transaction = clientContext->getTransaction();
    if (catalog->containsRelGroup(transaction, tableName)) {
        auto entry = catalog->getRelGroupEntry(transaction, tableName);
        if (entry->getNumRelTables() == 1) {
            auto tableEntry =
                catalog->getTableCatalogEntry(transaction, entry->getRelTableIDs()[0]);
            return bindCopyRelFrom(statement, tableEntry->ptrCast<RelTableCatalogEntry>());
        } else {
            auto options = bindParsingOptions(copyStatement.getParsingOptions());
            if (!options.contains(CopyConstants::FROM_OPTION_NAME) ||
                !options.contains(CopyConstants::TO_OPTION_NAME)) {
                throw BinderException(stringFormat(
                    "The table {} has multiple FROM and TO pairs defined in the schema. A specific "
                    "pair of FROM and TO options is expected when copying data into the {} table.",
                    tableName, tableName));
            }
            auto from = options.at(CopyConstants::FROM_OPTION_NAME).getValue<std::string>();
            auto to = options.at(CopyConstants::TO_OPTION_NAME).getValue<std::string>();
            auto relTableName = RelGroupCatalogEntry::getChildTableName(tableName, from, to);
            if (catalog->containsTable(transaction, relTableName)) {
                auto relEntry = catalog->getTableCatalogEntry(transaction, relTableName);
                return bindCopyRelFrom(statement, relEntry->ptrCast<RelTableCatalogEntry>());
            }
        }
        throw BinderException(stringFormat("REL GROUP {} does not exist.", tableName));
    } else if (catalog->getTableCatalogEntry(transaction, tableName)) {
        auto tableEntry = catalog->getTableCatalogEntry(transaction, tableName);
        switch (tableEntry->getType()) {
        case CatalogEntryType::NODE_TABLE_ENTRY: {
            auto nodeTableEntry = tableEntry->ptrCast<NodeTableCatalogEntry>();
            return bindCopyNodeFrom(statement, nodeTableEntry);
        }
        case CatalogEntryType::REL_TABLE_ENTRY: {
            auto relTableEntry = tableEntry->ptrCast<RelTableCatalogEntry>();
            return bindCopyRelFrom(statement, relTableEntry);
        }
        default: {
            KU_UNREACHABLE;
        }
        }
    }
    throw BinderException(stringFormat("Table {} does not exist.", tableName));
}

static void bindExpectedNodeColumns(const NodeTableCatalogEntry* nodeTableEntry,
    const std::vector<std::string>& inputColumnNames, std::vector<std::string>& columnNames,
    std::vector<LogicalType>& columnTypes);
static void bindExpectedRelColumns(const RelTableCatalogEntry* relTableEntry,
    const std::vector<std::string>& inputColumnNames, std::vector<std::string>& columnNames,
    std::vector<LogicalType>& columnTypes, const main::ClientContext* context);

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
    auto boundSource = bindScanSource(copyStatement.getSource(), copyStatement.getParsingOptions(),
        expectedColumnNames, expectedColumnTypes);
    expression_vector warningDataExprs = boundSource->getWarningColumns();
    if (boundSource->type == ScanSourceType::FILE) {
        auto& source = boundSource->constCast<BoundTableScanSource>();
        auto bindData = source.info.bindData->constPtrCast<ScanFileBindData>();
        if (copyStatement.byColumn() &&
            bindData->fileScanInfo.fileTypeInfo.fileType != FileType::NPY) {
            throw BinderException(stringFormat("Copy by column with {} file type is not supported.",
                bindData->fileScanInfo.fileTypeInfo.fileTypeStr));
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
    auto offset =
        createInvisibleVariable(std::string(InternalKeyword::ROW_OFFSET), LogicalType::INT64());
    auto boundCopyFromInfo = BoundCopyFromInfo(nodeTableEntry, std::move(boundSource),
        std::move(offset), std::move(columns), std::move(evaluateTypes), nullptr /* extraInfo */);
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCopyRelFrom(const Statement& statement,
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
    auto boundSource = bindScanSource(copyStatement.getSource(), copyStatement.getParsingOptions(),
        expectedColumnNames, expectedColumnTypes);
    expression_vector warningDataExprs = boundSource->getWarningColumns();
    auto columns = boundSource->getColumns();
    auto offset =
        createInvisibleVariable(std::string(InternalKeyword::ROW_OFFSET), LogicalType::INT64());
    auto srcTableID = relTableEntry->getSrcTableID();
    auto dstTableID = relTableEntry->getDstTableID();

    auto srcOffset = createVariable(std::string(InternalKeyword::SRC_OFFSET), LogicalType::INT64());
    auto dstOffset = createVariable(std::string(InternalKeyword::DST_OFFSET), LogicalType::INT64());
    expression_vector columnExprs{srcOffset, dstOffset, offset};
    std::vector<ColumnEvaluateType> evaluateTypes{ColumnEvaluateType::REFERENCE,
        ColumnEvaluateType::REFERENCE, ColumnEvaluateType::REFERENCE};
    auto properties = relTableEntry->getProperties();
    for (auto i = 1u; i < properties.size(); ++i) { // skip internal ID
        auto& property = properties[i];
        auto [evaluateType, column] =
            matchColumnExpression(boundSource->getColumns(), property, expressionBinder);
        columnExprs.push_back(column);
        evaluateTypes.push_back(evaluateType);
    }
    columnExprs.insert(columnExprs.end(), warningDataExprs.begin(), warningDataExprs.end());
    std::shared_ptr<Expression> srcKey = nullptr, dstKey = nullptr;
    if (expectedColumnTypes[0] != columns[0]->getDataType()) {
        srcKey = expressionBinder.forceCast(columns[0], expectedColumnTypes[0]);
    } else {
        srcKey = columns[0];
    }
    if (expectedColumnTypes[1] != columns[1]->getDataType()) {
        dstKey = expressionBinder.forceCast(columns[1], expectedColumnTypes[1]);
    } else {
        dstKey = columns[1];
    }
    auto srcLookUpInfo = IndexLookupInfo(srcTableID, srcOffset, srcKey, warningDataExprs);
    auto dstLookUpInfo = IndexLookupInfo(dstTableID, dstOffset, dstKey, warningDataExprs);
    auto lookupInfos = std::vector<IndexLookupInfo>{srcLookUpInfo, dstLookUpInfo};
    auto internalIDColumnIndices = std::vector<idx_t>{0, 1, 2};
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

static void bindExpectedColumns(const TableCatalogEntry* tableEntry,
    const std::vector<std::string>& inputColumnNames, std::vector<std::string>& columnNames,
    std::vector<LogicalType>& columnTypes) {
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

void bindExpectedNodeColumns(const NodeTableCatalogEntry* nodeTableEntry,
    const std::vector<std::string>& inputColumnNames, std::vector<std::string>& columnNames,
    std::vector<LogicalType>& columnTypes) {
    KU_ASSERT(columnNames.empty() && columnTypes.empty());
    bindExpectedColumns(nodeTableEntry, inputColumnNames, columnNames, columnTypes);
}

void bindExpectedRelColumns(const RelTableCatalogEntry* relTableEntry,
    const std::vector<std::string>& inputColumnNames, std::vector<std::string>& columnNames,
    std::vector<LogicalType>& columnTypes, const main::ClientContext* context) {
    KU_ASSERT(columnNames.empty() && columnTypes.empty());
    auto catalog = context->getCatalog();
    auto transaction = context->getTransaction();
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

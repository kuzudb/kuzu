#include "binder/binder.h"
#include "binder/copy/bound_copy_from.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "catalog/catalog_entry/rel_table_catalog_entry.h"
#include "common/enums/table_type.h"
#include "common/exception/binder.h"
#include "common/string_format.h"
#include "function/table/bind_input.h"
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
    auto& copyStatement = ku_dynamic_cast<const Statement&, const CopyFrom&>(statement);
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
        auto nodeTableEntry =
            ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(tableEntry);
        return bindCopyNodeFrom(statement, nodeTableEntry);
    }
    case TableType::REL: {
        auto relTableEntry = ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(tableEntry);
        return bindCopyRelFrom(statement, relTableEntry);
    }
    case TableType::RDF: {
        auto rdfGraphEntry = ku_dynamic_cast<TableCatalogEntry*, RDFGraphCatalogEntry*>(tableEntry);
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

std::unique_ptr<BoundStatement> Binder::bindCopyNodeFrom(const Statement& statement,
    NodeTableCatalogEntry* nodeTableEntry) {
    auto& copyStatement = ku_dynamic_cast<const Statement&, const CopyFrom&>(statement);
    // Bind expected columns based on catalog information.
    std::vector<std::string> expectedColumnNames;
    std::vector<LogicalType> expectedColumnTypes;
    bindExpectedNodeColumns(nodeTableEntry, copyStatement.getColumnNames(), expectedColumnNames,
        expectedColumnTypes);
    auto boundSource = bindScanSource(copyStatement.getSource(),
        copyStatement.getParsingOptionsRef(), expectedColumnNames, expectedColumnTypes);
    if (boundSource->type == ScanSourceType::FILE) {
        auto fileSource =
            ku_dynamic_cast<BoundBaseScanSource*, BoundFileScanSource*>(boundSource.get());
        auto bindData = ku_dynamic_cast<TableFuncBindData*, ScanBindData*>(
            fileSource->fileScanInfo.bindData.get());
        if (copyStatement.byColumn() && bindData->config.fileType != FileType::NPY) {
            throw BinderException(stringFormat("Copy by column with {} file type is not supported.",
                FileTypeUtils::toString(bindData->config.fileType)));
        }
    }
    auto offset = expressionBinder.createVariableExpression(*LogicalType::INT64(),
        std::string(InternalKeyword::ANONYMOUS));
    auto boundCopyFromInfo =
        BoundCopyFromInfo(nodeTableEntry, std::move(boundSource), offset, nullptr /* extraInfo */);
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCopyRelFrom(const parser::Statement& statement,
    RelTableCatalogEntry* relTableEntry) {
    auto& copyStatement = ku_dynamic_cast<const Statement&, const CopyFrom&>(statement);
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
    auto columns = boundSource->getColumns();
    auto offset = expressionBinder.createVariableExpression(*LogicalType::INT64(),
        std::string(InternalKeyword::ROW_OFFSET));
    auto srcTableID = relTableEntry->getSrcTableID();
    auto dstTableID = relTableEntry->getDstTableID();
    auto catalog = clientContext->getCatalog();
    auto srcEntry = catalog->getTableCatalogEntry(clientContext->getTx(), srcTableID);
    auto dstEntry = catalog->getTableCatalogEntry(clientContext->getTx(), dstTableID);
    auto srcNodeEntry = ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(srcEntry);
    auto dstNodeEntry = ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(dstEntry);
    auto srcKey = columns[0];
    auto dstKey = columns[1];
    expression_vector propertyColumns;
    for (auto i = 2u; i < columns.size(); ++i) {
        propertyColumns.push_back(columns[i]);
    }
    auto srcOffset = createVariable(InternalKeyword::SRC_OFFSET, LogicalTypeID::INT64);
    auto dstOffset = createVariable(InternalKeyword::DST_OFFSET, LogicalTypeID::INT64);
    auto srcPkType = srcNodeEntry->getPrimaryKey()->getDataType();
    auto dstPkType = dstNodeEntry->getPrimaryKey()->getDataType();
    auto srcLookUpInfo = IndexLookupInfo(srcTableID, srcOffset, srcKey, *srcPkType);
    auto dstLookUpInfo = IndexLookupInfo(dstTableID, dstOffset, dstKey, *dstPkType);
    auto extraCopyRelInfo = std::make_unique<ExtraBoundCopyRelInfo>();
    extraCopyRelInfo->fromOffset = srcOffset;
    extraCopyRelInfo->toOffset = dstOffset;
    extraCopyRelInfo->propertyColumns = std::move(propertyColumns);
    extraCopyRelInfo->infos.push_back(std::move(srcLookUpInfo));
    extraCopyRelInfo->infos.push_back(std::move(dstLookUpInfo));
    auto boundCopyFromInfo =
        BoundCopyFromInfo(relTableEntry, boundSource->copy(), offset, std::move(extraCopyRelInfo));
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

static bool skipPropertyInFile(const Property& property) {
    if (*property.getDataType() == *LogicalType::SERIAL()) {
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
            if (!tableEntry->containProperty(columnName)) {
                throw BinderException(stringFormat("Table {} does not contain column {}.",
                    tableEntry->getName(), columnName));
            }
            auto propertyID = tableEntry->getPropertyID(columnName);
            auto property = tableEntry->getProperty(propertyID);
            if (skipPropertyInFile(*property)) {
                continue;
            }
            columnNames.push_back(columnName);
            columnTypes.push_back(*property->getDataType());
        }
    } else {
        // No column specified. Fall back to schema columns.
        for (auto& property : tableEntry->getPropertiesRef()) {
            if (skipPropertyInFile(property)) {
                continue;
            }
            columnNames.push_back(property.getName());
            columnTypes.push_back(*property.getDataType());
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
    auto srcEntry = catalog->getTableCatalogEntry(context->getTx(), relTableEntry->getSrcTableID());
    auto srcTable = ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(srcEntry);
    auto dstEntry = catalog->getTableCatalogEntry(context->getTx(), relTableEntry->getDstTableID());
    auto dstTable = ku_dynamic_cast<TableCatalogEntry*, NodeTableCatalogEntry*>(dstEntry);
    columnNames.push_back("from");
    columnNames.push_back("to");
    auto srcPKColumnType = *srcTable->getPrimaryKey()->getDataType();
    if (srcPKColumnType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
        srcPKColumnType = *LogicalType::INT64();
    }
    auto dstPKColumnType = *dstTable->getPrimaryKey()->getDataType();
    if (dstPKColumnType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
        dstPKColumnType = *LogicalType::INT64();
    }
    columnTypes.push_back(std::move(srcPKColumnType));
    columnTypes.push_back(std::move(dstPKColumnType));
    bindExpectedColumns(relTableEntry, inputColumnNames, columnNames, columnTypes);
}

} // namespace binder
} // namespace kuzu

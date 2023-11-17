#include "binder/binder.h"
#include "binder/copy/bound_copy_from.h"
#include "binder/copy/bound_copy_to.h"
#include "catalog/node_table_schema.h"
#include "catalog/rel_table_schema.h"
#include "common/enums/table_type.h"
#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "common/string_format.h"
#include "function/table_functions/bind_input.h"
#include "parser/copy.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCopyToClause(const Statement& statement) {
    auto& copyToStatement = reinterpret_cast<const CopyTo&>(statement);
    auto boundFilePath = copyToStatement.getFilePath();
    auto fileType = bindFileType(boundFilePath);
    std::vector<std::string> columnNames;
    std::vector<std::unique_ptr<LogicalType>> columnTypes;
    auto query = bindQuery(*copyToStatement.getRegularQuery());
    auto columns = query->getStatementResult()->getColumns();
    for (auto& column : columns) {
        auto columnName = column->hasAlias() ? column->getAlias() : column->toString();
        columnNames.push_back(columnName);
        columnTypes.push_back(column->getDataType().copy());
    }
    if (fileType != FileType::CSV && fileType != FileType::PARQUET) {
        throw BinderException(ExceptionMessage::validateCopyToCSVParquetExtensionsException());
    }
    if (fileType != FileType::CSV && copyToStatement.getParsingOptionsRef().size() != 0) {
        throw BinderException{"Only copy to csv can have options."};
    }
    auto csvOption = bindParsingOptions(copyToStatement.getParsingOptionsRef());
    return std::make_unique<BoundCopyTo>(boundFilePath, fileType, std::move(columnNames),
        std::move(columnTypes), std::move(query), std::move(csvOption));
}

// As a temporary constraint, we require npy files loaded with COPY FROM BY COLUMN keyword.
// And csv and parquet files loaded with COPY FROM keyword.
static void validateByColumnKeyword(FileType fileType, bool byColumn) {
    if (fileType == FileType::NPY && !byColumn) {
        throw BinderException(ExceptionMessage::validateCopyNPYByColumnException());
    }
    if (fileType != FileType::NPY && byColumn) {
        throw BinderException(ExceptionMessage::validateCopyCSVParquetByColumnException());
    }
}

static void validateCopyNpyNotForRelTables(TableSchema* schema) {
    if (schema->tableType == TableType::REL) {
        throw BinderException(
            ExceptionMessage::validateCopyNpyNotForRelTablesException(schema->tableName));
    }
}

std::unique_ptr<BoundStatement> Binder::bindCopyFromClause(const Statement& statement) {
    auto& copyStatement = reinterpret_cast<const CopyFrom&>(statement);
    auto catalogContent = catalog.getReadOnlyVersion();
    auto tableName = copyStatement.getTableName();
    validateTableExist(tableName);
    // Bind to table schema.
    auto tableID = catalogContent->getTableID(tableName);
    auto tableSchema = catalogContent->getTableSchema(tableID);
    switch (tableSchema->tableType) {
    case TableType::REL_GROUP:
    case TableType::RDF: {
        throw BinderException(stringFormat("Cannot copy into {} table with type {}.", tableName,
            TableTypeUtils::toString(tableSchema->tableType)));
    }
    default:
        break;
    }
    auto csvReaderConfig = bindParsingOptions(copyStatement.getParsingOptionsRef());
    auto filePaths = bindFilePaths(copyStatement.getFilePaths());
    auto fileType = bindFileType(filePaths);
    auto readerConfig =
        std::make_unique<ReaderConfig>(fileType, std::move(filePaths), std::move(csvReaderConfig));
    validateByColumnKeyword(readerConfig->fileType, copyStatement.byColumn());
    if (readerConfig->fileType == FileType::NPY) {
        validateCopyNpyNotForRelTables(tableSchema);
    }
    switch (tableSchema->tableType) {
    case TableType::NODE:
        if (readerConfig->fileType == FileType::TURTLE) {
            return bindCopyRdfNodeFrom(statement, std::move(readerConfig), tableSchema);
        } else {
            return bindCopyNodeFrom(statement, std::move(readerConfig), tableSchema);
        }
    case TableType::REL: {
        if (readerConfig->fileType == FileType::TURTLE) {
            return bindCopyRdfRelFrom(statement, std::move(readerConfig), tableSchema);
        } else {
            return bindCopyRelFrom(statement, std::move(readerConfig), tableSchema);
        }
    }
        // LCOV_EXCL_START
    default: {
        KU_UNREACHABLE;
    }
        // LCOV_EXCL_STOP
    }
}

std::unique_ptr<BoundStatement> Binder::bindCopyNodeFrom(const Statement& statement,
    std::unique_ptr<common::ReaderConfig> config, TableSchema* tableSchema) {
    auto& copyStatement = reinterpret_cast<const CopyFrom&>(statement);
    auto func = getScanFunction(config->fileType, config->csvReaderConfig->parallel);
    // For table with SERIAL columns, we need to read in serial from files.
    auto containsSerial = tableSchema->containsColumnType(LogicalType(LogicalTypeID::SERIAL));
    std::vector<std::string> expectedColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> expectedColumnTypes;
    bindExpectedNodeColumns(
        tableSchema, copyStatement.getColumnNames(), expectedColumnNames, expectedColumnTypes);
    auto bindInput = std::make_unique<function::ScanTableFuncBindInput>(
        memoryManager, *config, std::move(expectedColumnNames), std::move(expectedColumnTypes));
    auto bindData = func->bindFunc(clientContext, bindInput.get(), catalog.getReadOnlyVersion());
    expression_vector columns;
    for (auto i = 0u; i < bindData->columnTypes.size(); i++) {
        columns.push_back(createVariable(bindData->columnNames[i], *bindData->columnTypes[i]));
    }
    auto offset = expressionBinder.createVariableExpression(
        LogicalType(LogicalTypeID::INT64), InternalKeyword::ANONYMOUS);
    auto boundFileScanInfo =
        std::make_unique<BoundFileScanInfo>(func, std::move(bindData), columns, std::move(offset));
    auto boundCopyFromInfo = std::make_unique<BoundCopyFromInfo>(
        tableSchema, std::move(boundFileScanInfo), containsSerial, nullptr /* extraInfo */);
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCopyRelFrom(const parser::Statement& statement,
    std::unique_ptr<common::ReaderConfig> config, TableSchema* tableSchema) {
    auto& copyStatement = reinterpret_cast<const CopyFrom&>(statement);
    auto func = getScanFunction(config->fileType, config->csvReaderConfig->parallel);
    // For table with SERIAL columns, we need to read in serial from files.
    auto containsSerial = tableSchema->containsColumnType(LogicalType(LogicalTypeID::SERIAL));
    KU_ASSERT(containsSerial == false);
    std::vector<std::string> expectedColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> expectedColumnTypes;
    bindExpectedRelColumns(
        tableSchema, copyStatement.getColumnNames(), expectedColumnNames, expectedColumnTypes);
    auto bindInput = std::make_unique<function::ScanTableFuncBindInput>(memoryManager,
        std::move(*config), std::move(expectedColumnNames), std::move(expectedColumnTypes));
    auto bindData = func->bindFunc(clientContext, bindInput.get(), catalog.getReadOnlyVersion());
    expression_vector columns;
    for (auto i = 0u; i < bindData->columnTypes.size(); i++) {
        columns.push_back(createVariable(bindData->columnNames[i], *bindData->columnTypes[i]));
    }
    auto offset = expressionBinder.createVariableExpression(
        LogicalType(LogicalTypeID::INT64), std::string(InternalKeyword::ROW_OFFSET));
    auto boundFileScanInfo =
        std::make_unique<BoundFileScanInfo>(func, std::move(bindData), columns, offset);
    auto relTableSchema = reinterpret_cast<RelTableSchema*>(tableSchema);
    auto srcTableSchema =
        catalog.getReadOnlyVersion()->getTableSchema(relTableSchema->getSrcTableID());
    auto dstTableSchema =
        catalog.getReadOnlyVersion()->getTableSchema(relTableSchema->getDstTableID());
    auto srcKey = columns[0];
    auto dstKey = columns[1];
    auto srcNodeID = createVariable(std::string(InternalKeyword::SRC_OFFSET), LogicalTypeID::INT64);
    auto dstNodeID = createVariable(std::string(InternalKeyword::DST_OFFSET), LogicalTypeID::INT64);
    auto extraCopyRelInfo = std::make_unique<ExtraBoundCopyRelInfo>(
        srcTableSchema, dstTableSchema, srcNodeID, dstNodeID, srcKey, dstKey);
    auto boundCopyFromInfo = std::make_unique<BoundCopyFromInfo>(
        tableSchema, std::move(boundFileScanInfo), containsSerial, std::move(extraCopyRelInfo));
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

static bool skipPropertyInFile(const Property& property) {
    return property.getDataType()->getLogicalTypeID() == LogicalTypeID::SERIAL ||
           TableSchema::isReservedPropertyName(property.getName());
}

static void bindExpectedColumns(TableSchema* tableSchema,
    const std::vector<std::string>& inputColumnNames, std::vector<std::string>& columnNames,
    logical_types_t& columnTypes) {
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
            if (!tableSchema->containProperty(columnName)) {
                throw BinderException(stringFormat(
                    "Table {} does not contain column {}.", tableSchema->tableName, columnName));
            }
            auto propertyID = tableSchema->getPropertyID(columnName);
            auto property = tableSchema->getProperty(propertyID);
            if (skipPropertyInFile(*property)) {
                continue;
            }
            columnNames.push_back(columnName);
            columnTypes.push_back(property->getDataType()->copy());
        }
    } else {
        // No column specified. Fall back to schema columns.
        for (auto& property : tableSchema->properties) {
            if (skipPropertyInFile(*property)) {
                continue;
            }
            columnNames.push_back(property->getName());
            columnTypes.push_back(property->getDataType()->copy());
        }
    }
}

void Binder::bindExpectedNodeColumns(catalog::TableSchema* tableSchema,
    const std::vector<std::string>& inputColumnNames, std::vector<std::string>& columnNames,
    std::vector<std::unique_ptr<common::LogicalType>>& columnTypes) {
    KU_ASSERT(columnNames.empty() && columnTypes.empty());
    bindExpectedColumns(tableSchema, inputColumnNames, columnNames, columnTypes);
}

void Binder::bindExpectedRelColumns(TableSchema* tableSchema,
    const std::vector<std::string>& inputColumnNames, std::vector<std::string>& columnNames,
    std::vector<std::unique_ptr<common::LogicalType>>& columnTypes) {
    KU_ASSERT(columnNames.empty() && columnTypes.empty());
    auto relTableSchema = reinterpret_cast<RelTableSchema*>(tableSchema);
    auto srcTable = reinterpret_cast<NodeTableSchema*>(
        catalog.getReadOnlyVersion()->getTableSchema(relTableSchema->getSrcTableID()));
    auto dstTable = reinterpret_cast<NodeTableSchema*>(
        catalog.getReadOnlyVersion()->getTableSchema(relTableSchema->getDstTableID()));
    columnNames.push_back("from");
    columnNames.push_back("to");
    auto srcPKColumnType = srcTable->getPrimaryKey()->getDataType()->copy();
    if (srcPKColumnType->getLogicalTypeID() == LogicalTypeID::SERIAL) {
        srcPKColumnType = LogicalType::INT64();
    }
    auto dstPKColumnType = dstTable->getPrimaryKey()->getDataType()->copy();
    if (dstPKColumnType->getLogicalTypeID() == LogicalTypeID::SERIAL) {
        dstPKColumnType = LogicalType::INT64();
    }
    columnTypes.push_back(std::move(srcPKColumnType));
    columnTypes.push_back(std::move(dstPKColumnType));
    bindExpectedColumns(tableSchema, inputColumnNames, columnNames, columnTypes);
}

} // namespace binder
} // namespace kuzu

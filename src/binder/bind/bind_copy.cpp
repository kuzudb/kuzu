#include "binder/binder.h"
#include "binder/copy/bound_copy_from.h"
#include "binder/copy/bound_copy_to.h"
#include "catalog/node_table_schema.h"
#include "catalog/rel_table_schema.h"
#include "common/enums/table_type.h"
#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "common/string_format.h"
#include "function/table_functions.h"
#include "function/table_functions/bind_input.h"
#include "parser/copy.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

static constexpr uint64_t NUM_COLUMNS_TO_SKIP_IN_REL_FILE = 2;

std::unique_ptr<BoundStatement> Binder::bindCopyToClause(const Statement& statement) {
    auto& copyToStatement = (CopyTo&)statement;
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
    auto readerConfig = std::make_unique<ReaderConfig>(
        fileType, std::vector<std::string>{boundFilePath}, columnNames, std::move(columnTypes));
    return std::make_unique<BoundCopyTo>(std::move(readerConfig), std::move(query));
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

static bool bindContainsSerial(TableSchema* tableSchema) {
    bool containsSerial = false;
    for (auto& property : tableSchema->properties) {
        if (property->getDataType()->getLogicalTypeID() == LogicalTypeID::SERIAL) {
            containsSerial = true;
            break;
        }
    }
    return containsSerial;
}

std::unique_ptr<BoundStatement> Binder::bindCopyFromClause(const Statement& statement) {
    auto& copyStatement = (CopyFrom&)statement;
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
    auto inputType = std::make_unique<LogicalType>(LogicalTypeID::STRING);
    std::vector<LogicalType*> inputTypes;
    inputTypes.push_back(inputType.get());
    auto scanFunction =
        getScanFunction(fileType, std::move(inputTypes), readerConfig->csvReaderConfig->parallel);
    std::vector<std::unique_ptr<Value>> inputValues;
    inputValues.push_back(std::make_unique<Value>(Value::createValue(readerConfig->filePaths[0])));
    auto tableFuncBindInput =
        std::make_unique<function::ScanTableFuncBindInput>(*readerConfig, memoryManager);
    validateByColumnKeyword(readerConfig->fileType, copyStatement.byColumn());
    if (readerConfig->fileType == FileType::NPY) {
        validateCopyNpyNotForRelTables(tableSchema);
    }
    switch (tableSchema->tableType) {
    case TableType::NODE:
        if (readerConfig->fileType == FileType::TURTLE) {
            return bindCopyRdfNodeFrom(scanFunction, std::move(readerConfig), tableSchema);
        } else {
            return bindCopyNodeFrom(scanFunction, std::move(readerConfig), tableSchema);
        }
    case TableType::REL: {
        if (readerConfig->fileType == FileType::TURTLE) {
            return bindCopyRdfRelFrom(scanFunction, std::move(readerConfig), tableSchema);
        } else {
            return bindCopyRelFrom(scanFunction, std::move(readerConfig), tableSchema);
        }
    }
        // LCOV_EXCL_START
    default: {
        KU_UNREACHABLE;
    }
        // LCOV_EXCL_STOP
    }
}

std::unique_ptr<BoundStatement> Binder::bindCopyNodeFrom(function::TableFunction* copyFunc,
    std::unique_ptr<common::ReaderConfig> readerConfig, TableSchema* tableSchema) {
    // For table with SERIAL columns, we need to read in serial from files.
    auto containsSerial = bindContainsSerial(tableSchema);
    auto columns = bindExpectedNodeFileColumns(tableSchema, *readerConfig);
    auto tableFuncBindInput =
        std::make_unique<function::ScanTableFuncBindInput>(std::move(*readerConfig), memoryManager);
    auto copyFuncBindData =
        copyFunc->bindFunc(clientContext, tableFuncBindInput.get(), catalog.getReadOnlyVersion());
    auto nodeID = createVariable(std::string(Property::INTERNAL_ID_NAME), LogicalTypeID::INT64);
    auto boundFileScanInfo = std::make_unique<BoundFileScanInfo>(
        copyFunc, copyFuncBindData->copy(), columns, std::move(nodeID), TableType::NODE);
    auto boundCopyFromInfo = std::make_unique<BoundCopyFromInfo>(tableSchema,
        std::move(boundFileScanInfo), containsSerial, std::move(columns), nullptr /* extraInfo */);
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCopyRelFrom(function::TableFunction* copyFunc,
    std::unique_ptr<common::ReaderConfig> readerConfig, TableSchema* tableSchema) {
    // For table with SERIAL columns, we need to read in serial from files.
    auto containsSerial = bindContainsSerial(tableSchema);
    KU_ASSERT(containsSerial == false);
    auto columnsToRead = bindExpectedRelFileColumns(tableSchema, *readerConfig);
    auto tableFuncBindInput =
        std::make_unique<function::ScanTableFuncBindInput>(std::move(*readerConfig), memoryManager);
    auto copyFuncBindData =
        copyFunc->bindFunc(clientContext, tableFuncBindInput.get(), catalog.getReadOnlyVersion());
    auto relID = createVariable(std::string(Property::INTERNAL_ID_NAME), LogicalTypeID::INT64);
    auto boundFileScanInfo = std::make_unique<BoundFileScanInfo>(
        copyFunc, copyFuncBindData->copy(), columnsToRead, relID->copy(), TableType::REL);
    auto relTableSchema = reinterpret_cast<RelTableSchema*>(tableSchema);
    auto srcTableSchema =
        catalog.getReadOnlyVersion()->getTableSchema(relTableSchema->getSrcTableID());
    auto dstTableSchema =
        catalog.getReadOnlyVersion()->getTableSchema(relTableSchema->getDstTableID());
    auto srcKey = columnsToRead[0];
    auto dstKey = columnsToRead[1];
    auto srcNodeID =
        createVariable(std::string(Property::REL_BOUND_OFFSET_NAME), LogicalTypeID::INT64);
    auto dstNodeID =
        createVariable(std::string(Property::REL_NBR_OFFSET_NAME), LogicalTypeID::INT64);
    auto extraCopyRelInfo = std::make_unique<ExtraBoundCopyRelInfo>(
        srcTableSchema, dstTableSchema, srcNodeID, dstNodeID, srcKey, dstKey);
    // Skip the first two columns.
    expression_vector columnsToCopy{std::move(srcNodeID), std::move(dstNodeID), std::move(relID)};
    for (auto i = NUM_COLUMNS_TO_SKIP_IN_REL_FILE; i < columnsToRead.size(); i++) {
        columnsToCopy.push_back(std::move(columnsToRead[i]));
    }
    auto boundCopyFromInfo =
        std::make_unique<BoundCopyFromInfo>(tableSchema, std::move(boundFileScanInfo),
            containsSerial, std::move(columnsToCopy), std::move(extraCopyRelInfo));
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

static bool skipPropertyInFile(const Property& property) {
    return property.getDataType()->getLogicalTypeID() == LogicalTypeID::SERIAL ||
           TableSchema::isReservedPropertyName(property.getName());
}

expression_vector Binder::bindExpectedNodeFileColumns(
    TableSchema* tableSchema, ReaderConfig& readerConfig) {
    // Resolve expected columns.
    std::vector<std::string> expectedColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> expectedColumnTypes;
    switch (readerConfig.fileType) {
    case FileType::NPY:
    case FileType::PARQUET:
    case FileType::CSV: {
        for (auto& property : tableSchema->properties) {
            if (skipPropertyInFile(*property)) {
                continue;
            }
            expectedColumnNames.push_back(property->getName());
            expectedColumnTypes.push_back(property->getDataType()->copy());
        }
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    // Detect columns from file.
    std::vector<std::string> detectedColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> detectedColumnTypes;
    sniffFiles(readerConfig, detectedColumnNames, detectedColumnTypes);
    // Validate.
    validateNumColumns(expectedColumnTypes.size(), detectedColumnTypes.size());
    if (readerConfig.fileType == common::FileType::PARQUET) {
        // HACK(Ziyi): We should allow casting in Parquet reader.
        validateColumnTypes(expectedColumnNames, expectedColumnTypes, detectedColumnTypes);
    }
    return createColumnExpressions(readerConfig, expectedColumnNames, expectedColumnTypes);
}

expression_vector Binder::bindExpectedRelFileColumns(
    TableSchema* tableSchema, ReaderConfig& readerConfig) {
    auto relTableSchema = reinterpret_cast<RelTableSchema*>(tableSchema);
    expression_vector columns;
    switch (readerConfig.fileType) {
    case FileType::CSV:
    case FileType::PARQUET:
    case FileType::NPY: {
        auto srcColumnName = std::string(Property::REL_FROM_PROPERTY_NAME);
        auto dstColumnName = std::string(Property::REL_TO_PROPERTY_NAME);
        readerConfig.columnNames.push_back(srcColumnName);
        readerConfig.columnNames.push_back(dstColumnName);
        auto srcTable =
            catalog.getReadOnlyVersion()->getTableSchema(relTableSchema->getSrcTableID());
        KU_ASSERT(srcTable->tableType == TableType::NODE);
        auto dstTable =
            catalog.getReadOnlyVersion()->getTableSchema(relTableSchema->getDstTableID());
        KU_ASSERT(dstTable->tableType == TableType::NODE);
        auto srcPKColumnType =
            reinterpret_cast<NodeTableSchema*>(srcTable)->getPrimaryKey()->getDataType()->copy();
        if (srcPKColumnType->getLogicalTypeID() == LogicalTypeID::SERIAL) {
            srcPKColumnType = std::make_unique<LogicalType>(LogicalTypeID::INT64);
        }
        auto dstPKColumnType =
            reinterpret_cast<NodeTableSchema*>(dstTable)->getPrimaryKey()->getDataType()->copy();
        if (dstPKColumnType->getLogicalTypeID() == LogicalTypeID::SERIAL) {
            dstPKColumnType = std::make_unique<LogicalType>(LogicalTypeID::INT64);
        }
        columns.push_back(createVariable(srcColumnName, *srcPKColumnType));
        columns.push_back(createVariable(dstColumnName, *dstPKColumnType));
        readerConfig.columnTypes.push_back(std::move(srcPKColumnType));
        readerConfig.columnTypes.push_back(std::move(dstPKColumnType));
        for (auto& property : tableSchema->properties) {
            if (skipPropertyInFile(*property)) {
                continue;
            }
            readerConfig.columnNames.push_back(property->getName());
            auto columnType = property->getDataType()->copy();
            columns.push_back(createVariable(property->getName(), *columnType));
            readerConfig.columnTypes.push_back(std::move(columnType));
        }
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
    // Detect columns from file.
    std::vector<std::string> detectedColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> detectedColumnTypes;
    sniffFiles(readerConfig, detectedColumnNames, detectedColumnTypes);
    // Validate number of columns.
    validateNumColumns(readerConfig.getNumColumns(), detectedColumnTypes.size());
    if (readerConfig.fileType == common::FileType::PARQUET) {
        validateColumnTypes(
            readerConfig.columnNames, readerConfig.columnTypes, detectedColumnTypes);
    }
    return columns;
}

} // namespace binder
} // namespace kuzu

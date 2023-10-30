#include "binder/binder.h"
#include "binder/copy/bound_copy_from.h"
#include "binder/copy/bound_copy_to.h"
#include "catalog/node_table_schema.h"
#include "catalog/rel_table_schema.h"
#include "common/enums/table_type.h"
#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "common/string_format.h"
#include "parser/copy.h"
#include "storage/storage_manager.h"

using namespace kuzu::binder;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

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
    validateByColumnKeyword(readerConfig->fileType, copyStatement.byColumn());
    if (readerConfig->fileType == FileType::NPY) {
        validateCopyNpyNotForRelTables(tableSchema);
    }
    switch (tableSchema->tableType) {
    case TableType::NODE:
        if (readerConfig->fileType == FileType::TURTLE) {
            return bindCopyRdfNodeFrom(std::move(readerConfig), tableSchema);
        } else {
            return bindCopyNodeFrom(std::move(readerConfig), tableSchema);
        }
    case TableType::REL: {
        if (readerConfig->fileType == FileType::TURTLE) {
            return bindCopyRdfRelFrom(std::move(readerConfig), tableSchema);
        } else {
            return bindCopyRelFrom(std::move(readerConfig), tableSchema);
        }
    }
    default:
        throw NotImplementedException("bindCopyFromClause");
    }
}

std::unique_ptr<BoundStatement> Binder::bindCopyNodeFrom(
    std::unique_ptr<ReaderConfig> readerConfig, TableSchema* tableSchema) {
    // For table with SERIAL columns, we need to read in serial from files.
    auto containsSerial = bindContainsSerial(tableSchema);
    auto columns = bindExpectedNodeFileColumns(tableSchema, *readerConfig);
    auto nodeID =
        createVariable(std::string(Property::INTERNAL_ID_NAME), LogicalTypeID::INTERNAL_ID);
    auto boundFileScanInfo = std::make_unique<BoundFileScanInfo>(
        std::move(readerConfig), std::move(columns), std::move(nodeID), TableType::NODE);
    auto boundCopyFromInfo = std::make_unique<BoundCopyFromInfo>(
        tableSchema, std::move(boundFileScanInfo), containsSerial, nullptr /* extraInfo */);
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCopyRdfNodeFrom(
    std::unique_ptr<ReaderConfig> readerConfig, TableSchema* tableSchema) {
    auto containsSerial = bindContainsSerial(tableSchema);
    auto stringType = LogicalType{LogicalTypeID::STRING};
    auto nodeID =
        createVariable(std::string(Property::INTERNAL_ID_NAME), LogicalTypeID::INTERNAL_ID);
    expression_vector columns;
    auto columnName = std::string(RDFKeyword::ANONYMOUS);
    readerConfig->columnNames.push_back(columnName);
    readerConfig->columnTypes.push_back(stringType.copy());
    columns.push_back(createVariable(columnName, stringType));
    if (tableSchema->tableName.ends_with(common::RDFKeyword::RESOURCE_TABLE_SUFFIX)) {
        readerConfig->rdfReaderConfig =
            std::make_unique<RdfReaderConfig>(RdfReaderMode::RESOURCE, nullptr /* index */);
    } else {
        assert(tableSchema->tableName.ends_with(common::RDFKeyword::LITERAL_TABLE_SUFFIX));
        readerConfig->rdfReaderConfig =
            std::make_unique<RdfReaderConfig>(RdfReaderMode::LITERAL, nullptr /* index */);
    }
    auto boundFileScanInfo = std::make_unique<BoundFileScanInfo>(
        std::move(readerConfig), std::move(columns), std::move(nodeID), TableType::NODE);
    auto boundCopyFromInfo = std::make_unique<BoundCopyFromInfo>(
        tableSchema, std::move(boundFileScanInfo), containsSerial, nullptr /* extraInfo */);
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCopyRelFrom(
    std::unique_ptr<ReaderConfig> readerConfig, TableSchema* tableSchema) {
    // For table with SERIAL columns, we need to read in serial from files.
    auto containsSerial = bindContainsSerial(tableSchema);
    assert(containsSerial == false);
    auto columns = bindExpectedRelFileColumns(tableSchema, *readerConfig);
    auto srcKey = columns[0];
    auto dstKey = columns[1];
    auto relID =
        createVariable(std::string(Property::INTERNAL_ID_NAME), LogicalTypeID::INTERNAL_ID);
    auto boundFileScanInfo = std::make_unique<BoundFileScanInfo>(
        std::move(readerConfig), std::move(columns), std::move(relID), TableType::REL);
    auto relTableSchema = reinterpret_cast<RelTableSchema*>(tableSchema);
    auto srcTableSchema =
        catalog.getReadOnlyVersion()->getTableSchema(relTableSchema->getSrcTableID());
    auto dstTableSchema =
        catalog.getReadOnlyVersion()->getTableSchema(relTableSchema->getDstTableID());
    auto arrowColumnType = LogicalType{LogicalTypeID::ARROW_COLUMN};
    auto srcOffset = createVariable(std::string(Property::REL_BOUND_OFFSET_NAME), arrowColumnType);
    auto dstOffset = createVariable(std::string(Property::REL_NBR_OFFSET_NAME), arrowColumnType);
    auto extraCopyRelInfo = std::make_unique<ExtraBoundCopyRelInfo>(
        srcTableSchema, dstTableSchema, srcOffset, dstOffset, srcKey, dstKey);
    auto boundCopyFromInfo = std::make_unique<BoundCopyFromInfo>(
        tableSchema, std::move(boundFileScanInfo), containsSerial, std::move(extraCopyRelInfo));
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCopyRdfRelFrom(
    std::unique_ptr<ReaderConfig> readerConfig, TableSchema* tableSchema) {
    auto containsSerial = bindContainsSerial(tableSchema);
    auto offsetType = std::make_unique<LogicalType>(LogicalTypeID::ARROW_COLUMN);
    expression_vector columns;
    for (auto i = 0u; i < 3; ++i) {
        auto columnName = std::string(RDFKeyword::ANONYMOUS) + std::to_string(i);
        readerConfig->columnNames.push_back(columnName);
        readerConfig->columnTypes.push_back(offsetType->copy());
        columns.push_back(createVariable(columnName, *offsetType));
    }
    auto relTableSchema = reinterpret_cast<RelTableSchema*>(tableSchema);
    auto resourceTableID = relTableSchema->getSrcTableID();
    auto index = storageManager->getNodesStore().getPKIndex(resourceTableID);
    if (tableSchema->tableName.ends_with(common::RDFKeyword::RESOURCE_TRIPLE_TABLE_SUFFIX)) {
        readerConfig->rdfReaderConfig =
            std::make_unique<RdfReaderConfig>(RdfReaderMode::RESOURCE_TRIPLE, index);
    } else {
        readerConfig->rdfReaderConfig =
            std::make_unique<RdfReaderConfig>(RdfReaderMode::LITERAL_TRIPLE, index);
    }
    auto relID =
        createVariable(std::string(Property::INTERNAL_ID_NAME), LogicalTypeID::INTERNAL_ID);
    auto boundFileScanInfo = std::make_unique<BoundFileScanInfo>(
        std::move(readerConfig), columns, std::move(relID), TableType::REL);
    auto extraInfo = std::make_unique<ExtraBoundCopyRdfRelInfo>(columns[0], columns[1], columns[2]);
    auto boundCopyFromInfo = std::make_unique<BoundCopyFromInfo>(
        tableSchema, std::move(boundFileScanInfo), containsSerial, std::move(extraInfo));
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
        throw NotImplementedException{"Binder::bindCopyNodeColumns"};
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
        auto arrowColumnType = LogicalType{LogicalTypeID::ARROW_COLUMN};
        auto srcColumnName = std::string(Property::REL_FROM_PROPERTY_NAME);
        auto dstColumnName = std::string(Property::REL_TO_PROPERTY_NAME);
        readerConfig.columnNames.push_back(srcColumnName);
        readerConfig.columnNames.push_back(dstColumnName);
        auto srcTable =
            catalog.getReadOnlyVersion()->getTableSchema(relTableSchema->getSrcTableID());
        assert(srcTable->tableType == TableType::NODE);
        auto dstTable =
            catalog.getReadOnlyVersion()->getTableSchema(relTableSchema->getDstTableID());
        assert(dstTable->tableType == TableType::NODE);
        readerConfig.columnTypes.push_back(
            reinterpret_cast<NodeTableSchema*>(srcTable)->getPrimaryKey()->getDataType()->copy());
        readerConfig.columnTypes.push_back(
            reinterpret_cast<NodeTableSchema*>(dstTable)->getPrimaryKey()->getDataType()->copy());
        columns.push_back(createVariable(srcColumnName, arrowColumnType));
        columns.push_back(createVariable(dstColumnName, arrowColumnType));
        for (auto& property : tableSchema->properties) {
            if (skipPropertyInFile(*property)) {
                continue;
            }
            readerConfig.columnNames.push_back(property->getName());
            readerConfig.columnTypes.push_back(property->getDataType()->copy());
            columns.push_back(createVariable(property->getName(), arrowColumnType));
        }
    } break;
    default: {
        throw NotImplementedException{"Binder::bindCopyRelColumns"};
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

#include "binder/binder.h"
#include "binder/copy/bound_copy_from.h"
#include "binder/copy/bound_copy_to.h"
#include "catalog/node_table_schema.h"
#include "catalog/rel_table_schema.h"
#include "common/exception/binder.h"
#include "common/exception/message.h"
#include "common/string_utils.h"
#include "common/table_type.h"
#include "parser/copy.h"

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

static void validateCopyNpyFilesMatchSchema(uint32_t numFiles, TableSchema* schema) {
    if (schema->properties.size() != numFiles) {
        throw BinderException(StringUtils::string_format(
            "Number of npy files is not equal to number of properties in table {}.",
            schema->tableName));
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
    validateNodeRelTableExist(tableName);
    // Bind to table schema.
    auto tableID = catalogContent->getTableID(tableName);
    auto tableSchema = catalogContent->getTableSchema(tableID);
    auto csvReaderConfig = bindParsingOptions(copyStatement.getParsingOptionsRef());
    auto filePaths = bindFilePaths(copyStatement.getFilePaths());
    auto fileType = bindFileType(filePaths);
    auto readerConfig =
        std::make_unique<ReaderConfig>(fileType, std::move(filePaths), std::move(csvReaderConfig));
    validateByColumnKeyword(readerConfig->fileType, copyStatement.byColumn());
    if (readerConfig->fileType == FileType::NPY) {
        validateCopyNpyFilesMatchSchema(readerConfig->getNumFiles(), tableSchema);
        validateCopyNpyNotForRelTables(tableSchema);
    }
    switch (tableSchema->tableType) {
    case TableType::NODE:
        return bindCopyNodeFrom(std::move(readerConfig), tableSchema);
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
    auto offset = createVariable(std::string(Property::OFFSET_NAME), LogicalTypeID::INT64);
    auto boundFileScanInfo = std::make_unique<BoundFileScanInfo>(
        std::move(readerConfig), std::move(columns), std::move(offset), TableType::NODE);
    auto boundCopyFromInfo = std::make_unique<BoundCopyFromInfo>(
        tableSchema, std::move(boundFileScanInfo), containsSerial, nullptr);
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
    auto offset = createVariable(std::string(Property::OFFSET_NAME), LogicalTypeID::INT64);
    auto boundFileScanInfo = std::make_unique<BoundFileScanInfo>(
        std::move(readerConfig), std::move(columns), std::move(offset), TableType::REL);
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

static constexpr std::string_view RDF_SUBJECT = "_SUBJECT";
static constexpr std::string_view RDF_PREDICATE = "_PREDICATE";
static constexpr std::string_view RDF_OBJECT = "_OBJECT";
static constexpr std::string_view RDF_SUBJECT_OFFSET = "_SUBJECT_OFFSET";
static constexpr std::string_view RDF_PREDICATE_OFFSET = "_PREDICATE_OFFSET";
static constexpr std::string_view RDF_OBJECT_OFFSET = "_OBJECT_OFFSET";

std::unique_ptr<BoundStatement> Binder::bindCopyRdfRelFrom(
    std::unique_ptr<ReaderConfig> readerConfig, TableSchema* tableSchema) {
    auto columns = bindExpectedRelFileColumns(tableSchema, *readerConfig);
    auto subjectKey = columns[0];
    auto predicateKey = columns[1];
    auto objectKey = columns[2];
    auto offset = createVariable(std::string(Property::OFFSET_NAME), LogicalTypeID::INT64);
    auto containsSerial = false;
    auto boundFileScanInfo = std::make_unique<BoundFileScanInfo>(
        std::move(readerConfig), std::move(columns), std::move(offset), TableType::REL);
    auto relTableSchema = reinterpret_cast<RelTableSchema*>(tableSchema);
    assert(relTableSchema->getSrcTableID() == relTableSchema->getDstTableID());
    auto nodeTableID = relTableSchema->getSrcTableID();
    auto arrowColumnType = LogicalType{LogicalTypeID::ARROW_COLUMN};
    auto subjectOffset = createVariable(std::string(RDF_SUBJECT_OFFSET), arrowColumnType);
    auto predicateOffset = createVariable(std::string(RDF_PREDICATE_OFFSET), arrowColumnType);
    auto objectOffset = createVariable(std::string(RDF_OBJECT_OFFSET), arrowColumnType);
    auto extraInfo = std::make_unique<ExtraBoundCopyRdfRelInfo>(nodeTableID, subjectOffset,
        predicateOffset, objectOffset, subjectKey, predicateKey, objectKey);
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
    expression_vector columns;
    switch (readerConfig.fileType) {
    case FileType::TURTLE: {
        auto stringType = LogicalType{LogicalTypeID::STRING};
        auto columnNames = std::vector<std::string>{
            std::string(RDF_SUBJECT), std::string(RDF_PREDICATE), std::string(RDF_OBJECT)};
        for (auto& columnName : columnNames) {
            readerConfig.columnNames.push_back(columnName);
            readerConfig.columnTypes.push_back(stringType.copy());
            columns.push_back(createVariable(columnName, stringType));
        }
    } break;
    case FileType::CSV: {
        for (auto& property : tableSchema->properties) {
            if (skipPropertyInFile(*property)) {
                continue;
            }
            readerConfig.columnNames.push_back(property->getName());
            readerConfig.columnTypes.push_back(property->getDataType()->copy());
            columns.push_back(createVariable(property->getName(), *property->getDataType()));
        }
    } break;
    case FileType::NPY:
    case FileType::PARQUET: {
        for (auto& property : tableSchema->properties) {
            if (skipPropertyInFile(*property)) {
                continue;
            }
            readerConfig.columnNames.push_back(property->getName());
            readerConfig.columnTypes.push_back(property->getDataType()->copy());
            columns.push_back(
                createVariable(property->getName(), LogicalType{LogicalTypeID::ARROW_COLUMN}));
        }
    } break;
    default: {
        throw NotImplementedException{"Binder::bindCopyNodeColumns"};
    }
    }
    return columns;
}

expression_vector Binder::bindExpectedRelFileColumns(
    TableSchema* tableSchema, ReaderConfig& readerConfig) {
    auto relTableSchema = reinterpret_cast<RelTableSchema*>(tableSchema);
    expression_vector columns;
    switch (readerConfig.fileType) {
    case FileType::TURTLE: {
        auto stringType = LogicalType{LogicalTypeID::STRING};
        auto columnNames = std::vector<std::string>{
            std::string(RDF_SUBJECT), std::string(RDF_PREDICATE), std::string(RDF_OBJECT)};
        for (auto& columnName : columnNames) {
            readerConfig.columnNames.push_back(columnName);
            readerConfig.columnTypes.push_back(stringType.copy());
            columns.push_back(createVariable(columnName, stringType));
        }
    } break;
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
    return columns;
}

} // namespace binder
} // namespace kuzu

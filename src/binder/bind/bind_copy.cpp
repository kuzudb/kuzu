#include "binder/binder.h"
#include "binder/copy/bound_copy_from.h"
#include "binder/copy/bound_copy_to.h"
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
    std::vector<LogicalType> columnTypes;
    auto query = bindQuery(*copyToStatement.getRegularQuery());
    auto columns = query->getStatementResult()->getColumns();
    for (auto& column : columns) {
        auto columnName = column->hasAlias() ? column->getAlias() : column->toString();
        columnNames.push_back(columnName);
        columnTypes.push_back(column->getDataType());
    }
    if (fileType != CopyDescription::FileType::CSV &&
        fileType != CopyDescription::FileType::PARQUET) {
        throw BinderException("COPY TO currently only supports csv and parquet files.");
    }
    auto copyDescription = std::make_unique<CopyDescription>(
        fileType, std::vector<std::string>{boundFilePath}, nullptr /* parsing option */);
    copyDescription->columnNames = columnNames;
    copyDescription->columnTypes = columnTypes;
    return std::make_unique<BoundCopyTo>(std::move(copyDescription), std::move(query));
}

// As a temporary constraint, we require npy files loaded with COPY FROM BY COLUMN keyword.
// And csv and parquet files loaded with COPY FROM keyword.
static void validateByColumnKeyword(CopyDescription::FileType fileType, bool byColumn) {
    if (fileType == CopyDescription::FileType::NPY && !byColumn) {
        throw BinderException(ExceptionMessage::validateCopyNPYByColumnException());
    }
    if (fileType != CopyDescription::FileType::NPY && byColumn) {
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
    auto copyDesc =
        bindCopyDesc(copyStatement.getFilePaths(), copyStatement.getParsingOptionsRef());
    validateByColumnKeyword(copyDesc->fileType, copyStatement.byColumn());
    if (copyDesc->fileType == CopyDescription::FileType::NPY) {
        validateCopyNpyFilesMatchSchema(copyDesc->filePaths.size(), tableSchema);
        validateCopyNpyNotForRelTables(tableSchema);
    }
    switch (tableSchema->tableType) {
    case TableType::NODE:
        return bindCopyNodeFrom(std::move(copyDesc), tableSchema);
    case TableType::REL: {
        if (copyDesc->fileType == CopyDescription::FileType::TURTLE) {
            return bindCopyRdfRelFrom(std::move(copyDesc), tableSchema);
        } else {
            return bindCopyRelFrom(std::move(copyDesc), tableSchema);
        }
    }
    default:
        throw NotImplementedException("bindCopyFromClause");
    }
}

std::unique_ptr<BoundStatement> Binder::bindCopyNodeFrom(
    std::unique_ptr<CopyDescription> copyDesc, TableSchema* tableSchema) {
    // For table with SERIAL columns, we need to read in serial from files.
    auto containsSerial = bindContainsSerial(tableSchema);
    auto columns = bindExpectedNodeFileColumns(tableSchema, copyDesc->fileType);
    auto offset = createVariable(std::string(Property::OFFSET_NAME), LogicalTypeID::INT64);
    auto boundFileScanInfo = std::make_unique<BoundFileScanInfo>(
        std::move(copyDesc), std::move(columns), std::move(offset), tableSchema, containsSerial);
    auto boundCopyFromInfo = std::make_unique<BoundCopyFromInfo>(
        tableSchema, std::move(boundFileScanInfo), containsSerial, nullptr);
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCopyRelFrom(
    std::unique_ptr<CopyDescription> copyDesc, TableSchema* tableSchema) {
    // For table with SERIAL columns, we need to read in serial from files.
    auto containsSerial = bindContainsSerial(tableSchema);
    auto columns = bindExpectedRelFileColumns(tableSchema, copyDesc->fileType);
    auto srcKey = columns[0];
    auto dstKey = columns[1];
    auto offset = createVariable(std::string(Property::OFFSET_NAME), LogicalTypeID::INT64);
    auto boundFileScanInfo = std::make_unique<BoundFileScanInfo>(
        std::move(copyDesc), std::move(columns), std::move(offset), tableSchema, containsSerial);
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
    std::unique_ptr<CopyDescription> copyDesc, TableSchema* tableSchema) {
    auto columns = bindExpectedRelFileColumns(tableSchema, copyDesc->fileType);
    auto subjectKey = columns[0];
    auto predicateKey = columns[1];
    auto objectKey = columns[2];
    auto offset = createVariable(std::string(Property::OFFSET_NAME), LogicalTypeID::INT64);
    auto containsSerial = false;
    auto boundFileScanInfo = std::make_unique<BoundFileScanInfo>(
        std::move(copyDesc), std::move(columns), std::move(offset), tableSchema, containsSerial);
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
    TableSchema* tableSchema, CopyDescription::FileType fileType) {
    expression_vector columns;
    switch (fileType) {
    case common::CopyDescription::FileType::TURTLE: {
        auto stringType = LogicalType{LogicalTypeID::STRING};
        columns.push_back(createVariable(std::string(RDF_SUBJECT), stringType));
        columns.push_back(createVariable(std::string(RDF_PREDICATE), stringType));
        columns.push_back(createVariable(std::string(RDF_OBJECT), stringType));
    } break;
    case common::CopyDescription::FileType::CSV: {
        for (auto& property : tableSchema->properties) {
            if (skipPropertyInFile(*property)) {
                continue;
            }
            columns.push_back(createVariable(property->getName(), *property->getDataType()));
        }
    } break;
    case common::CopyDescription::FileType::NPY:
    case common::CopyDescription::FileType::PARQUET: {
        for (auto& property : tableSchema->properties) {
            if (skipPropertyInFile(*property)) {
                continue;
            }
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
    TableSchema* tableSchema, CopyDescription::FileType fileType) {
    expression_vector columns;
    switch (fileType) {
    case common::CopyDescription::FileType::TURTLE: {
        auto stringType = LogicalType{LogicalTypeID::STRING};
        columns.push_back(createVariable("SUBJECT", stringType));
        columns.push_back(createVariable("PREDICATE", stringType));
        columns.push_back(createVariable("OBJECT", stringType));
    } break;
    case common::CopyDescription::FileType::CSV:
    case common::CopyDescription::FileType::PARQUET:
    case common::CopyDescription::FileType::NPY: {
        auto arrowColumnType = LogicalType{LogicalTypeID::ARROW_COLUMN};
        auto srcKey =
            createVariable(std::string(Property::REL_FROM_PROPERTY_NAME), arrowColumnType);
        auto dstKey = createVariable(std::string(Property::REL_TO_PROPERTY_NAME), arrowColumnType);
        columns.push_back(srcKey);
        columns.push_back(dstKey);
        for (auto& property : tableSchema->properties) {
            if (skipPropertyInFile(*property)) {
                continue;
            }
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

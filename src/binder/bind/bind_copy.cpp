#include "binder/binder.h"
#include "binder/copy/bound_copy_from.h"
#include "binder/copy/bound_copy_to.h"
#include "binder/expression/literal_expression.h"
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
    auto query = bindQuery(*copyToStatement.getRegularQuery());
    auto columns = query->getStatementResult()->getColumns();
    for (auto& column : columns) {
        auto columnName = column->hasAlias() ? column->getAlias() : column->toString();
        columnNames.push_back(columnName);
    }
    if (fileType != CopyDescription::FileType::CSV) {
        throw BinderException("COPY TO currently only supports csv files.");
    }
    auto copyDescription = std::make_unique<CopyDescription>(
        fileType, std::vector<std::string>{boundFilePath}, columnNames);
    return std::make_unique<BoundCopyTo>(std::move(copyDescription), std::move(query));
}

// As a temporary constraint, we require npy files loaded with COPY FROM BY COLUMN keyword.
// And csv and parquet files loaded with COPY FROM keyword.
static void validateCopyNpyKeyword(
    CopyDescription::FileType expectedType, CopyDescription::FileType actualType) {
    if (expectedType == CopyDescription::FileType::UNKNOWN &&
        actualType == CopyDescription::FileType::NPY) {
        throw BinderException(ExceptionMessage::validateCopyNPYByColumnException());
    }
    if (expectedType == CopyDescription::FileType::NPY && actualType != expectedType) {
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

expression_vector Binder::bindColumnExpressions(
    TableSchema* tableSchema, CopyDescription::FileType fileType) {
    expression_vector columnExpressions;
    if (tableSchema->tableType == TableType::REL) {
        // For rel table, add FROM and TO columns as data columns to be read from file.
        columnExpressions.push_back(createVariable(std::string(Property::REL_FROM_PROPERTY_NAME),
            LogicalType{LogicalTypeID::ARROW_COLUMN}));
        columnExpressions.push_back(createVariable(
            std::string(Property::REL_TO_PROPERTY_NAME), LogicalType{LogicalTypeID::ARROW_COLUMN}));
    }
    auto isCopyNodeCSV =
        tableSchema->tableType == TableType::NODE && fileType == CopyDescription::FileType::CSV;
    for (auto& property : tableSchema->properties) {
        if (property->getDataType()->getLogicalTypeID() == LogicalTypeID::SERIAL ||
            TableSchema::isReservedPropertyName(property->getName())) {
            continue;
        } else if (isCopyNodeCSV) {
            columnExpressions.push_back(
                createVariable(property->getName(), *property->getDataType()));
        } else {
            columnExpressions.push_back(
                createVariable(property->getName(), LogicalType{LogicalTypeID::ARROW_COLUMN}));
        }
    }
    return columnExpressions;
}

bool Binder::bindContainsSerial(TableSchema* tableSchema, CopyDescription::FileType fileType) {
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
    // Bind to table schema.
    validateTableExist(catalog, tableName);
    auto tableID = catalogContent->getTableID(tableName);
    auto tableSchema = catalogContent->getTableSchema(tableID);
    // Bind csv reader configuration
    auto csvReaderConfig = bindParsingOptions(copyStatement.getParsingOptions());
    auto boundFilePaths = bindFilePaths(copyStatement.getFilePaths());
    // Bind file type.
    auto actualFileType = bindFileType(boundFilePaths);
    auto expectedFileType = copyStatement.getFileType();
    validateCopyNpyKeyword(expectedFileType, actualFileType);
    if (actualFileType == CopyDescription::FileType::NPY) {
        validateCopyNpyFilesMatchSchema(boundFilePaths.size(), tableSchema);
        validateCopyNpyNotForRelTables(tableSchema);
    }
    // Bind execution mode.
    // For CSV file, and table with SERIAL columns, we need to read in serial from files.
    auto containsSerial = bindContainsSerial(tableSchema, actualFileType);
    // Bind expressions.
    auto columnExpressions = bindColumnExpressions(tableSchema, actualFileType);
    auto copyDescription = std::make_unique<CopyDescription>(
        actualFileType, boundFilePaths, std::move(csvReaderConfig));
    auto nodeOffsetExpression =
        createVariable(std::string(Property::OFFSET_NAME), LogicalType{LogicalTypeID::INT64});
    auto boundOffsetExpression = tableSchema->tableType == TableType::REL ?
                                     createVariable(std::string(Property::REL_BOUND_OFFSET_NAME),
                                         LogicalType{LogicalTypeID::ARROW_COLUMN}) :
                                     nullptr;
    auto nbrOffsetExpression = tableSchema->tableType == TableType::REL ?
                                   createVariable(std::string(Property::REL_NBR_OFFSET_NAME),
                                       LogicalType{LogicalTypeID::ARROW_COLUMN}) :
                                   nullptr;
    auto boundCopyFromInfo = std::make_unique<BoundCopyFromInfo>(std::move(copyDescription),
        tableSchema, std::move(columnExpressions), std::move(nodeOffsetExpression),
        std::move(boundOffsetExpression), std::move(nbrOffsetExpression), containsSerial);
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

std::vector<std::string> Binder::bindFilePaths(const std::vector<std::string>& filePaths) {
    std::vector<std::string> boundFilePaths;
    for (auto& filePath : filePaths) {
        auto globbedFilePaths = FileUtils::globFilePath(filePath);
        if (globbedFilePaths.empty()) {
            throw BinderException{StringUtils::string_format(
                "No file found that matches the pattern: {}.", filePath)};
        }
        boundFilePaths.insert(
            boundFilePaths.end(), globbedFilePaths.begin(), globbedFilePaths.end());
    }
    if (boundFilePaths.empty()) {
        throw BinderException{StringUtils::string_format("Invalid file path: {}.", filePaths[0])};
    }
    return boundFilePaths;
}

std::unique_ptr<CSVReaderConfig> Binder::bindParsingOptions(
    const std::unordered_map<std::string, std::unique_ptr<ParsedExpression>>* parsingOptions) {
    auto csvReaderConfig = std::make_unique<CSVReaderConfig>();
    for (auto& parsingOption : *parsingOptions) {
        auto copyOptionName = parsingOption.first;
        StringUtils::toUpper(copyOptionName);
        bool isValidStringParsingOption = validateStringParsingOptionName(copyOptionName);
        auto copyOptionExpression = parsingOption.second.get();
        auto boundCopyOptionExpression = expressionBinder.bindExpression(*copyOptionExpression);
        assert(boundCopyOptionExpression->expressionType == LITERAL);
        if (copyOptionName == "HEADER") {
            if (boundCopyOptionExpression->dataType.getLogicalTypeID() != LogicalTypeID::BOOL) {
                throw BinderException(
                    "The value type of parsing csv option " + copyOptionName + " must be boolean.");
            }
            csvReaderConfig->hasHeader =
                ((LiteralExpression&)(*boundCopyOptionExpression)).value->getValue<bool>();
        } else if (boundCopyOptionExpression->dataType.getLogicalTypeID() ==
                       LogicalTypeID::STRING &&
                   isValidStringParsingOption) {
            if (boundCopyOptionExpression->dataType.getLogicalTypeID() != LogicalTypeID::STRING) {
                throw BinderException(
                    "The value type of parsing csv option " + copyOptionName + " must be string.");
            }
            auto copyOptionValue =
                ((LiteralExpression&)(*boundCopyOptionExpression)).value->getValue<std::string>();
            bindStringParsingOptions(*csvReaderConfig, copyOptionName, copyOptionValue);
        } else {
            throw BinderException("Unrecognized parsing csv option: " + copyOptionName + ".");
        }
    }
    return csvReaderConfig;
}

void Binder::bindStringParsingOptions(
    CSVReaderConfig& csvReaderConfig, const std::string& optionName, std::string& optionValue) {
    auto parsingOptionValue = bindParsingOptionValue(optionValue);
    if (optionName == "ESCAPE") {
        csvReaderConfig.escapeChar = parsingOptionValue;
    } else if (optionName == "DELIM") {
        csvReaderConfig.delimiter = parsingOptionValue;
    } else if (optionName == "QUOTE") {
        csvReaderConfig.quoteChar = parsingOptionValue;
    } else if (optionName == "LIST_BEGIN") {
        csvReaderConfig.listBeginChar = parsingOptionValue;
    } else if (optionName == "LIST_END") {
        csvReaderConfig.listEndChar = parsingOptionValue;
    }
}

char Binder::bindParsingOptionValue(std::string value) {
    if (value == "\\t") {
        return '\t';
    }
    if (value.length() > 2 || (value.length() == 2 && value[0] != '\\')) {
        throw BinderException("Copy csv option value can only be a single character with an "
                              "optional escape character.");
    }
    return value[value.length() - 1];
}

CopyDescription::FileType Binder::bindFileType(const std::string& filePath) {
    std::filesystem::path fileName(filePath);
    auto extension = FileUtils::getFileExtension(fileName);
    auto fileType = CopyDescription::getFileTypeFromExtension(extension);
    if (fileType == CopyDescription::FileType::UNKNOWN) {
        throw CopyException("Unsupported file type: " + filePath);
    }
    return fileType;
}

CopyDescription::FileType Binder::bindFileType(const std::vector<std::string>& filePaths) {
    auto expectedFileType = CopyDescription::FileType::UNKNOWN;
    for (auto& filePath : filePaths) {
        auto fileType = bindFileType(filePath);
        expectedFileType =
            (expectedFileType == CopyDescription::FileType::UNKNOWN) ? fileType : expectedFileType;
        if (fileType != expectedFileType) {
            throw CopyException("Loading files with different types is not currently supported.");
        }
    }
    return expectedFileType;
}

} // namespace binder
} // namespace kuzu

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

// Start file binding
static CopyDescription::FileType bindFileType(const std::string& filePath) {
    std::filesystem::path fileName(filePath);
    auto extension = FileUtils::getFileExtension(fileName);
    auto fileType = CopyDescription::getFileTypeFromExtension(extension);
    if (fileType == CopyDescription::FileType::UNKNOWN) {
        throw CopyException("Unsupported file type: " + filePath);
    }
    return fileType;
}

static CopyDescription::FileType bindFileType(const std::vector<std::string>& filePaths) {
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

static std::vector<std::string> bindFilePaths(const std::vector<std::string>& filePaths) {
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
// End file binding

// Start parsing option binding
static char bindParsingOptionValue(std::string value) {
    if (value == "\\t") {
        return '\t';
    }
    if (value.length() > 2 || (value.length() == 2 && value[0] != '\\')) {
        throw BinderException("Copy csv option value can only be a single character with an "
                              "optional escape character.");
    }
    return value[value.length() - 1];
}

static void bindStringParsingOptions(
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

static bool validateStringParsingOptionName(std::string& parsingOptionName) {
    for (auto i = 0; i < std::size(CopyConstants::STRING_CSV_PARSING_OPTIONS); i++) {
        if (parsingOptionName == CopyConstants::STRING_CSV_PARSING_OPTIONS[i]) {
            return true;
        }
    }
    return false;
}
// End parsing option binding

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
    // Bind csv reader configuration
    auto csvReaderConfig = bindParsingOptions(copyStatement.getParsingOptionsRef());
    auto boundFilePaths = bindFilePaths(copyStatement.getFilePaths());
    // Bind file type.
    auto fileType = bindFileType(boundFilePaths);
    validateByColumnKeyword(fileType, copyStatement.byColumn());
    if (fileType == CopyDescription::FileType::NPY) {
        validateCopyNpyFilesMatchSchema(boundFilePaths.size(), tableSchema);
        validateCopyNpyNotForRelTables(tableSchema);
    }
    auto copyDescription =
        std::make_unique<CopyDescription>(fileType, boundFilePaths, std::move(csvReaderConfig));
    switch (tableSchema->tableType) {
    case TableType::NODE:
        return bindCopyNodeFrom(std::move(copyDescription), tableSchema);
    case TableType::REL:
        return bindCopyRelFrom(std::move(copyDescription), tableSchema);
    default:
        throw NotImplementedException("bindCopyFromClause");
    }
}

std::unique_ptr<BoundStatement> Binder::bindCopyNodeFrom(
    std::unique_ptr<CopyDescription> copyDescription, TableSchema* tableSchema) {
    // For table with SERIAL columns, we need to read in serial from files.
    auto containsSerial = bindContainsSerial(tableSchema);
    auto columns = bindCopyNodeColumns(tableSchema, copyDescription->fileType);
    auto nodeOffset =
        createVariable(std::string(Property::OFFSET_NAME), LogicalType{LogicalTypeID::INT64});
    auto boundCopyFromInfo = std::make_unique<BoundCopyFromInfo>(std::move(copyDescription),
        tableSchema, std::move(columns), std::move(nodeOffset), nullptr, nullptr, containsSerial);
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

std::unique_ptr<BoundStatement> Binder::bindCopyRelFrom(
    std::unique_ptr<CopyDescription> copyDescription, TableSchema* tableSchema) {
    // For table with SERIAL columns, we need to read in serial from files.
    auto containsSerial = bindContainsSerial(tableSchema);
    auto columns = bindCopyRelColumns(tableSchema);
    auto nodeOffset =
        createVariable(std::string(Property::OFFSET_NAME), LogicalType{LogicalTypeID::INT64});
    auto boundOffset = createVariable(
        std::string(Property::REL_BOUND_OFFSET_NAME), LogicalType{LogicalTypeID::ARROW_COLUMN});
    auto nbrOffset = createVariable(
        std::string(Property::REL_NBR_OFFSET_NAME), LogicalType{LogicalTypeID::ARROW_COLUMN});
    auto boundCopyFromInfo =
        std::make_unique<BoundCopyFromInfo>(std::move(copyDescription), tableSchema,
            std::move(columns), std::move(nodeOffset), boundOffset, nbrOffset, containsSerial);
    return std::make_unique<BoundCopyFrom>(std::move(boundCopyFromInfo));
}

static bool skipPropertyInFile(const Property& property) {
    return property.getDataType()->getLogicalTypeID() == LogicalTypeID::SERIAL ||
           TableSchema::isReservedPropertyName(property.getName());
}

expression_vector Binder::bindCopyNodeColumns(
    TableSchema* tableSchema, CopyDescription::FileType fileType) {
    expression_vector columnExpressions;
    auto isCopyCSV = fileType == CopyDescription::FileType::CSV;
    for (auto& property : tableSchema->properties) {
        if (skipPropertyInFile(*property)) {
            continue;
        }
        auto dataType =
            isCopyCSV ? *property->getDataType() : LogicalType{LogicalTypeID::ARROW_COLUMN};
        columnExpressions.push_back(createVariable(property->getName(), dataType));
    }
    return columnExpressions;
}

expression_vector Binder::bindCopyRelColumns(TableSchema* tableSchema) {
    expression_vector columnExpressions;
    columnExpressions.push_back(createVariable(
        std::string(Property::REL_FROM_PROPERTY_NAME), LogicalType{LogicalTypeID::ARROW_COLUMN}));
    columnExpressions.push_back(createVariable(
        std::string(Property::REL_TO_PROPERTY_NAME), LogicalType{LogicalTypeID::ARROW_COLUMN}));
    for (auto& property : tableSchema->properties) {
        if (skipPropertyInFile(*property)) {
            continue;
        }
        columnExpressions.push_back(
            createVariable(property->getName(), LogicalType{LogicalTypeID::ARROW_COLUMN}));
    }
    return columnExpressions;
}

std::unique_ptr<CSVReaderConfig> Binder::bindParsingOptions(
    const std::unordered_map<std::string, std::unique_ptr<ParsedExpression>>& parsingOptions) {
    auto csvReaderConfig = std::make_unique<CSVReaderConfig>();
    for (auto& parsingOption : parsingOptions) {
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

} // namespace binder
} // namespace kuzu

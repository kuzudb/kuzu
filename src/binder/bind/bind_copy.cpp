#include "binder/binder.h"
#include "binder/copy/bound_copy_from.h"
#include "binder/copy/bound_copy_to.h"
#include "binder/expression/literal_expression.h"
#include "common/string_utils.h"
#include "parser/copy.h"

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
    return std::make_unique<BoundCopyTo>(
        CopyDescription(std::vector<std::string>{boundFilePath}, fileType, columnNames),
        std::move(query));
}

std::unique_ptr<BoundStatement> Binder::bindCopyFromClause(const Statement& statement) {
    auto& copyStatement = (CopyFrom&)statement;
    auto catalogContent = catalog.getReadOnlyVersion();
    auto tableName = copyStatement.getTableName();
    validateTableExist(catalog, tableName);
    auto tableID = catalogContent->getTableID(tableName);
    auto csvReaderConfig = bindParsingOptions(copyStatement.getParsingOptions());
    auto boundFilePaths = bindFilePaths(copyStatement.getFilePaths());
    auto actualFileType = bindFileType(boundFilePaths);
    auto expectedFileType = copyStatement.getFileType();
    if (expectedFileType == CopyDescription::FileType::UNKNOWN &&
        actualFileType == CopyDescription::FileType::NPY) {
        throw BinderException("Please use COPY FROM BY COLUMN statement for copying npy files.");
    }
    if (expectedFileType == CopyDescription::FileType::NPY && actualFileType != expectedFileType) {
        throw BinderException("Please use COPY FROM statement for copying csv and parquet files.");
    }
    if (actualFileType == CopyDescription::FileType::NPY) {
        auto tableSchema = catalogContent->getTableSchema(tableID);
        if (tableSchema->properties.size() != boundFilePaths.size()) {
            throw BinderException(StringUtils::string_format(
                "Number of npy files is not equal to number of properties in table {}.",
                tableSchema->tableName));
        }
    }
    return std::make_unique<BoundCopyFrom>(
        CopyDescription(boundFilePaths, actualFileType, csvReaderConfig), tableID, tableName);
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

CSVReaderConfig Binder::bindParsingOptions(
    const std::unordered_map<std::string, std::unique_ptr<ParsedExpression>>* parsingOptions) {
    CSVReaderConfig csvReaderConfig;
    for (auto& parsingOption : *parsingOptions) {
        auto copyOptionName = parsingOption.first;
        StringUtils::toUpper(copyOptionName);
        bool isValidStringParsingOption = validateStringParsingOptionName(copyOptionName);
        auto copyOptionExpression = parsingOption.second.get();
        auto boundCopyOptionExpression = expressionBinder.bindExpression(*copyOptionExpression);
        assert(boundCopyOptionExpression->expressionType = LITERAL);
        if (copyOptionName == "HEADER") {
            if (boundCopyOptionExpression->dataType.getLogicalTypeID() != LogicalTypeID::BOOL) {
                throw BinderException(
                    "The value type of parsing csv option " + copyOptionName + " must be boolean.");
            }
            csvReaderConfig.hasHeader =
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
            bindStringParsingOptions(csvReaderConfig, copyOptionName, copyOptionValue);
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

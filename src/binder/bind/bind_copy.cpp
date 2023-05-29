#include "binder/binder.h"
#include "binder/copy/bound_copy.h"
#include "binder/expression/literal_expression.h"
#include "common/string_utils.h"
#include "parser/copy.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCopyClause(const Statement& statement) {
    auto& copyCSV = (Copy&)statement;
    auto catalogContent = catalog.getReadOnlyVersion();
    auto tableName = copyCSV.getTableName();
    validateTableExist(catalog, tableName);
    auto tableID = catalogContent->getTableID(tableName);
    auto csvReaderConfig = bindParsingOptions(copyCSV.getParsingOptions());
    auto boundFilePaths = bindFilePaths(copyCSV.getFilePaths());
    auto actualFileType = bindFileType(boundFilePaths);
    auto expectedFileType = copyCSV.getFileType();
    if (expectedFileType == common::CopyDescription::FileType::UNKNOWN &&
        actualFileType == common::CopyDescription::FileType::NPY) {
        throw BinderException("Please use COPY FROM BY COLUMN statement for copying npy files.");
    }
    if (expectedFileType == common::CopyDescription::FileType::NPY &&
        actualFileType != expectedFileType) {
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
    return std::make_unique<BoundCopy>(
        CopyDescription(boundFilePaths, csvReaderConfig, actualFileType), tableID, tableName);
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

CopyDescription::FileType Binder::bindFileType(std::vector<std::string> filePaths) {
    // We currently only support loading from files with the same type. Loading files with different
    // types is not supported.
    auto fileName = filePaths[0];
    auto csvSuffix = CopyDescription::getFileTypeSuffix(CopyDescription::FileType::CSV);
    auto parquetSuffix = CopyDescription::getFileTypeSuffix(CopyDescription::FileType::PARQUET);
    auto npySuffix = CopyDescription::getFileTypeSuffix(CopyDescription::FileType::NPY);
    CopyDescription::FileType fileType;
    std::string expectedSuffix;
    if (fileName.ends_with(csvSuffix)) {
        fileType = CopyDescription::FileType::CSV;
        expectedSuffix = csvSuffix;
    } else if (fileName.ends_with(parquetSuffix)) {
        fileType = CopyDescription::FileType::PARQUET;
        expectedSuffix = parquetSuffix;
    } else if (fileName.ends_with(npySuffix)) {
        fileType = CopyDescription::FileType::NPY;
        expectedSuffix = npySuffix;
    } else {
        throw CopyException("Unsupported file type: " + fileName);
    }
    for (auto& path : filePaths) {
        if (!path.ends_with(expectedSuffix)) {
            throw CopyException("Loading files with different types is not currently supported.");
        }
    }
    return fileType;
}

} // namespace binder
} // namespace kuzu

#include "binder/binder.h"
#include "binder/copy/bound_copy.h"
#include "binder/expression/literal_expression.h"
#include "parser/copy_csv/copy_csv.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCopy(const Statement& statement) {
    auto& copyCSV = (CopyCSV&)statement;
    auto catalogContent = catalog.getReadOnlyVersion();
    auto tableName = copyCSV.getTableName();
    validateTableExist(catalog, tableName);
    auto tableID = catalogContent->getTableID(tableName);
    auto csvReaderConfig = bindParsingOptions(copyCSV.getParsingOptions());
    auto filePaths = bindFilePaths(copyCSV.getFilePaths());
    auto fileType = bindFileType(filePaths);
    return make_unique<BoundCopy>(
        CopyDescription(filePaths, csvReaderConfig, fileType), tableID, tableName);
}

std::vector<std::string> Binder::bindFilePaths(const std::vector<std::string>& filePaths) {
    std::vector<std::string> boundFilePaths;
    for (auto& filePath : filePaths) {
        auto globbedFilePaths = FileUtils::globFilePath(filePath);
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
        bool isValidStringParsingOption = validateStringParsingOptionName(copyOptionName);
        auto copyOptionExpression = parsingOption.second.get();
        auto boundCopyOptionExpression = expressionBinder.bindExpression(*copyOptionExpression);
        assert(boundCopyOptionExpression->expressionType = LITERAL);
        if (copyOptionName == "HEADER") {
            if (boundCopyOptionExpression->dataType.typeID != BOOL) {
                throw BinderException(
                    "The value type of parsing csv option " + copyOptionName + " must be boolean.");
            }
            csvReaderConfig.hasHeader =
                ((LiteralExpression&)(*boundCopyOptionExpression)).value->getValue<bool>();
        } else if (boundCopyOptionExpression->dataType.typeID == STRING &&
                   isValidStringParsingOption) {
            if (boundCopyOptionExpression->dataType.typeID != STRING) {
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
    if (fileName.ends_with(csvSuffix)) {
        return CopyDescription::FileType::CSV;
    } else if (fileName.ends_with(parquetSuffix)) {
        return CopyDescription::FileType::PARQUET;
    } else {
        throw CopyException("Unsupported file type: " + fileName);
    }
}

} // namespace binder
} // namespace kuzu

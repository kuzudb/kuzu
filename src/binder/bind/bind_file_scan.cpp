#include "binder/binder.h"
#include "binder/copy/bound_file_scan_info.h"
#include "binder/expression/literal_expression.h"
#include "common/exception/binder.h"
#include "common/exception/copy.h"
#include "common/string_utils.h"

using namespace kuzu::parser;
using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace binder {

/*
 * Bind file.
 */
FileType Binder::bindFileType(const std::string& filePath) {
    std::filesystem::path fileName(filePath);
    auto extension = FileUtils::getFileExtension(fileName);
    auto fileType = FileTypeUtils::getFileTypeFromExtension(extension);
    if (fileType == FileType::UNKNOWN) {
        throw CopyException("Unsupported file type: " + filePath);
    }
    return fileType;
}

FileType Binder::bindFileType(const std::vector<std::string>& filePaths) {
    auto expectedFileType = FileType::UNKNOWN;
    for (auto& filePath : filePaths) {
        auto fileType = bindFileType(filePath);
        expectedFileType = (expectedFileType == FileType::UNKNOWN) ? fileType : expectedFileType;
        if (fileType != expectedFileType) {
            throw CopyException("Loading files with different types is not currently supported.");
        }
    }
    return expectedFileType;
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

/*
 * Bind parsing options.
 */
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

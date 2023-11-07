#include "binder/binder.h"
#include "binder/expression/literal_expression.h"
#include "common/exception/binder.h"
#include "common/exception/copy.h"
#include "common/string_format.h"
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
            throw BinderException{
                stringFormat("No file found that matches the pattern: {}.", filePath)};
        }
        boundFilePaths.insert(
            boundFilePaths.end(), globbedFilePaths.begin(), globbedFilePaths.end());
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
    if (value.length() < 1 || value.length() > 2 || (value.length() == 2 && value[0] != '\\')) {
        throw BinderException("Copy csv option value must be a single character with an "
                              "optional escape character.");
    }
    return value[value.length() - 1];
}

static void bindBoolParsingOption(
    CSVReaderConfig& csvReaderConfig, const std::string& optionName, bool optionValue) {
    if (optionName == "HEADER") {
        csvReaderConfig.hasHeader = optionValue;
    } else if (optionName == "PARALLEL") {
        csvReaderConfig.parallel = optionValue;
    }
}

static void bindStringParsingOption(
    CSVReaderConfig& csvReaderConfig, const std::string& optionName, std::string& optionValue) {
    auto parsingOptionValue = bindParsingOptionValue(optionValue);
    if (optionName == "ESCAPE") {
        csvReaderConfig.escapeChar = parsingOptionValue;
    } else if (optionName == "DELIM") {
        csvReaderConfig.delimiter = parsingOptionValue;
    } else if (optionName == "QUOTE") {
        csvReaderConfig.quoteChar = parsingOptionValue;
    }
}

template<uint64_t size>
static bool hasOption(const char* const (&arr)[size], const std::string& option) {
    return std::find(std::begin(arr), std::end(arr), option) != std::end(arr);
}

static bool validateBoolParsingOptionName(const std::string& parsingOptionName) {
    return hasOption(CopyConstants::BOOL_CSV_PARSING_OPTIONS, parsingOptionName);
}

static bool validateStringParsingOptionName(const std::string& parsingOptionName) {
    return hasOption(CopyConstants::STRING_CSV_PARSING_OPTIONS, parsingOptionName);
}

std::unique_ptr<CSVReaderConfig> Binder::bindParsingOptions(
    const std::unordered_map<std::string, std::unique_ptr<ParsedExpression>>& parsingOptions) {
    auto csvReaderConfig = std::make_unique<CSVReaderConfig>();
    for (auto& parsingOption : parsingOptions) {
        auto copyOptionName = parsingOption.first;
        StringUtils::toUpper(copyOptionName);

        bool isValidStringParsingOption = validateStringParsingOptionName(copyOptionName);
        bool isValidBoolParsingOption = validateBoolParsingOptionName(copyOptionName);

        auto copyOptionExpression = parsingOption.second.get();
        auto boundCopyOptionExpression = expressionBinder.bindExpression(*copyOptionExpression);
        KU_ASSERT(boundCopyOptionExpression->expressionType == ExpressionType::LITERAL);
        if (isValidBoolParsingOption) {
            if (boundCopyOptionExpression->dataType.getLogicalTypeID() != LogicalTypeID::BOOL) {
                throw BinderException(
                    "The type of csv parsing option " + copyOptionName + " must be a boolean.");
            }
            auto copyOptionValue =
                ((LiteralExpression&)(*boundCopyOptionExpression)).value->getValue<bool>();
            bindBoolParsingOption(*csvReaderConfig, copyOptionName, copyOptionValue);
        } else if (isValidStringParsingOption) {
            if (boundCopyOptionExpression->dataType.getLogicalTypeID() != LogicalTypeID::STRING) {
                throw BinderException(
                    "The type of csv parsing option " + copyOptionName + " must be a string.");
            }
            auto copyOptionValue =
                ((LiteralExpression&)(*boundCopyOptionExpression)).value->getValue<std::string>();
            bindStringParsingOption(*csvReaderConfig, copyOptionName, copyOptionValue);
        } else {
            throw BinderException("Unrecognized csv parsing option: " + copyOptionName + ".");
        }
    }
    return csvReaderConfig;
}

} // namespace binder
} // namespace kuzu

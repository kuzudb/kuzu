#include "binder/binder.h"
#include "binder/expression/literal_expression.h"
#include "common/exception/binder.h"
#include "common/exception/copy.h"
#include "common/file_system/virtual_file_system.h"
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
    auto extension = vfs->getFileExtension(fileName);
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
        auto globbedFilePaths = vfs->glob(clientContext, filePath);
        if (globbedFilePaths.empty()) {
            throw BinderException{
                stringFormat("No file found that matches the pattern: {}.", filePath)};
        }
        boundFilePaths.insert(
            boundFilePaths.end(), globbedFilePaths.begin(), globbedFilePaths.end());
    }
    return boundFilePaths;
}

std::unordered_map<std::string, Value> Binder::bindParsingOptions(
    const parser::parsing_option_t& parsingOptions) {
    std::unordered_map<std::string, Value> options;
    for (auto& option : parsingOptions) {
        auto name = option.first;
        common::StringUtils::toUpper(name);
        auto expr = expressionBinder.bindExpression(*option.second);
        KU_ASSERT(expr->expressionType == ExpressionType::LITERAL);
        auto literalExpr = ku_dynamic_cast<Expression*, LiteralExpression*>(expr.get());
        options.insert({name, *literalExpr->getValue()});
    }
    return options;
}

} // namespace binder
} // namespace kuzu

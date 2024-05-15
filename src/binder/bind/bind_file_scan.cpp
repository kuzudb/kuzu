#include "binder/binder.h"
#include "binder/bound_scan_source.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "common/exception/binder.h"
#include "common/exception/copy.h"
#include "common/file_system/virtual_file_system.h"
#include "common/string_format.h"
#include "common/string_utils.h"
#include "function/table/bind_input.h"
#include "parser/scan_source.h"

using namespace kuzu::parser;
using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace binder {

FileType Binder::bindFileType(const std::string& filePath) {
    std::filesystem::path fileName(filePath);
    auto extension = clientContext->getVFSUnsafe()->getFileExtension(fileName);
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
        auto globbedFilePaths = clientContext->getVFSUnsafe()->glob(clientContext, filePath);
        if (globbedFilePaths.empty()) {
            throw BinderException{
                stringFormat("No file found that matches the pattern: {}.", filePath)};
        }
        boundFilePaths.insert(boundFilePaths.end(), globbedFilePaths.begin(),
            globbedFilePaths.end());
    }
    return boundFilePaths;
}

std::unordered_map<std::string, Value> Binder::bindParsingOptions(
    const parser::options_t& parsingOptions) {
    std::unordered_map<std::string, Value> options;
    for (auto& option : parsingOptions) {
        auto name = option.first;
        common::StringUtils::toUpper(name);
        auto expr = expressionBinder.bindExpression(*option.second);
        KU_ASSERT(expr->expressionType == ExpressionType::LITERAL);
        auto literalExpr = ku_dynamic_cast<Expression*, LiteralExpression*>(expr.get());
        options.insert({name, literalExpr->getValue()});
    }
    return options;
}

std::unique_ptr<BoundBaseScanSource> Binder::bindScanSource(BaseScanSource* source,
    const options_t& options, const std::vector<std::string>& expectedColumnNames,
    const std::vector<LogicalType>& expectedColumnTypes) {
    switch (source->type) {
    case common::ScanSourceType::FILE: {
        auto fileSource = ku_dynamic_cast<BaseScanSource*, FileScanSource*>(source);
        // Bind path.
        auto filePaths = bindFilePaths(fileSource->filePaths);
        // Bind file type.
        auto fileType = bindFileType(filePaths);
        auto config = std::make_unique<ReaderConfig>(fileType, std::move(filePaths));
        // Bind options.
        config->options = bindParsingOptions(options);
        // Bind file scan function.
        auto func = getScanFunction(config->fileType, *config);
        auto bindInput = std::make_unique<ScanTableFuncBindInput>(config->copy(),
            std::move(expectedColumnNames), std::move(expectedColumnTypes), clientContext);
        auto bindData = func.bindFunc(clientContext, bindInput.get());
        // Bind input columns
        expression_vector inputColumns;
        for (auto i = 0u; i < bindData->columnTypes.size(); i++) {
            inputColumns.push_back(
                createVariable(bindData->columnNames[i], bindData->columnTypes[i]));
        }
        auto fileScanInfo = BoundFileScanInfo(func, std::move(bindData), inputColumns);
        return std::make_unique<BoundFileScanSource>(std::move(fileScanInfo));
    }
    case ScanSourceType::QUERY: {
        auto querySource = ku_dynamic_cast<BaseScanSource*, QueryScanSource*>(source);
        auto boundStatement = bind(*querySource->statement);
        // Bind input columns.
        auto columns = boundStatement->getStatementResult()->getColumns();
        if (columns.size() != expectedColumnNames.size()) {
            throw BinderException(
                stringFormat("Query returns {} columns but {} columns were expected.",
                    columns.size(), expectedColumnNames.size()));
        }
        for (auto i = 0u; i < columns.size(); ++i) {
            ExpressionUtil::validateDataType(*columns[i], expectedColumnTypes[i]);
            columns[i]->setAlias(expectedColumnNames[i]);
        }
        return std::make_unique<BoundQueryScanSource>(std::move(boundStatement));
    }
    case ScanSourceType::OBJECT: {
        auto objectSource = source->constPtrCast<ObjectScanSource>();
        auto objectStr = StringUtils::join(objectSource->objectNames, ",");
        throw BinderException(
            stringFormat("Scan from external object {} is not supported.", objectStr));
    }
    default:
        KU_UNREACHABLE;
    }
}

} // namespace binder
} // namespace kuzu

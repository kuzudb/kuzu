#include "binder/binder.h"
#include "binder/bound_scan_source.h"
#include "binder/expression/expression_util.h"
#include "binder/expression/literal_expression.h"
#include "common/exception/binder.h"
#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/file_system/virtual_file_system.h"
#include "common/string_format.h"
#include "common/string_utils.h"
#include "function/table/bind_input.h"
#include "main/database_manager.h"
#include "parser/scan_source.h"

using namespace kuzu::parser;
using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::function;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

FileTypeInfo bindSingleFileType(main::ClientContext* context, const std::string& filePath) {
    std::filesystem::path fileName(filePath);
    auto extension = context->getVFSUnsafe()->getFileExtension(fileName);
    return FileTypeInfo{FileTypeUtils::getFileTypeFromExtension(extension), extension.substr(1)};
}

FileTypeInfo Binder::bindFileTypeInfo(const std::vector<std::string>& filePaths) {
    auto expectedFileType = FileTypeInfo{FileType::UNKNOWN, "" /* fileTypeStr */};
    for (auto& filePath : filePaths) {
        auto fileType = bindSingleFileType(clientContext, filePath);
        expectedFileType =
            (expectedFileType.fileType == FileType::UNKNOWN) ? fileType : expectedFileType;
        if (fileType.fileType != expectedFileType.fileType) {
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
        for (auto& globbedPath : globbedFilePaths) {
            boundFilePaths.push_back(globbedPath);
        }
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
        auto literalExpr = ku_dynamic_cast<LiteralExpression*>(expr.get());
        options.insert({name, literalExpr->getValue()});
    }
    return options;
}

std::unique_ptr<BoundBaseScanSource> Binder::bindScanSource(BaseScanSource* source,
    const options_t& options, const std::vector<std::string>& columnNames,
    const std::vector<LogicalType>& columnTypes) {
    switch (source->type) {
    case common::ScanSourceType::FILE: {
        return bindFileScanSource(*source, options, columnNames, columnTypes);
    }
    case ScanSourceType::QUERY: {
        return bindQueryScanSource(*source, columnNames, columnTypes);
    }
    case ScanSourceType::OBJECT: {
        return bindObjectScanSource(*source, options, columnNames, columnTypes);
    }
    default:
        KU_UNREACHABLE;
    }
}

std::unique_ptr<BoundBaseScanSource> Binder::bindFileScanSource(const BaseScanSource& scanSource,
    const options_t& options, const std::vector<std::string>& columnNames,
    const std::vector<LogicalType>& columnTypes) {
    auto fileSource = scanSource.constPtrCast<FileScanSource>();
    auto filePaths = bindFilePaths(fileSource->filePaths);
    auto fileTypeInfo = bindFileTypeInfo(filePaths);
    auto config = std::make_unique<ReaderConfig>(std::move(fileTypeInfo), std::move(filePaths));
    config->options = bindParsingOptions(options);
    auto func = getScanFunction(config->fileTypeInfo, *config);
    auto bindInput = std::make_unique<ScanTableFuncBindInput>(config->copy(), columnNames,
        LogicalType::copy(columnTypes), clientContext, &func);
    auto bindData = func.bindFunc(clientContext, bindInput.get());
    expression_vector inputColumns;
    for (auto i = 0u; i < bindData->columnTypes.size(); i++) {
        inputColumns.push_back(createVariable(bindData->columnNames[i], bindData->columnTypes[i]));
    }

    auto info = BoundTableScanSourceInfo(func, std::move(bindData), std::move(inputColumns));
    return std::make_unique<BoundTableScanSource>(ScanSourceType::FILE, std::move(info));
}

std::unique_ptr<BoundBaseScanSource> Binder::bindQueryScanSource(const BaseScanSource& scanSource,
    const std::vector<std::string>& columnNames, const std::vector<LogicalType>& columnTypes) {
    auto querySource = scanSource.constPtrCast<QueryScanSource>();
    auto boundStatement = bind(*querySource->statement);
    auto columns = boundStatement->getStatementResult()->getColumns();
    if (columns.size() != columnNames.size()) {
        throw BinderException(stringFormat("Query returns {} columns but {} columns were expected.",
            columns.size(), columnNames.size()));
    }
    for (auto i = 0u; i < columns.size(); ++i) {
        ExpressionUtil::validateDataType(*columns[i], columnTypes[i]);
        columns[i]->setAlias(columnNames[i]);
    }
    return std::make_unique<BoundQueryScanSource>(std::move(boundStatement));
}

static TableFunction getObjectScanFunc(const std::string& dbName, const std::string& tableName,
    main::ClientContext* clientContext) {
    // Bind external database table
    auto attachedDB = clientContext->getDatabaseManager()->getAttachedDatabase(dbName);
    if (attachedDB == nullptr) {
        throw BinderException{stringFormat("No database named {} has been attached.", dbName)};
    }
    auto attachedCatalog = attachedDB->getCatalog();
    auto tableID = attachedCatalog->getTableID(clientContext->getTx(), tableName);
    auto entry = attachedCatalog->getTableCatalogEntry(clientContext->getTx(), tableID);
    return entry->ptrCast<TableCatalogEntry>()->getScanFunction();
}

std::unique_ptr<BoundBaseScanSource> Binder::bindObjectScanSource(const BaseScanSource& scanSource,
    const parser::options_t& options, const std::vector<std::string>& columnNames,
    const std::vector<common::LogicalType>& columnTypes) {
    auto objectSource = scanSource.constPtrCast<ObjectScanSource>();
    TableFunction func;
    std::unique_ptr<TableFuncBindData> bindData;
    std::string objectName;
    if (objectSource->objectNames.size() == 1) {
        // Bind external object as table
        objectName = objectSource->objectNames[0];
        auto replacementData = clientContext->tryReplace(objectName);
        if (replacementData != nullptr) { // Replace as
            func = replacementData->func;
            replacementData->bindInput.config.options = bindParsingOptions(options);
            bindData = func.bindFunc(clientContext, &replacementData->bindInput);
        } else if (clientContext->getDatabaseManager()->hasDefaultDatabase()) {
            auto dbName = clientContext->getDatabaseManager()->getDefaultDatabase();
            func = getObjectScanFunc(dbName, objectSource->objectNames[0], clientContext);
            auto bindInput = function::ScanTableFuncBindInput();
            bindData = func.bindFunc(clientContext, &bindInput);
        } else {
            throw BinderException(ExceptionMessage::variableNotInScope(objectName));
        }
    } else if (objectSource->objectNames.size() == 2) {
        // Bind external database table
        objectName = objectSource->objectNames[0] + "." + objectSource->objectNames[1];
        func = getObjectScanFunc(objectSource->objectNames[0], objectSource->objectNames[1],
            clientContext);
        auto bindInput = function::ScanTableFuncBindInput();
        bindData = func.bindFunc(clientContext, &bindInput);
    } else {
        // LCOV_EXCL_START
        throw BinderException(stringFormat("Cannot find object {}.",
            StringUtils::join(objectSource->objectNames, ",")));
        // LCOV_EXCL_STOP
    }
    expression_vector columns;
    if (columnTypes.empty()) {
        for (auto i = 0u; i < bindData->columnTypes.size(); i++) {
            columns.push_back(createVariable(bindData->columnNames[i], bindData->columnTypes[i]));
        }
    } else {
        if (bindData->columnTypes.size() != columnTypes.size()) {
            throw BinderException(stringFormat("{} has {} columns but {} columns were expected.",
                objectName, bindData->columnTypes.size(), columnTypes.size()));
        }
        for (auto i = 0u; i < bindData->columnTypes.size(); ++i) {
            columns.push_back(createVariable(columnNames[i], bindData->columnTypes[i]));
        }
    }
    auto info = BoundTableScanSourceInfo(func, std::move(bindData), columns);
    return std::make_unique<BoundTableScanSource>(ScanSourceType::OBJECT, std::move(info));
}

} // namespace binder
} // namespace kuzu

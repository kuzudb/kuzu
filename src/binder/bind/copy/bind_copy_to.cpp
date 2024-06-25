#include "binder/binder.h"
#include "binder/copy/bound_copy_to.h"
#include "catalog/catalog.h"
#include "common/exception/binder.h"
#include "function/built_in_function_utils.h"
#include "main/client_context.h"
#include "parser/copy.h"

using namespace kuzu::common;
using namespace kuzu::parser;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindCopyToClause(const Statement& statement) {
    auto& copyToStatement = statement.constCast<CopyTo>();
    auto boundFilePath = copyToStatement.getFilePath();
    auto fileType = bindFileType(boundFilePath);
    std::vector<std::string> columnNames;
    auto parsedQuery = copyToStatement.getStatement()->constPtrCast<RegularQuery>();
    auto query = bindQuery(*parsedQuery);
    auto columns = query->getStatementResult()->getColumns();
    auto functions = clientContext->getCatalog()->getFunctions(clientContext->getTx());
    auto fileTypeStr = common::FileTypeUtils::toString(fileType);
    auto name = common::stringFormat("COPY_{}", fileTypeStr);
    auto exportFunc =
        function::BuiltInFunctionsUtils::matchFunction(clientContext->getTx(), name, functions)
            ->constPtrCast<function::ExportFunction>();
    for (auto& column : columns) {
        auto columnName = column->hasAlias() ? column->getAlias() : column->toString();
        columnNames.push_back(columnName);
    }
    if (fileType != FileType::CSV && copyToStatement.getParsingOptionsRef().size() != 0) {
        throw BinderException{"Only copy to csv can have options."};
    }
    function::ExportFuncBindInput bindInput{std::move(columnNames), std::move(boundFilePath),
        bindParsingOptions(copyToStatement.getParsingOptionsRef())};
    auto bindData = exportFunc->bind(bindInput);
    return std::make_unique<BoundCopyTo>(std::move(bindData), *exportFunc, std::move(query));
}

} // namespace binder
} // namespace kuzu

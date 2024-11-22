#include "binder/binder.h"
#include "binder/bound_standalone_call_function.h"
#include "catalog/catalog.h"
#include "common/exception/binder.h"
#include "function/built_in_function_utils.h"
#include "main/client_context.h"
#include "parser/expression/parsed_function_expression.h"
#include "parser/standalone_call_function.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindStandaloneCallFunction(
    const parser::Statement& statement) {
    auto& callStatement = statement.constCast<parser::StandaloneCallFunction>();
    auto& funcExpr =
        callStatement.getFunctionExpression()->constCast<parser::ParsedFunctionExpression>();
    auto funcName = funcExpr.getFunctionName();
    auto catalogSet = clientContext->getCatalog()->getFunctions(clientContext->getTx());
    auto entry = function::BuiltInFunctionsUtils::getFunctionCatalogEntry(clientContext->getTx(),
        funcName, catalogSet);
    if (entry->getType() != catalog::CatalogEntryType::STANDALONE_TABLE_FUNCTION_ENTRY) {
        throw common::BinderException(
            "Only standalone table functions can be called without return statement.");
    }
    expression_vector columns;
    auto boundTableFunction = bindTableFunc(funcName, funcExpr, columns);
    return std::make_unique<BoundStandaloneCallFunction>(std::move(boundTableFunction));
}

} // namespace binder
} // namespace kuzu

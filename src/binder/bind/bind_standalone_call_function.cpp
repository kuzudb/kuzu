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
    auto bindInput = function::ScanTableFuncBindInput();
    // TODO(Ziyi): We currently doesn't have any standalone call function that takes parameters.
    KU_ASSERT(funcExpr.getNumChildren() == 0);
    auto func = function::BuiltInFunctionsUtils::matchFunction(funcName, {}, entry);
    auto tableFunc = func->constPtrCast<function::TableFunction>();
    auto bindData = tableFunc->bindFunc(clientContext, &bindInput);
    auto offset = expressionBinder.createVariableExpression(LogicalType::INT64(),
        std::string(InternalKeyword::ROW_OFFSET));
    BoundTableFunction boundTableFunction{*tableFunc, std::move(bindData), std::move(offset)};
    return std::make_unique<BoundStandaloneCallFunction>(std::move(boundTableFunction));
}

} // namespace binder
} // namespace kuzu

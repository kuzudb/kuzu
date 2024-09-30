#include "binder/binder.h"
#include "binder/bound_void_function_call.h"
#include "catalog/catalog.h"
#include "function/built_in_function_utils.h"
#include "main/client_context.h"
#include "parser/void_function_call.h"

namespace kuzu {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindVoidFunctionCall(const parser::Statement& statement) {
    const auto& call = common::ku_dynamic_cast<const parser::VoidFunctionCall&>(statement);
    auto functionName = call.getFunctionName();
    auto catalogSet = clientContext->getCatalog()->getFunctions(clientContext->getTx());
    const auto* func = function::BuiltInFunctionsUtils::matchFunction(clientContext->getTx(),
        functionName, catalogSet)
                           ->constPtrCast<function::TableFunction>();
    auto bindInput = function::ScanTableFuncBindInput();
    auto bindData = func->bindFunc(clientContext, &bindInput);
    return std::make_unique<BoundVoidFunctionCall>(*func, std::move(bindData));
}

} // namespace binder
} // namespace kuzu

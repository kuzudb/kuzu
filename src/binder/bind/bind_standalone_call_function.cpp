#include "binder/binder.h"
#include "binder/bound_standalone_call_function.h"
#include "catalog/catalog.h"
#include "common/exception/binder.h"
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
    auto entry = clientContext->getCatalog()->getFunctionEntry(clientContext->getTransaction(),
        funcName, clientContext->useInternalCatalogEntry());
    KU_ASSERT(entry);
    if (entry->getType() != catalog::CatalogEntryType::STANDALONE_TABLE_FUNCTION_ENTRY) {
        throw BinderException(
            "Only standalone table functions can be called without return statement.");
    }
    auto boundTableFunction = bindTableFunc(funcName, funcExpr, {} /* yieldVariables */);
    return std::make_unique<BoundStandaloneCallFunction>(std::move(boundTableFunction));
}

} // namespace binder
} // namespace kuzu

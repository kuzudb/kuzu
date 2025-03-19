#include "parser/visitor/standalone_call_rewriter.h"

#include "binder/binder.h"
#include "binder/bound_standalone_call_function.h"
#include "catalog/catalog.h"
#include "common/exception/parser.h"
#include "main/client_context.h"
#include "parser/expression/parsed_function_expression.h"
#include "parser/standalone_call_function.h"

namespace kuzu {
namespace parser {

std::string StandaloneCallRewriter::getRewriteQuery(const Statement& statement) {
    visit(statement);
    return rewriteQuery;
}

void StandaloneCallRewriter::visitStandaloneCallFunction(const Statement& statement) {
    auto& standaloneCallFunc = statement.constCast<StandaloneCallFunction>();
    main::ClientContext::TransactionHelper::runFuncInTransaction(
        *context->getTransactionContext(),
        [&]() -> void {
            auto funcName = standaloneCallFunc.getFunctionExpression()
                                ->constPtrCast<parser::ParsedFunctionExpression>()
                                ->getFunctionName();
            if (!context->getCatalog()->containsFunction(context->getTransaction(), funcName) &&
                !singleStatement) {
                throw common::ParserException{
                    funcName + " must be called in a query which doesn't have other statements."};
            }
            binder::Binder binder{context};
            const auto boundStatement = binder.bind(standaloneCallFunc);
            auto& boundStandaloneCall =
                boundStatement->constCast<binder::BoundStandaloneCallFunction>();
            const auto func =
                boundStandaloneCall.getTableFunction().constPtrCast<function::TableFunction>();
            if (func->rewriteFunc) {
                rewriteQuery = func->rewriteFunc(*context, *boundStandaloneCall.getBindData());
            }
        },
        true /*readOnlyStatement*/, false /*isTransactionStatement*/,
        main::ClientContext::TransactionHelper::TransactionCommitAction::COMMIT_IF_NEW);
}

} // namespace parser
} // namespace kuzu

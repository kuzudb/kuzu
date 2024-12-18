#include "parser/visitor/standalone_call_rewriter.h"

#include "binder/binder.h"
#include "binder/bound_standalone_call_function.h"
#include "binder/query/reading_clause/bound_table_function_call.h"
#include "parser/query/reading_clause/in_query_call_clause.h"
#include "parser/standalone_call_function.h"

namespace kuzu {
namespace parser {

std::string StandaloneCallRewriter::getRewriteQuery(const Statement& statement) {
    visit(statement);
    return rewriteQuery;
}

void StandaloneCallRewriter::rewrite(const function::TableFunction& tableFunc,
    const function::TableFuncBindData& bindData) {
    if (tableFunc.rewriteFunc) {
        rewriteQuery = tableFunc.rewriteFunc(*context, bindData);
    }
}

void StandaloneCallRewriter::visitStandaloneCallFunction(const Statement& statement) {
    auto& standaloneCallFunc = statement.constCast<StandaloneCallFunction>();
    binder::Binder binder{context};
    auto boundStatement = binder.bind(standaloneCallFunc);
    auto& boundStandaloneCall = boundStatement->constCast<binder::BoundStandaloneCallFunction>();
    auto func = boundStandaloneCall.getTableFunction().constPtrCast<function::TableFunction>();
    rewrite(*func, *boundStandaloneCall.getBindData());
}

void StandaloneCallRewriter::visitInQueryCall(const ReadingClause* readingClause) {
    if (readingClause->getClauseType() != common::ClauseType::IN_QUERY_CALL) {
        return;
    }
    auto& inQueryCall = readingClause->constCast<InQueryCallClause>();
    binder::Binder binder{context};
    auto boundInQueryCall = binder.bindInQueryCall(inQueryCall);
    if (boundInQueryCall->getClauseType() != common::ClauseType::TABLE_FUNCTION_CALL) {
        return;
    }
    auto& boundTableFuncCall = boundInQueryCall->constCast<binder::BoundTableFunctionCall>();
    rewrite(boundTableFuncCall.getTableFunc(), *boundTableFuncCall.getBindData());
}

} // namespace parser
} // namespace kuzu

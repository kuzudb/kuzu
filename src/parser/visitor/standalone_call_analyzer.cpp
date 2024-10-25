#include "parser/visitor/standalone_call_analyzer.h"

#include "binder/binder.h"
#include "binder/bound_standalone_call_function.h"
#include "parser/standalone_call_function.h"

namespace kuzu {
namespace parser {

std::string StandaloneCallAnalyzer::getRewriteQuery(const Statement& statement) {
    visit(statement);
    return rewriteQuery;
}

void StandaloneCallAnalyzer::visitStandaloneCallFunction(const Statement& statement) {
    auto& standaloneCallFunc = statement.constCast<StandaloneCallFunction>();
    binder::Binder binder{context};
    auto boundStatement = binder.bind(standaloneCallFunc);
    auto& boundStandaloneCall = boundStatement->constCast<binder::BoundStandaloneCallFunction>();
    auto func = boundStandaloneCall.getTableFunction().constPtrCast<function::TableFunction>();
    if (func->rewriteFunc) {
        rewriteQuery = func->rewriteFunc(*context, *boundStandaloneCall.getBindData());
    }
}

} // namespace parser
} // namespace kuzu

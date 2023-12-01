#include "binder/query/reading_clause/bound_in_query_call.h"
#include "planner/operator/logical_in_query_call.h"
#include "planner/query_planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void QueryPlanner::appendInQueryCall(
    const BoundReadingClause& boundReadingClause, LogicalPlan& plan) {
    auto& boundInQueryCall = (BoundInQueryCall&)boundReadingClause;
    auto logicalInQueryCall = make_shared<LogicalInQueryCall>(boundInQueryCall.getTableFunc(),
        boundInQueryCall.getBindData()->copy(), boundInQueryCall.getOutputExpressions(),
        boundInQueryCall.getRowIdxExpression());
    logicalInQueryCall->computeFactorizedSchema();
    plan.setLastOperator(logicalInQueryCall);
}

} // namespace planner
} // namespace kuzu

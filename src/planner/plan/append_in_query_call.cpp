#include "planner/logical_plan/logical_operator/logical_in_query_call.h"
#include "planner/query_planner.h"

namespace kuzu {
namespace planner {

void QueryPlanner::appendInQueryCall(BoundInQueryCall& boundInQueryCall, LogicalPlan& plan) {
    auto logicalInQueryCall = make_shared<LogicalInQueryCall>(boundInQueryCall.getTableFunc(),
        boundInQueryCall.getBindData()->copy(), boundInQueryCall.getOutputExpressions());
    logicalInQueryCall->computeFactorizedSchema();
    plan.setLastOperator(logicalInQueryCall);
}

} // namespace planner
} // namespace kuzu

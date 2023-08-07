#include "planner/logical_plan/logical_operator/logical_unwind.h"
#include "planner/query_planner.h"

namespace kuzu {
namespace planner {

void QueryPlanner::appendUnwind(BoundUnwindClause& boundUnwindClause, LogicalPlan& plan) {
    auto unwind = make_shared<LogicalUnwind>(boundUnwindClause.getExpression(),
        boundUnwindClause.getAliasExpression(), plan.getLastOperator());
    QueryPlanner::appendFlattens(unwind->getGroupsPosToFlatten(), plan);
    unwind->setChild(0, plan.getLastOperator());
    unwind->computeFactorizedSchema();
    plan.setLastOperator(unwind);
}

} // namespace planner
} // namespace kuzu

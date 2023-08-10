#include "planner/logical_plan/logical_order_by.h"
#include "planner/query_planner.h"

namespace kuzu {
namespace planner {

void QueryPlanner::appendOrderBy(
    const expression_vector& expressions, const std::vector<bool>& isAscOrders, LogicalPlan& plan) {
    auto orderBy = make_shared<LogicalOrderBy>(expressions, isAscOrders, plan.getLastOperator());
    appendFlattens(orderBy->getGroupsPosToFlatten(), plan);
    orderBy->setChild(0, plan.getLastOperator());
    orderBy->computeFactorizedSchema();
    plan.setLastOperator(std::move(orderBy));
}

} // namespace planner
} // namespace kuzu

#include "planner/logical_plan/logical_aggregate.h"
#include "planner/query_planner.h"

namespace kuzu {
namespace planner {

void QueryPlanner::appendAggregate(const expression_vector& expressionsToGroupBy,
    const expression_vector& expressionsToAggregate, LogicalPlan& plan) {
    auto aggregate = make_shared<LogicalAggregate>(
        expressionsToGroupBy, expressionsToAggregate, plan.getLastOperator());
    appendFlattens(aggregate->getGroupsPosToFlattenForGroupBy(), plan);
    aggregate->setChild(0, plan.getLastOperator());
    appendFlattens(aggregate->getGroupsPosToFlattenForAggregate(), plan);
    aggregate->setChild(0, plan.getLastOperator());
    aggregate->computeFactorizedSchema();
    plan.setLastOperator(std::move(aggregate));
}

} // namespace planner
} // namespace kuzu

#include "planner/operator/logical_flatten.h"
#include "planner/query_planner.h"

namespace kuzu {
namespace planner {

void QueryPlanner::appendFlattens(const f_group_pos_set& groupsPos, LogicalPlan& plan) {
    for (auto groupPos : groupsPos) {
        appendFlattenIfNecessary(groupPos, plan);
    }
}

void QueryPlanner::appendFlattenIfNecessary(f_group_pos groupPos, LogicalPlan& plan) {
    auto group = plan.getSchema()->getGroup(groupPos);
    if (group->isFlat()) {
        return;
    }
    auto flatten = make_shared<LogicalFlatten>(groupPos, plan.getLastOperator());
    flatten->computeFactorizedSchema();
    // update cardinality
    plan.setCardinality(cardinalityEstimator->estimateFlatten(plan, groupPos));
    plan.setLastOperator(std::move(flatten));
}

} // namespace planner
} // namespace kuzu

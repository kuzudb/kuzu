#include "planner/logical_plan/logical_operator/logical_accumulate.h"
#include "planner/query_planner.h"

namespace kuzu {
namespace planner {

void QueryPlanner::appendAccumulate(common::AccumulateType accumulateType, LogicalPlan& plan) {
    auto op = make_shared<LogicalAccumulate>(accumulateType, plan.getLastOperator());
    op->computeFactorizedSchema();
    plan.setLastOperator(op);
}

} // namespace planner
} // namespace kuzu

#include "planner/logical_plan/logical_accumulate.h"
#include "planner/query_planner.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

void QueryPlanner::appendAccumulate(AccumulateType accumulateType, LogicalPlan& plan) {
    auto op = make_shared<LogicalAccumulate>(accumulateType, plan.getLastOperator());
    op->computeFactorizedSchema();
    plan.setLastOperator(op);
}

} // namespace planner
} // namespace kuzu

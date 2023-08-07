#include "planner/logical_plan/logical_limit.h"
#include "planner/query_planner.h"

namespace kuzu {
namespace planner {

void QueryPlanner::appendLimit(uint64_t limitNumber, LogicalPlan& plan) {
    auto limit = make_shared<LogicalLimit>(limitNumber, plan.getLastOperator());
    appendFlattens(limit->getGroupsPosToFlatten(), plan);
    limit->setChild(0, plan.getLastOperator());
    limit->computeFactorizedSchema();
    plan.setCardinality(limitNumber);
    plan.setLastOperator(std::move(limit));
}

} // namespace planner
} // namespace kuzu

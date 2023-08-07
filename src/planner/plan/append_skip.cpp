#include "planner/logical_plan/logical_skip.h"
#include "planner/query_planner.h"

namespace kuzu {
namespace planner {

void QueryPlanner::appendSkip(uint64_t skipNumber, LogicalPlan& plan) {
    auto skip = make_shared<LogicalSkip>(skipNumber, plan.getLastOperator());
    appendFlattens(skip->getGroupsPosToFlatten(), plan);
    skip->setChild(0, plan.getLastOperator());
    skip->computeFactorizedSchema();
    plan.setCardinality(plan.getCardinality() - skipNumber);
    plan.setLastOperator(std::move(skip));
}

} // namespace planner
} // namespace kuzu

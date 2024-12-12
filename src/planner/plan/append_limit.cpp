#include "planner/operator/logical_limit.h"
#include "planner/planner.h"

namespace kuzu {
namespace planner {

void Planner::appendLimit(uint64_t skipNum, uint64_t limitNum, LogicalPlan& plan) {
    auto limit = make_shared<LogicalLimit>(skipNum, limitNum, plan.getLastOperator());
    appendFlattens(limit->getGroupsPosToFlatten(), plan);
    limit->setChild(0, plan.getLastOperator());
    limit->computeFactorizedSchema();
    plan.setLastOperator(std::move(limit));
}

} // namespace planner
} // namespace kuzu

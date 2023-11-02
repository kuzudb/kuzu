#include "planner/operator/logical_accumulate.h"
#include "planner/query_planner.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

void QueryPlanner::appendAccumulate(AccumulateType accumulateType,
    const expression_vector& expressionsToFlatten, LogicalPlan& plan) {
    auto op = make_shared<LogicalAccumulate>(
        accumulateType, expressionsToFlatten, plan.getLastOperator());
    appendFlattens(op->getGroupPositionsToFlatten(), plan);
    op->computeFactorizedSchema();
    plan.setLastOperator(std::move(op));
}

} // namespace planner
} // namespace kuzu

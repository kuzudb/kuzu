#include "planner/operator/logical_accumulate.h"
#include "planner/planner.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

void Planner::appendAccumulate(AccumulateType accumulateType, LogicalPlan& plan) {
    appendAccumulate(accumulateType, expression_vector{}, plan);
}

void Planner::appendAccumulate(AccumulateType accumulateType, const expression_vector& flatExprs,
    LogicalPlan& plan) {
    appendAccumulate(accumulateType, flatExprs, nullptr, plan);
}

void Planner::appendAccumulate(AccumulateType accumulateType, const expression_vector& flatExprs,
    std::shared_ptr<Expression> offset, LogicalPlan& plan) {
    auto op =
        make_shared<LogicalAccumulate>(accumulateType, flatExprs, offset, plan.getLastOperator());
    appendFlattens(op->getGroupPositionsToFlatten(), plan);
    op->setChild(0, plan.getLastOperator());
    op->computeFactorizedSchema();
    plan.setLastOperator(std::move(op));
}

} // namespace planner
} // namespace kuzu

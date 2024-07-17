#include "planner/operator/logical_accumulate.h"
#include "planner/planner.h"

using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace planner {

void Planner::appendAccumulate(LogicalPlan& plan) {
    appendAccumulate(AccumulateType::REGULAR, expression_vector{}, nullptr /* offset */,
        nullptr /* mark */, plan);
}

void Planner::appendOptionalAccumulate(std::shared_ptr<Expression> mark, LogicalPlan& plan) {
    appendAccumulate(AccumulateType::OPTIONAL_, expression_vector{}, nullptr /* offset */, mark,
        plan);
}

void Planner::appendAccumulate(const expression_vector& flatExprs, LogicalPlan& plan) {
    appendAccumulate(AccumulateType::REGULAR, flatExprs, nullptr /* offset */, nullptr /* mark */,
        plan);
}

void Planner::appendAccumulate(AccumulateType accumulateType, const expression_vector& flatExprs,
    std::shared_ptr<Expression> offset, std::shared_ptr<Expression> mark, LogicalPlan& plan) {
    auto info = LogicalAccumulateInfo(accumulateType, mark);
    auto op = std::make_shared<LogicalAccumulate>(info, flatExprs, offset, plan.getLastOperator());
    appendFlattens(op->getGroupPositionsToFlatten(), plan);
    op->setChild(0, plan.getLastOperator());
    op->computeFactorizedSchema();
    plan.setLastOperator(std::move(op));
}

} // namespace planner
} // namespace kuzu

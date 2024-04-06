#include "planner/operator/logical_mark_accmulate.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void Planner::appendMarkAccumulate(const expression_vector& keys, std::shared_ptr<Expression> mark,
    LogicalPlan& plan) {
    auto markAccumulate =
        std::make_shared<LogicalMarkAccumulate>(keys, mark, plan.getLastOperator());
    appendFlattens(markAccumulate->getGroupsPosToFlatten(), plan);
    markAccumulate->setChild(0, plan.getLastOperator());
    markAccumulate->computeFactorizedSchema();
    plan.setLastOperator(std::move(markAccumulate));
}

} // namespace planner
} // namespace kuzu

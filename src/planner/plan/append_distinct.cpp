#include "planner/operator/logical_distinct.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void Planner::appendDistinct(const expression_vector& expressionsToDistinct, LogicalPlan& plan) {
    auto distinct = make_shared<LogicalDistinct>(expressionsToDistinct, plan.getLastOperator());
    appendFlattens(distinct->getGroupsPosToFlatten(), plan);
    distinct->setChild(0, plan.getLastOperator());
    distinct->computeFactorizedSchema();
    plan.setLastOperator(std::move(distinct));
}

} // namespace planner
} // namespace kuzu

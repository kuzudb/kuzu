#include "planner/logical_plan/logical_plan.h"

#include <utility>

namespace kuzu {
namespace planner {

unique_ptr<LogicalPlan> LogicalPlan::shallowCopy() const {
    auto plan = make_unique<LogicalPlan>(expressionsToCollect, estCardinality, cost);
    plan->lastOperator = lastOperator;
    return plan;
}

unique_ptr<LogicalPlan> LogicalPlan::deepCopy() const {
    assert(!isEmpty());
    auto plan = make_unique<LogicalPlan>(expressionsToCollect, estCardinality, cost);
    plan->lastOperator = lastOperator->copy();
    plan->lastOperator->computeSchemaRecursive();
    return plan;
}

} // namespace planner
} // namespace kuzu

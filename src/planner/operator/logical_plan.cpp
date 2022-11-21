#include "planner/logical_plan/logical_plan.h"

#include <utility>

namespace kuzu {
namespace planner {

void LogicalPlan::setLastOperator(shared_ptr<LogicalOperator> op) {
    lastOperator = move(op);
}

unique_ptr<LogicalPlan> LogicalPlan::shallowCopy() const {
    auto plan =
        make_unique<LogicalPlan>(schema->copy(), expressionsToCollect, estCardinality, cost);
    plan->lastOperator = lastOperator;
    return plan;
}

unique_ptr<LogicalPlan> LogicalPlan::deepCopy() const {
    auto plan =
        make_unique<LogicalPlan>(schema->copy(), expressionsToCollect, estCardinality, cost);
    plan->lastOperator = lastOperator ? lastOperator->copy() : lastOperator;
    return plan;
}

} // namespace planner
} // namespace kuzu

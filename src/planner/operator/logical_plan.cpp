#include "planner/operator/logical_plan.h"

namespace kuzu {
namespace planner {

std::unique_ptr<LogicalPlan> LogicalPlan::shallowCopy() const {
    auto plan = std::make_unique<LogicalPlan>();
    plan->lastOperator = lastOperator; // shallow copy sub-plan
    plan->estCardinality = estCardinality;
    plan->cost = cost;
    return plan;
}

std::unique_ptr<LogicalPlan> LogicalPlan::deepCopy() const {
    KU_ASSERT(!isEmpty());
    auto plan = std::make_unique<LogicalPlan>();
    plan->lastOperator = lastOperator->copy(); // deep copy sub-plan
    plan->estCardinality = estCardinality;
    plan->cost = cost;
    return plan;
}

} // namespace planner
} // namespace kuzu

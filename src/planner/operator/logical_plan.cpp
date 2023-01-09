#include "planner/logical_plan/logical_plan.h"

namespace kuzu {
namespace planner {

unique_ptr<LogicalPlan> LogicalPlan::shallowCopy() const {
    auto plan = make_unique<LogicalPlan>();
    plan->lastOperator = lastOperator; // shallow copy sub-plan
    plan->estCardinality = estCardinality;
    plan->cost = cost;
    return plan;
}

unique_ptr<LogicalPlan> LogicalPlan::deepCopy() const {
    assert(!isEmpty());
    auto plan = make_unique<LogicalPlan>();
    plan->lastOperator = lastOperator->copy(); // deep copy sub-plan
    plan->lastOperator->computeSchemaRecursive();
    plan->estCardinality = estCardinality;
    plan->cost = cost;
    return plan;
}

} // namespace planner
} // namespace kuzu

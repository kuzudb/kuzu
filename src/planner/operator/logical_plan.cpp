#include "planner/operator/logical_plan.h"

#include "planner/operator/logical_explain.h"

namespace kuzu {
namespace planner {

bool LogicalPlan::isProfile() const {
    return lastOperator->getOperatorType() == LogicalOperatorType::EXPLAIN &&
           reinterpret_cast<LogicalExplain*>(lastOperator.get())->getExplainType() ==
               common::ExplainType::PROFILE;
}

bool LogicalPlan::hasUpdate() const {
    return lastOperator->hasUpdateRecursive();
}

std::unique_ptr<LogicalPlan> LogicalPlan::shallowCopy() const {
    auto plan = std::make_unique<LogicalPlan>();
    plan->lastOperator = lastOperator; // shallow copy sub-plan
    plan->cost = cost;
    return plan;
}

std::unique_ptr<LogicalPlan> LogicalPlan::deepCopy() const {
    KU_ASSERT(!isEmpty());
    auto plan = std::make_unique<LogicalPlan>();
    plan->lastOperator = lastOperator->copy(); // deep copy sub-plan
    plan->cost = cost;
    return plan;
}

} // namespace planner
} // namespace kuzu

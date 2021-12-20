#include "src/planner/include/logical_plan/logical_plan.h"

#include <utility>

namespace graphflow {
namespace planner {

void LogicalPlan::appendOperator(shared_ptr<LogicalOperator> op) {
    lastOperator = move(op);
}

unique_ptr<LogicalPlan> LogicalPlan::copy() const {
    auto plan = make_unique<LogicalPlan>(schema->copy());
    plan->lastOperator = lastOperator;
    plan->cost = cost;
    plan->containAggregation = containAggregation;
    return plan;
}

} // namespace planner
} // namespace graphflow

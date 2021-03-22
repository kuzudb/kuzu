#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalPlan {

public:
    LogicalPlan(unique_ptr<LogicalOperator> lastOperator) : lastOperator{move(lastOperator)} {}

    unique_ptr<LogicalOperator> getLastOperator() { return move(lastOperator); }

private:
    unique_ptr<LogicalOperator> lastOperator;
};

} // namespace planner
} // namespace graphflow

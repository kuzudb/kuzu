#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalPlan {

public:
    LogicalPlan(shared_ptr<LogicalOperator> lastOperator) : lastOperator{lastOperator} {}

    LogicalPlan(const string& path);

    const LogicalOperator& getLastOperator() { return *lastOperator; }

private:
    shared_ptr<LogicalOperator> lastOperator;
};

} // namespace planner
} // namespace graphflow

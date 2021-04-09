#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"
#include "src/planner/include/logical_plan/schema.h"

namespace graphflow {
namespace planner {

class LogicalPlan {

public:
    LogicalPlan(shared_ptr<LogicalOperator> lastOperator) : lastOperator{lastOperator} {}

    LogicalPlan(shared_ptr<LogicalOperator> lastOperator, unique_ptr<Schema> schema)
        : lastOperator{lastOperator}, schema{move(schema)} {}

    LogicalPlan(const string& path);

    const LogicalOperator& getLastOperator() { return *lastOperator; }

public:
    shared_ptr<LogicalOperator> lastOperator;
    unique_ptr<Schema> schema;
};

} // namespace planner
} // namespace graphflow

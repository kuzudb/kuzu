#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"
#include "src/planner/include/logical_plan/schema.h"

namespace graphflow {
namespace planner {

class LogicalPlan {

public:
    LogicalPlan() { schema = make_unique<Schema>(); }

    LogicalPlan(shared_ptr<LogicalOperator> lastOperator, unique_ptr<Schema> schema)
        : lastOperator{lastOperator}, schema{move(schema)} {}

    const LogicalOperator& getLastOperator() { return *lastOperator; }

    void appendOperator(shared_ptr<LogicalOperator> op) { lastOperator = op; }

    unique_ptr<LogicalPlan> copy() {
        return make_unique<LogicalPlan>(lastOperator, schema->copy());
    }

public:
    shared_ptr<LogicalOperator> lastOperator;
    unique_ptr<Schema> schema;
};

} // namespace planner
} // namespace graphflow

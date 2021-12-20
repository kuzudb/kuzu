#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"
#include "src/planner/include/logical_plan/schema.h"

namespace graphflow {
namespace planner {

class LogicalPlan {

public:
    LogicalPlan() : schema{make_unique<Schema>()}, cost{0}, containAggregation{false} {}

    explicit LogicalPlan(unique_ptr<Schema> schema) : schema{move(schema)}, cost{0} {}

    void appendOperator(shared_ptr<LogicalOperator> op);

    inline bool isEmpty() const { return lastOperator == nullptr; }
    inline Schema* getSchema() const { return schema.get(); }

    unique_ptr<LogicalPlan> copy() const;

public:
    shared_ptr<LogicalOperator> lastOperator;
    unique_ptr<Schema> schema;
    uint64_t cost;

    // Note: this is a protection to avoid executing plan contain aggregation with multi-threading.
    // We should remove this field when we support aggregation with multi-threading.
    bool containAggregation;
};

} // namespace planner
} // namespace graphflow

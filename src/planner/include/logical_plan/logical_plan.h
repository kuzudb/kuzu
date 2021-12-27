#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"
#include "src/planner/include/logical_plan/schema.h"

namespace graphflow {
namespace planner {

class LogicalPlan {

public:
    LogicalPlan() : schema{make_unique<Schema>()}, cost{0} {}

    explicit LogicalPlan(unique_ptr<Schema> schema) : schema{move(schema)}, cost{0} {}

    void appendOperator(shared_ptr<LogicalOperator> op);

    inline bool isEmpty() const { return lastOperator == nullptr; }

    inline bool containAggregation() const {
        return lastOperator->descendantsContainType(
            unordered_set<LogicalOperatorType>{LOGICAL_AGGREGATE});
    }

    // Our sub-plan (specific to the right plan of Exists and LeftNestedLoopJoin operator) does not
    // support multiple pipelines. Any pipeline breaking operator is not allowed.
    inline bool containOperatorsNotAllowedInSubPlan() const {
        return lastOperator->descendantsContainType(unordered_set<LogicalOperatorType>{
            LOGICAL_HASH_JOIN, LOGICAL_AGGREGATE, LOGICAL_ORDER_BY});
    }

    unique_ptr<LogicalPlan> copy() const;

public:
    shared_ptr<LogicalOperator> lastOperator;
    unique_ptr<Schema> schema;
    uint64_t cost;
};

} // namespace planner
} // namespace graphflow

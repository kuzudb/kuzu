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

    inline void setExpressionsToCollect(vector<shared_ptr<Expression>> expressions) {
        expressionsToCollect = move(expressions);
    }
    inline vector<shared_ptr<Expression>> getExpressionsToCollect() { return expressionsToCollect; }

    vector<DataType> getExpressionsToCollectDataTypes() const;

    Schema* getSchema() { return schema.get(); }

    unique_ptr<LogicalPlan> deepCopy() const;

    // This copy function does a shallow copy of the logical Plan, so it should be renamed to
    // shallowCopy().
    unique_ptr<LogicalPlan> copy() const;

public:
    shared_ptr<LogicalOperator> lastOperator;
    unique_ptr<Schema> schema;
    uint64_t cost;

private:
    // This fields represents return columns in the same order as user input.
    vector<shared_ptr<Expression>> expressionsToCollect;
};

} // namespace planner
} // namespace graphflow

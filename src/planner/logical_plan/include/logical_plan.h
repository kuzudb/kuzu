#pragma once

#include "src/planner/logical_plan/logical_operator/include/base_logical_operator.h"
#include "src/planner/logical_plan/logical_operator/include/schema.h"

namespace graphflow {
namespace planner {

class LogicalPlan {

public:
    LogicalPlan() : schema{make_unique<Schema>()}, cost{0} {}

    LogicalPlan(unique_ptr<Schema> schema, expression_vector expressionsToCollect, uint64_t cost)
        : schema{move(schema)}, expressionsToCollect{move(expressionsToCollect)}, cost{cost} {}

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

    inline void setExpressionsToCollect(expression_vector expressions) {
        expressionsToCollect = move(expressions);
    }

    inline expression_vector getExpressionsToCollect() { return expressionsToCollect; }

    inline shared_ptr<LogicalOperator> getLastOperator() const { return lastOperator; }

    inline Schema* getSchema() { return schema.get(); }

    vector<DataType> getExpressionsToCollectDataTypes() const;

    vector<string> getExpressionsToCollectNames() const;

    unique_ptr<LogicalPlan> shallowCopy() const;

    unique_ptr<LogicalPlan> deepCopy() const;

private:
    shared_ptr<LogicalOperator> lastOperator;
    unique_ptr<Schema> schema;
    // This fields represents return columns in the same order as user input.
    expression_vector expressionsToCollect;

public:
    uint64_t cost;
};

} // namespace planner
} // namespace graphflow

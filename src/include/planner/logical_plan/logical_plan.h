#pragma once

#include "planner/logical_plan/logical_operator/base_logical_operator.h"
#include "planner/logical_plan/logical_operator/schema.h"

namespace kuzu {
namespace planner {

class LogicalPlan {
public:
    LogicalPlan() : schema{make_unique<Schema>()}, estCardinality{1}, cost{0} {}

    LogicalPlan(unique_ptr<Schema> schema, expression_vector expressionsToCollect,
        uint64_t estCardinality, uint64_t cost)
        : schema{move(schema)}, expressionsToCollect{move(expressionsToCollect)},
          estCardinality{estCardinality}, cost{cost} {}

    void setLastOperator(shared_ptr<LogicalOperator> op);

    inline bool isEmpty() const { return lastOperator == nullptr; }

    inline bool isReadOnly() const {
        return !lastOperator->descendantsContainType(unordered_set<LogicalOperatorType>{
            LOGICAL_SET_NODE_PROPERTY, LOGICAL_CREATE_NODE, LOGICAL_CREATE_REL, LOGICAL_DELETE_NODE,
            LOGICAL_DELETE_REL, LOGICAL_CREATE_NODE_TABLE, LOGICAL_CREATE_REL_TABLE,
            LOGICAL_COPY_CSV, LOGICAL_DROP_TABLE});
    }

    inline void setExpressionsToCollect(expression_vector expressions) {
        expressionsToCollect = move(expressions);
    }

    inline expression_vector getExpressionsToCollect() { return expressionsToCollect; }

    inline shared_ptr<LogicalOperator> getLastOperator() const { return lastOperator; }

    inline Schema* getSchema() { return schema.get(); }

    inline void multiplyCardinality(uint64_t factor) { estCardinality *= factor; }
    inline void setCardinality(uint64_t cardinality) { estCardinality = cardinality; }
    inline uint64_t getCardinality() const { return estCardinality; }

    inline void multiplyCost(uint64_t factor) { cost *= factor; }
    inline void increaseCost(uint64_t costToIncrease) { cost += costToIncrease; }
    inline uint64_t getCost() const { return cost; }

    inline string toString() const { return lastOperator->toString(); }

    inline bool isDDLOrCopyCSV() const {
        return lastOperator->descendantsContainType(
            unordered_set<LogicalOperatorType>{LOGICAL_COPY_CSV, LOGICAL_CREATE_NODE_TABLE,
                LOGICAL_CREATE_REL_TABLE, LOGICAL_DROP_TABLE});
    }

    unique_ptr<LogicalPlan> shallowCopy() const;

    unique_ptr<LogicalPlan> deepCopy() const;

private:
    shared_ptr<LogicalOperator> lastOperator;
    unique_ptr<Schema> schema;
    // This fields represents return columns in the same order as user input.
    expression_vector expressionsToCollect;
    uint64_t estCardinality;
    uint64_t cost;
};

} // namespace planner
} // namespace kuzu

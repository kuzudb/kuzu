#pragma once

#include "planner/logical_plan/logical_operator/base_logical_operator.h"
#include "planner/logical_plan/logical_operator/schema.h"

namespace kuzu {
namespace planner {

class LogicalPlan {
public:
    LogicalPlan() : estCardinality{1}, cost{0} {}

    LogicalPlan(expression_vector expressionsToCollect, uint64_t estCardinality, uint64_t cost)
        : expressionsToCollect{std::move(expressionsToCollect)},
          estCardinality{estCardinality}, cost{cost} {}

    inline void setLastOperator(shared_ptr<LogicalOperator> op) { lastOperator = std::move(op); }

    inline bool isEmpty() const { return lastOperator == nullptr; }

    // TODO(Xiyang): check read only from parser
    inline bool isReadOnly() const {
        return !lastOperator->descendantsContainType(
            unordered_set<LogicalOperatorType>{LogicalOperatorType::SET_NODE_PROPERTY,
                LogicalOperatorType::CREATE_NODE, LogicalOperatorType::CREATE_REL,
                LogicalOperatorType::DELETE_NODE, LogicalOperatorType::DELETE_REL,
                LogicalOperatorType::CREATE_NODE_TABLE, LogicalOperatorType::CREATE_REL_TABLE,
                LogicalOperatorType::COPY_CSV, LogicalOperatorType::DROP_TABLE});
    }

    inline void setExpressionsToCollect(expression_vector expressions) {
        expressionsToCollect = std::move(expressions);
    }

    inline expression_vector getExpressionsToCollect() { return expressionsToCollect; }

    inline shared_ptr<LogicalOperator> getLastOperator() const { return lastOperator; }
    inline Schema* getSchema() const { return lastOperator->getSchema(); }

    inline void multiplyCardinality(uint64_t factor) { estCardinality *= factor; }
    inline void setCardinality(uint64_t cardinality) { estCardinality = cardinality; }
    inline uint64_t getCardinality() const { return estCardinality; }

    inline void multiplyCost(uint64_t factor) { cost *= factor; }
    inline void increaseCost(uint64_t costToIncrease) { cost += costToIncrease; }
    inline uint64_t getCost() const { return cost; }

    inline string toString() const { return lastOperator->toString(); }

    inline bool isDDLOrCopyCSV() const {
        return lastOperator->descendantsContainType(unordered_set<LogicalOperatorType>{
            LogicalOperatorType::COPY_CSV, LogicalOperatorType::CREATE_NODE_TABLE,
            LogicalOperatorType::CREATE_REL_TABLE, LogicalOperatorType::DROP_TABLE});
    }

    unique_ptr<LogicalPlan> shallowCopy() const;

    unique_ptr<LogicalPlan> deepCopy() const;

private:
    shared_ptr<LogicalOperator> lastOperator;
    // This fields represents return columns in the same order as user input.
    expression_vector expressionsToCollect;
    uint64_t estCardinality;
    uint64_t cost;
};

} // namespace planner
} // namespace kuzu

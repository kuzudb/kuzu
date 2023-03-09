#pragma once

#include "planner/logical_plan/logical_operator/base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalPlan {
public:
    LogicalPlan() : estCardinality{1}, cost{0} {}

    inline void setLastOperator(std::shared_ptr<LogicalOperator> op) {
        lastOperator = std::move(op);
    }

    inline bool isEmpty() const { return lastOperator == nullptr; }

    inline std::shared_ptr<LogicalOperator> getLastOperator() const { return lastOperator; }
    inline Schema* getSchema() const { return lastOperator->getSchema(); }

    inline void multiplyCardinality(uint64_t factor) { estCardinality *= factor; }
    inline void setCardinality(uint64_t cardinality) { estCardinality = cardinality; }
    inline uint64_t getCardinality() const { return estCardinality; }

    inline void multiplyCost(uint64_t factor) { cost *= factor; }
    inline void increaseCost(uint64_t costToIncrease) { cost += costToIncrease; }
    inline uint64_t getCost() const { return cost; }

    inline std::string toString() const { return lastOperator->toString(); }

    std::unique_ptr<LogicalPlan> shallowCopy() const;

    std::unique_ptr<LogicalPlan> deepCopy() const;

private:
    std::shared_ptr<LogicalOperator> lastOperator;
    uint64_t estCardinality;
    uint64_t cost;
};

} // namespace planner
} // namespace kuzu

#pragma once

#include "logical_operator.h"

namespace kuzu {
namespace planner {

using cardinality_t = uint64_t;

class LogicalPlan {
    friend class CardinalityEstimator;
    friend class CostModel;

public:
    LogicalPlan() : estCardinality{1}, cost{0} {}

    void setLastOperator(std::shared_ptr<LogicalOperator> op) { lastOperator = std::move(op); }

    bool isEmpty() const { return lastOperator == nullptr; }

    std::shared_ptr<LogicalOperator> getLastOperator() const { return lastOperator; }
    Schema* getSchema() const { return lastOperator->getSchema(); }

    void setCardinality(cardinality_t cardinality) { estCardinality = cardinality; }
    cardinality_t getCardinality() const { return estCardinality; }

    void setCost(uint64_t cost_) { cost = cost_; }
    uint64_t getCost() const { return cost; }

    std::string toString() const { return lastOperator->toString(); }

    bool isProfile() const;
    bool hasUpdate() const;

    std::unique_ptr<LogicalPlan> shallowCopy() const;

    std::unique_ptr<LogicalPlan> deepCopy() const;

private:
    std::shared_ptr<LogicalOperator> lastOperator;
    cardinality_t estCardinality;
    uint64_t cost;
};

} // namespace planner
} // namespace kuzu

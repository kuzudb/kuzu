#pragma once

#include "logical_explain.h"
#include "logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalPlan {
    friend class CardinalityEstimator;
    friend class CostModel;

public:
    LogicalPlan() : estCardinality{1}, cost{0} {}

    inline void setLastOperator(std::shared_ptr<LogicalOperator> op) {
        lastOperator = std::move(op);
    }

    inline bool isEmpty() const { return lastOperator == nullptr; }

    inline std::shared_ptr<LogicalOperator> getLastOperator() const { return lastOperator; }
    inline Schema* getSchema() const { return lastOperator->getSchema(); }

    inline void setCardinality(uint64_t cardinality) { estCardinality = cardinality; }
    inline uint64_t getCardinality() const { return estCardinality; }

    inline void setCost(uint64_t cost_) { cost = cost_; }
    inline uint64_t getCost() const { return cost; }

    inline std::string toString() const { return lastOperator->toString(); }

    inline bool isProfile() const {
        return lastOperator->getOperatorType() == LogicalOperatorType::EXPLAIN &&
               reinterpret_cast<LogicalExplain*>(lastOperator.get())->getExplainType() ==
                   common::ExplainType::PROFILE;
    }

    std::unique_ptr<LogicalPlan> shallowCopy() const;

    std::unique_ptr<LogicalPlan> deepCopy() const;

private:
    std::shared_ptr<LogicalOperator> lastOperator;
    uint64_t estCardinality;
    uint64_t cost;
};

} // namespace planner
} // namespace kuzu

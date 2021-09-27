#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalMultiplicityReducer : public LogicalOperator {

public:
    explicit LogicalMultiplicityReducer(shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator(move(prevOperator)) {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_MULTIPLICITY_REDUCER;
    }

    string getExpressionsForPrinting() const override { return string(); }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalMultiplicityReducer>(prevOperator->copy());
    }
};

} // namespace planner
} // namespace graphflow

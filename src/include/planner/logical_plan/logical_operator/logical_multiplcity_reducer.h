#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalMultiplicityReducer : public LogicalOperator {
public:
    explicit LogicalMultiplicityReducer(shared_ptr<LogicalOperator> child)
        : LogicalOperator(LogicalOperatorType::MULTIPLICITY_REDUCER, std::move(child)) {}

    string getExpressionsForPrinting() const override { return string(); }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalMultiplicityReducer>(children[0]->copy());
    }
};

} // namespace planner
} // namespace kuzu

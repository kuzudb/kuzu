#pragma once

#include <utility>

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalFlatten : public LogicalOperator {

public:
    LogicalFlatten(string variable, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, variable{move(variable)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_FLATTEN;
    }

    string getExpressionsForPrinting() const override { return variable; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalFlatten>(variable, children[0]->copy());
    }

public:
    string variable;
};

} // namespace planner
} // namespace graphflow

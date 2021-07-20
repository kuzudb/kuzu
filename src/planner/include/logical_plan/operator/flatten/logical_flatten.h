#pragma once

#include <utility>

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalFlatten : public LogicalOperator {

public:
    LogicalFlatten(string variable, const shared_ptr<LogicalOperator>& prevOperator)
        : LogicalOperator{prevOperator}, variable{move(variable)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_FLATTEN;
    }

    string getExpressionsForPrinting() const override { return variable; }

public:
    string variable;
};

} // namespace planner
} // namespace graphflow

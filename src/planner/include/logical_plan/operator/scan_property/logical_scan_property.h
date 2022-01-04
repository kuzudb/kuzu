#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalScanProperty : public LogicalOperator {

public:
    LogicalScanProperty(
        string propertyExpressionName, uint32_t propertyKey, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, propertyExpressionName{move(propertyExpressionName)},
          propertyKey{propertyKey} {}

    inline string getPropertyExpressionName() const { return propertyExpressionName; }
    inline uint32_t getPropertyKey() const { return propertyKey; }

    LogicalOperatorType getLogicalOperatorType() const override = 0;

    string getExpressionsForPrinting() const override { return propertyExpressionName; }

    unique_ptr<LogicalOperator> copy() override = 0;

protected:
    string propertyExpressionName;
    uint32_t propertyKey;
};

} // namespace planner
} // namespace graphflow

#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalLimit : public LogicalOperator {

public:
    LogicalLimit(uint64_t limitNumber, shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{move(prevOperator)}, limitNumber{limitNumber} {}

    LogicalOperatorType getLogicalOperatorType() const override { return LOGICAL_LIMIT; }

    string getExpressionsForPrinting() const override { return to_string(limitNumber); }

private:
    uint64_t limitNumber;
};

} // namespace planner
} // namespace graphflow

#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalSkip : public LogicalOperator {

public:
    LogicalSkip(uint64_t skipNumber, shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator(move(prevOperator)), skipNumber{skipNumber} {}

    LogicalOperatorType getLogicalOperatorType() const override { return LOGICAL_SKIP; }

    string getExpressionsForPrinting() const override { return to_string(skipNumber); }

    inline uint64_t getSkipNumber() const { return skipNumber; }

private:
    uint64_t skipNumber;
};

} // namespace planner
} // namespace graphflow

#pragma once

#include "base_logical_operator.h"

#include "src/binder/expression/include/expression.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalResultScan : public LogicalOperator {

public:
    LogicalResultScan(expression_vector expressionsToScan)
        : expressionsToScan{move(expressionsToScan)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_SELECT_SCAN;
    }

    string getExpressionsForPrinting() const override;

    inline expression_vector getExpressionsToScan() const { return expressionsToScan; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalResultScan>(expressionsToScan);
    }

private:
    expression_vector expressionsToScan;
};

} // namespace planner
} // namespace graphflow

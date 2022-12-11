#pragma once

#include "base_logical_operator.h"
#include "binder/expression/expression.h"

namespace kuzu {
namespace planner {
using namespace kuzu::binder;

class LogicalFTableScan : public LogicalOperator {
public:
    LogicalFTableScan(expression_vector expressionsToScan, expression_vector expressionsAccumulated)
        : LogicalOperator{LogicalOperatorType::FTABLE_SCAN},
          expressionsToScan{std::move(expressionsToScan)}, expressionsAccumulated{
                                                               std::move(expressionsAccumulated)} {}

    inline string getExpressionsForPrinting() const override {
        return ExpressionUtil::toString(expressionsToScan);
    }

    inline expression_vector getExpressionsToScan() const { return expressionsToScan; }
    inline expression_vector getExpressionsAccumulated() const { return expressionsAccumulated; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalFTableScan>(expressionsToScan, expressionsAccumulated);
    }

private:
    expression_vector expressionsToScan;
    // expressionsToScan can be a subset of expressionsAccumulated (i.e. partially scan a factorized
    // table).
    expression_vector expressionsAccumulated;
};

} // namespace planner
} // namespace kuzu

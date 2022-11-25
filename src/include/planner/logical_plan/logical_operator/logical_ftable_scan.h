#pragma once

#include "base_logical_operator.h"
#include "binder/expression/expression.h"

namespace kuzu {
namespace planner {
using namespace kuzu::binder;

class LogicalFTableScan : public LogicalOperator {
public:
    LogicalFTableScan(expression_vector expressionsToScan, expression_vector expressionsAccumulated,
        vector<uint64_t> flatOutputGroupPositions)
        : expressionsToScan{std::move(expressionsToScan)}, expressionsAccumulated{std::move(
                                                               expressionsAccumulated)},
          flatOutputGroupPositions{std::move(flatOutputGroupPositions)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_FTABLE_SCAN;
    }

    inline string getExpressionsForPrinting() const override {
        return ExpressionUtil::toString(expressionsToScan);
    }

    inline expression_vector getExpressionsToScan() const { return expressionsToScan; }
    inline expression_vector getExpressionsAccumulated() const { return expressionsAccumulated; }
    inline vector<uint64_t> getFlatOutputGroupPositions() const { return flatOutputGroupPositions; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalFTableScan>(
            expressionsToScan, expressionsAccumulated, flatOutputGroupPositions);
    }

private:
    expression_vector expressionsToScan;
    // expressionsToScan can be a subset of expressionsAccumulated (i.e. partially scan a factorized
    // table).
    expression_vector expressionsAccumulated;
    vector<uint64_t> flatOutputGroupPositions;
};

} // namespace planner
} // namespace kuzu

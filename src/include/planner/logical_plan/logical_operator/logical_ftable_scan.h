#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalFTableScan : public LogicalOperator {
public:
    LogicalFTableScan(expression_vector expressionsToScan, unique_ptr<Schema> schemaToScanFrom)
        : LogicalOperator{LogicalOperatorType::FTABLE_SCAN},
          expressionsToScan{std::move(expressionsToScan)}, schemaToScanFrom{
                                                               std::move(schemaToScanFrom)} {}

    void computeSchema() override;

    inline string getExpressionsForPrinting() const override {
        return ExpressionUtil::toString(expressionsToScan);
    }

    inline expression_vector getExpressionsToScan() const { return expressionsToScan; }
    inline expression_vector getExpressionsAccumulated() const {
        return schemaToScanFrom->getExpressionsInScope();
    }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalFTableScan>(expressionsToScan, schemaToScanFrom->copy());
    }

private:
    unordered_map<uint32_t, expression_vector> populateGroupPosToExpressionsMap();

private:
    expression_vector expressionsToScan;
    unique_ptr<Schema> schemaToScanFrom;
};

} // namespace planner
} // namespace kuzu

#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalFTableScan : public LogicalOperator {
public:
    LogicalFTableScan(
        binder::expression_vector expressionsToScan, std::unique_ptr<Schema> schemaToScanFrom)
        : LogicalOperator{LogicalOperatorType::FTABLE_SCAN},
          expressionsToScan{std::move(expressionsToScan)}, schemaToScanFrom{
                                                               std::move(schemaToScanFrom)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getExpressionsForPrinting() const override {
        return binder::ExpressionUtil::toString(expressionsToScan);
    }

    inline binder::expression_vector getExpressionsToScan() const { return expressionsToScan; }
    inline binder::expression_vector getExpressionsAccumulated() const {
        return schemaToScanFrom->getExpressionsInScope();
    }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalFTableScan>(expressionsToScan, schemaToScanFrom->copy());
    }

private:
    std::unordered_map<uint32_t, binder::expression_vector> populateGroupPosToExpressionsMap();

private:
    binder::expression_vector expressionsToScan;
    std::unique_ptr<Schema> schemaToScanFrom;
};

} // namespace planner
} // namespace kuzu

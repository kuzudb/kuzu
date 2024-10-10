#pragma once

#include "binder/expression/expression_util.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

// LogicalExpressionsScan scans from an outer factorize table
class LogicalExpressionsScan final : public LogicalOperator {
public:
    explicit LogicalExpressionsScan(binder::expression_vector expressions)
        : LogicalOperator{LogicalOperatorType::EXPRESSIONS_SCAN},
          expressions{std::move(expressions)}, outerAccumulate{nullptr} {}

    void computeFactorizedSchema() override { computeSchema(); }
    void computeFlatSchema() override { computeSchema(); }

    std::string getExpressionsForPrinting() const override {
        return binder::ExpressionUtil::toString(expressions);
    }

    binder::expression_vector getExpressions() const { return expressions; }
    void setOuterAccumulate(LogicalOperator* op) { outerAccumulate = op; }
    LogicalOperator* getOuterAccumulate() const { return outerAccumulate; }

    std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalExpressionsScan>(expressions);
    }

private:
    void computeSchema();

private:
    binder::expression_vector expressions;
    LogicalOperator* outerAccumulate;
};

} // namespace planner
} // namespace kuzu

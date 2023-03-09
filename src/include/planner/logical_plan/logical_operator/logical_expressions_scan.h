#pragma once

#include "base_logical_operator.h"
#include "binder/expression/expression.h"

namespace kuzu {
namespace planner {

class LogicalExpressionsScan : public LogicalOperator {
public:
    // LogicalExpressionsScan does not take input from child operator. So its input expressions must
    // be evaluated statically i.e. must be value.
    explicit LogicalExpressionsScan(binder::expression_vector expressions)
        : LogicalOperator{LogicalOperatorType::EXPRESSIONS_SCAN}, expressions{
                                                                      std::move(expressions)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline std::string getExpressionsForPrinting() const override {
        return binder::ExpressionUtil::toString(expressions);
    }

    inline binder::expression_vector getExpressions() const { return expressions; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalExpressionsScan>(expressions);
    }

private:
    binder::expression_vector expressions;
};

} // namespace planner
} // namespace kuzu

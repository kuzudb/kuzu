#pragma once

#include "base_logical_operator.h"
#include "binder/expression/expression.h"

namespace kuzu {
namespace planner {

class LogicalExpressionsScan : public LogicalOperator {
public:
    // LogicalExpressionsScan does not take input from child operator. So its input expressions must
    // be evaluated statically i.e. must be literal.
    explicit LogicalExpressionsScan(expression_vector expressions)
        : LogicalOperator{LogicalOperatorType::EXPRESSIONS_SCAN}, expressions{
                                                                      std::move(expressions)} {}

    void computeSchema() override;

    inline string getExpressionsForPrinting() const override {
        return ExpressionUtil::toString(expressions);
    }

    inline expression_vector getExpressions() const { return expressions; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalExpressionsScan>(expressions);
    }

private:
    expression_vector expressions;
};

} // namespace planner
} // namespace kuzu

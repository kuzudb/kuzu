#pragma once

#include "base_logical_operator.h"
#include "binder/expression/expression.h"

namespace kuzu {
namespace planner {

class LogicalExpressionsScan : public LogicalOperator {
public:
    // LogicalExpressionsScan does not take input from child operator. So its input expressions must
    // be evaluated statically i.e. must be literal.
    LogicalExpressionsScan(expression_vector expressions) : expressions{std::move(expressions)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_EXPRESSIONS_SCAN;
    }

    inline string getExpressionsForPrinting() const override {
        return ExpressionUtil::toString(expressions);
    }
    inline expression_vector getExpressions() const { return expressions; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalExpressionsScan>(expressions);
    }

private:
    expression_vector expressions;
};

} // namespace planner
} // namespace kuzu

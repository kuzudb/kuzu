#pragma once

#include "base_logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalUnwind : public LogicalOperator {
public:
    LogicalUnwind(shared_ptr<Expression> expression) : expression{move(expression)} {}

    inline shared_ptr<Expression> getExpression() { return expression; }

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_UNWIND;
    }

    inline string getExpressionsForPrinting() const override { return expression->getUniqueName(); }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalUnwind>(expression);
    }

private:
    shared_ptr<Expression> expression;
};

} // namespace planner
} // namespace graphflow

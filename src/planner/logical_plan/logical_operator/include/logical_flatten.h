#pragma once

#include "base_logical_operator.h"

#include "src/binder/expression/include/expression.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalFlatten : public LogicalOperator {

public:
    LogicalFlatten(shared_ptr<Expression> expression, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, expression{move(expression)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_FLATTEN;
    }

    string getExpressionsForPrinting() const override { return expression->getUniqueName(); }

    inline shared_ptr<Expression> getExpressionToFlatten() const { return expression; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalFlatten>(expression, children[0]->copy());
    }

private:
    shared_ptr<Expression> expression;
};

} // namespace planner
} // namespace graphflow

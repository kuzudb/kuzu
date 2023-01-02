#pragma once

#include "base_logical_operator.h"
#include "binder/expression/expression.h"

namespace kuzu {
namespace planner {

class LogicalFlatten : public LogicalOperator {
public:
    LogicalFlatten(shared_ptr<Expression> expression, shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::FLATTEN, std::move(child)}, expression{std::move(
                                                                               expression)} {}

    void computeSchema() override;

    inline string getExpressionsForPrinting() const override { return expression->getUniqueName(); }

    inline shared_ptr<Expression> getExpression() const { return expression; }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalFlatten>(expression, children[0]->copy());
    }

private:
    shared_ptr<Expression> expression;
};

} // namespace planner
} // namespace kuzu

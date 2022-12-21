#pragma once

#include "base_logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalUnwind : public LogicalOperator {
public:
    LogicalUnwind(shared_ptr<Expression> expression, shared_ptr<Expression> aliasExpression,
        shared_ptr<LogicalOperator> childOperator)
        : LogicalOperator{LogicalOperatorType::UNWIND, std::move(childOperator)},
          expression{std::move(expression)}, aliasExpression{std::move(aliasExpression)} {}

    void computeSchema() override;

    inline shared_ptr<Expression> getExpression() { return expression; }

    inline shared_ptr<Expression> getAliasExpression() { return aliasExpression; }

    inline string getExpressionsForPrinting() const override { return expression->getUniqueName(); }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalUnwind>(expression, aliasExpression, children[0]->copy());
    }

private:
    shared_ptr<Expression> expression;
    shared_ptr<Expression> aliasExpression;
};

} // namespace planner
} // namespace kuzu

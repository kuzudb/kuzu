#pragma once

#include "base_logical_operator.h"
#include "binder/expression/expression.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

class LogicalFlatten : public LogicalOperator {
public:
    LogicalFlatten(shared_ptr<Expression> expression, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, expression{move(expression)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_FLATTEN;
    }

    inline string getExpressionsForPrinting() const override { return expression->getUniqueName(); }

    inline shared_ptr<Expression> getExpression() const { return expression; }

    inline void computeSchema(Schema& schema) {
        auto group = schema.getGroup(expression);
        assert(!group->getIsFlat());
        group->setIsFlat(true);
    }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalFlatten>(expression, children[0]->copy());
    }

private:
    shared_ptr<Expression> expression;
};

} // namespace planner
} // namespace kuzu

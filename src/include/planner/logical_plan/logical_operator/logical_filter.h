#pragma once

#include "base_logical_operator.h"
#include "binder/expression/expression.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

class LogicalFilter : public LogicalOperator {
public:
    LogicalFilter(shared_ptr<Expression> expression, uint32_t groupPosToSelect,
        shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::FILTER, std::move(child)},
          expression{std::move(expression)}, groupPosToSelect{groupPosToSelect} {}

    string getExpressionsForPrinting() const override { return expression->getUniqueName(); }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalFilter>(expression, groupPosToSelect, children[0]->copy());
    }

public:
    shared_ptr<Expression> expression;
    uint32_t groupPosToSelect;
};

} // namespace planner
} // namespace kuzu

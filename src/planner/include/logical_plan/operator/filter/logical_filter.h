#pragma once

#include "src/binder/expression/include/expression.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalFilter : public LogicalOperator {

public:
    LogicalFilter(shared_ptr<Expression> expression, uint32_t groupPosToSelect,
        shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, expression{move(expression)}, groupPosToSelect{
                                                                          groupPosToSelect} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_FILTER;
    }

    string getExpressionsForPrinting() const override { return expression->getUniqueName(); }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalFilter>(expression, groupPosToSelect, children[0]->copy());
    }

public:
    shared_ptr<Expression> expression;
    uint32_t groupPosToSelect;
};

} // namespace planner
} // namespace graphflow

#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalFilter : public LogicalOperator {

public:
    LogicalFilter(shared_ptr<Expression> expression, uint32_t groupPosToSelect,
        shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, expression{expression}, groupPosToSelect{
                                                                     groupPosToSelect} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_FILTER;
    }

    string getExpressionsForPrinting() const override { return expression->rawExpression; }

public:
    shared_ptr<Expression> expression;
    uint32_t groupPosToSelect;
};

} // namespace planner
} // namespace graphflow

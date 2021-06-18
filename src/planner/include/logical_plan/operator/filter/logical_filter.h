#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalFilter : public LogicalOperator {

public:
    LogicalFilter(shared_ptr<Expression> rootExpr, shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{prevOperator}, rootExpr{rootExpr} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_FILTER;
    }

    const Expression& getRootExpression() const { return *rootExpr; }

    string getExpressionsForPrinting() const override { return rootExpr->rawExpression; }

public:
    shared_ptr<Expression> rootExpr;
};

} // namespace planner
} // namespace graphflow

#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalOrderBy : public LogicalOperator {

public:
    LogicalOrderBy(vector<shared_ptr<Expression>> expressions, vector<bool> sortOrders,
        shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{move(prevOperator)}, orderByExpressions{move(expressions)},
          isAscOrders{move(sortOrders)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_ORDER_BY;
    }

    string getExpressionsForPrinting() const override {
        auto result = orderByExpressions[0]->getUniqueName();
        for (auto i = 1u; i < orderByExpressions.size(); ++i) {
            result += ", " + orderByExpressions[i]->getUniqueName();
        }
        return result;
    }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalOrderBy>(orderByExpressions, isAscOrders, prevOperator->copy());
    }

private:
    vector<shared_ptr<Expression>> orderByExpressions;
    vector<bool> isAscOrders;
};

} // namespace planner
} // namespace graphflow

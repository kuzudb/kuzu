#pragma once

#include "src/binder/include/expression/expression.h"
#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalPlan;

class LogicalExists : public LogicalOperator {

public:
    LogicalExists(shared_ptr<Expression> subqueryExpression, unique_ptr<LogicalPlan> subPlan,
        shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{move(prevOperator)},
          subqueryExpression{move(subqueryExpression)}, subPlan{move(subPlan)} {}

    inline LogicalPlan* getSubPlan() { return subPlan.get(); }

    inline shared_ptr<Expression> getSubqueryExpression() { return subqueryExpression; }

    LogicalOperatorType getLogicalOperatorType() const override { return LOGICAL_EXISTS; }

    string toString(uint64_t depth = 0) const override {
        string result = LogicalOperatorTypeNames[getLogicalOperatorType()] + "[" +
                        getExpressionsForPrinting() + "]";
        result += prevOperator->toString(depth + 1);
        result += "\nSUBPLAN: \n";
        result += subPlan->lastOperator->toString(depth + 1);
        return result;
    }

    string getExpressionsForPrinting() const override { return string(); }

    unique_ptr<LogicalOperator> copy() override {
        auto subPlanCopy = subPlan->copy();
        subPlanCopy->lastOperator = subPlan->lastOperator->copy();
        return make_unique<LogicalExists>(
            subqueryExpression, move(subPlanCopy), prevOperator->copy());
    }

private:
    shared_ptr<Expression> subqueryExpression;
    unique_ptr<LogicalPlan> subPlan;
};

} // namespace planner
} // namespace graphflow

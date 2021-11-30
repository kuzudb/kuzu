#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalPlan;

class LogicalLeftNestedLoopJoin : public LogicalOperator {

public:
    LogicalLeftNestedLoopJoin(
        unique_ptr<LogicalPlan> subPlan, shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{move(prevOperator)}, subPlan{move(subPlan)} {}

    inline LogicalPlan* getSubPlan() { return subPlan.get(); }

    LogicalOperatorType getLogicalOperatorType() const override {
        return LOGICAL_LEFT_NESTED_LOOP_JOIN;
    }

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
        return make_unique<LogicalLeftNestedLoopJoin>(move(subPlanCopy), prevOperator->copy());
    }

private:
    unique_ptr<LogicalPlan> subPlan;
};

} // namespace planner
} // namespace graphflow

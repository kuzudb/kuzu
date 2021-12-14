#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

class LogicalLeftNestedLoopJoin : public LogicalOperator {

public:
    LogicalLeftNestedLoopJoin(const shared_ptr<LogicalOperator>& subPlanLastOperator,
        unique_ptr<Schema> subPlanSchema, vector<string> matchedNodeIDsInSubPlan,
        shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{move(prevOperator)}, subPlanLastOperator{subPlanLastOperator},
          subPlanSchema{move(subPlanSchema)}, matchedNodeIDsInSubPlan{
                                                  move(matchedNodeIDsInSubPlan)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LOGICAL_LEFT_NESTED_LOOP_JOIN;
    }

    string toString(uint64_t depth = 0) const override {
        string result = LogicalOperatorTypeNames[getLogicalOperatorType()] + "[" +
                        getExpressionsForPrinting() + "]";
        result += prevOperator->toString(depth + 1);
        result += "\nSUBPLAN: \n";
        result += subPlanLastOperator->toString(depth + 1);
        return result;
    }

    string getExpressionsForPrinting() const override { return string(); }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalLeftNestedLoopJoin>(subPlanLastOperator->copy(),
            subPlanSchema->copy(), matchedNodeIDsInSubPlan, prevOperator->copy());
    }

public:
    shared_ptr<LogicalOperator> subPlanLastOperator;
    unique_ptr<Schema> subPlanSchema;

    // This field is used to push property scanners on top of left nested loop join.
    vector<string> matchedNodeIDsInSubPlan;
};

} // namespace planner
} // namespace graphflow

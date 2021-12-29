#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"
#include "src/planner/include/logical_plan/schema.h"

namespace graphflow {
namespace planner {

// Sub-plan on right.
class LogicalLeftNestedLoopJoin : public LogicalOperator {

public:
    LogicalLeftNestedLoopJoin(unique_ptr<Schema> subPlanSchema,
        vector<string> matchedNodeIDsInSubPlan, shared_ptr<LogicalOperator> child,
        shared_ptr<LogicalOperator> subPlanChild)
        : LogicalOperator{move(child), move(subPlanChild)}, subPlanSchema{move(subPlanSchema)},
          matchedNodeIDsInSubPlan{move(matchedNodeIDsInSubPlan)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LOGICAL_LEFT_NESTED_LOOP_JOIN;
    }

    string getExpressionsForPrinting() const override { return string(); }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalLeftNestedLoopJoin>(subPlanSchema->copy(),
            matchedNodeIDsInSubPlan, children[0]->copy(), children[1]->copy());
    }

public:
    unique_ptr<Schema> subPlanSchema;

    // This field is used to push property scanners on top of left nested loop join.
    vector<string> matchedNodeIDsInSubPlan;
};

} // namespace planner
} // namespace graphflow

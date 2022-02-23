#pragma once

#include "base_logical_operator.h"
#include "schema.h"

namespace graphflow {
namespace planner {

// Sub-plan on right.
class LogicalLeftNestedLoopJoin : public LogicalOperator {

public:
    LogicalLeftNestedLoopJoin(unique_ptr<Schema> subPlanSchema, shared_ptr<LogicalOperator> child,
        shared_ptr<LogicalOperator> subPlanChild)
        : LogicalOperator{move(child), move(subPlanChild)}, subPlanSchema{move(subPlanSchema)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LOGICAL_LEFT_NESTED_LOOP_JOIN;
    }

    string getExpressionsForPrinting() const override { return string(); }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalLeftNestedLoopJoin>(
            subPlanSchema->copy(), children[0]->copy(), children[1]->copy());
    }

public:
    unique_ptr<Schema> subPlanSchema;
};

} // namespace planner
} // namespace graphflow

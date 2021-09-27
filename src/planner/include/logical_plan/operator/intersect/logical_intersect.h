#pragma once

#include "src/planner/include/logical_plan/operator/logical_operator.h"

namespace graphflow {
namespace planner {

/**
 * LogicalIntersect takes two nodeID with different names but representing the same query node and
 * intersect them. We assume leftNodeID has the original name while rightNodeID is the one being
 * created temporarily to break cycle.
 */
class LogicalIntersect : public LogicalOperator {

public:
    LogicalIntersect(
        string leftNodeID, string rightNodeID, shared_ptr<LogicalOperator> prevOperator)
        : LogicalOperator{move(prevOperator)}, leftNodeID{move(leftNodeID)}, rightNodeID{move(
                                                                                 rightNodeID)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_INTERSECT;
    }

    string getExpressionsForPrinting() const override { return leftNodeID + ", " + rightNodeID; }

    inline string getLeftNodeID() const { return leftNodeID; }
    inline string getRightNodeID() const { return rightNodeID; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalIntersect>(leftNodeID, rightNodeID, prevOperator->copy());
    }

private:
    string leftNodeID;
    // Right node ID should be the temporary node ID
    string rightNodeID;
};

} // namespace planner
} // namespace graphflow

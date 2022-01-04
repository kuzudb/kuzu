#pragma once

#include <unordered_map>

#include "src/planner/include/logical_plan/logical_plan.h"

namespace graphflow {
namespace planner {

// This optimization extract all property scanners and append them on top of scan node ID / extend.
class PropertyScanPushDown {

public:
    inline void rewrite(LogicalPlan& logicalPlan) {
        logicalPlan.lastOperator = rewrite(logicalPlan.lastOperator, *logicalPlan.schema);
    }

private:
    shared_ptr<LogicalOperator> rewrite(shared_ptr<LogicalOperator> op, Schema& schema);

    shared_ptr<LogicalOperator> rewriteScanNodeID(
        const shared_ptr<LogicalOperator>& op, Schema& schema);

    shared_ptr<LogicalOperator> rewriteExtend(
        const shared_ptr<LogicalOperator>& op, Schema& schema);

    shared_ptr<LogicalOperator> rewriteScanNodeProperty(
        const shared_ptr<LogicalOperator>& op, Schema& schema);

    shared_ptr<LogicalOperator> rewriteScanRelProperty(
        const shared_ptr<LogicalOperator>& op, Schema& schema);

    shared_ptr<LogicalOperator> rewriteAggregate(
        const shared_ptr<LogicalOperator>& op, Schema& schema);

    shared_ptr<LogicalOperator> rewriteHashJoin(
        const shared_ptr<LogicalOperator>& op, Schema& schema);

    shared_ptr<LogicalOperator> rewriteExists(
        const shared_ptr<LogicalOperator>& op, Schema& schema);

    shared_ptr<LogicalOperator> rewriteLeftNestedLoopJoin(
        const shared_ptr<LogicalOperator>& op, Schema& schema);

    shared_ptr<LogicalOperator> applyPropertyScansIfNecessary(
        const string& nodeID, const shared_ptr<LogicalOperator>& op, Schema& schema);

    void rewriteChildrenOperators(const shared_ptr<LogicalOperator>& op, Schema& schema);

    void addPropertyScan(const string& nodeID, const shared_ptr<LogicalOperator>& op);

    // For operators that merge right branch into left, i.e. hashJoin and leftNestedLoopJoin, any
    // property scanners that are pushed down into the right branch will also be merged into left.
    void addRemainingPropertyScansToLeftSchema(Schema& schema);

private:
    unordered_map<string, vector<shared_ptr<LogicalOperator>>> nodeIDToPropertyScansMap;
};

} // namespace planner
} // namespace graphflow

#include "src/planner/include/property_scan_pushdown.h"

#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/nested_loop_join/logical_left_nested_loop_join.h"
#include "src/planner/include/logical_plan/operator/scan_node_id/logical_scan_node_id.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_node_property.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_rel_property.h"

namespace graphflow {
namespace planner {

shared_ptr<LogicalOperator> PropertyScanPushDown::rewrite(shared_ptr<LogicalOperator> op) {
    switch (op->getLogicalOperatorType()) {
    case LOGICAL_SCAN_NODE_ID:
        return rewriteScanNodeID(op);
    case LOGICAL_EXTEND:
        return rewriteExtend(op);
    case LOGICAL_SCAN_NODE_PROPERTY:
        return rewriteScanNodeProperty(op);
    case LOGICAL_SCAN_REL_PROPERTY:
        return rewriteScanRelProperty(op);
    case LOGICAL_LEFT_NESTED_LOOP_JOIN:
        return rewriteLeftNestedLoopJoin(op);
    default:
        rewriteChildrenOperators(op);
        return op;
    }
}

shared_ptr<LogicalOperator> PropertyScanPushDown::rewriteScanNodeID(
    const shared_ptr<LogicalOperator>& op) {
    auto& scanNodeID = (LogicalScanNodeID&)*op;
    return applyPropertyScansIfNecessary(scanNodeID.nodeID, op);
}

shared_ptr<LogicalOperator> PropertyScanPushDown::rewriteExtend(
    const shared_ptr<LogicalOperator>& op) {
    auto& extend = (LogicalExtend&)*op;
    return applyPropertyScansIfNecessary(extend.nbrNodeID, op);
}

// Push down all property scanners on node IDs computed in subquery on top on the left nested loop
// join operator.
shared_ptr<LogicalOperator> PropertyScanPushDown::rewriteLeftNestedLoopJoin(
    const shared_ptr<LogicalOperator>& op) {
    auto& leftNLJ = (LogicalLeftNestedLoopJoin&)*op;
    shared_ptr<LogicalOperator> result;
    for (auto& nodeID : leftNLJ.matchedNodeIDsInSubPlan) {
        result = applyPropertyScansIfNecessary(nodeID, op);
    }
    return result;
}

shared_ptr<LogicalOperator> PropertyScanPushDown::rewriteScanNodeProperty(
    const shared_ptr<LogicalOperator>& op) {
    auto& scanNodeProperty = (LogicalScanNodeProperty&)*op;
    addPropertyScan(scanNodeProperty.nodeID, op);
    return rewrite(scanNodeProperty.getFirstChild());
}

shared_ptr<LogicalOperator> PropertyScanPushDown::rewriteScanRelProperty(
    const shared_ptr<LogicalOperator>& op) {
    auto& scanRelProperty = (LogicalScanRelProperty&)*op;
    addPropertyScan(scanRelProperty.nbrNodeID, op);
    return rewrite(scanRelProperty.getFirstChild());
}

shared_ptr<LogicalOperator> PropertyScanPushDown::applyPropertyScansIfNecessary(
    const string& nodeID, const shared_ptr<LogicalOperator>& op) {
    if (!nodeIDToPropertyScansMap.contains(nodeID)) {
        // nothing needs to be applied
        rewriteChildrenOperators(op);
        return op;
    }
    auto& propertyScans = nodeIDToPropertyScansMap.at(nodeID);
    // chain property scans
    for (auto i = 0u; i < propertyScans.size() - 1; ++i) {
        propertyScans[i]->setFirstChild(propertyScans[i + 1]);
    }
    propertyScans.back()->setFirstChild(op);
    auto result = propertyScans[0];
    nodeIDToPropertyScansMap.erase(nodeID);
    rewriteChildrenOperators(op);
    return result;
}

void PropertyScanPushDown::rewriteChildrenOperators(const shared_ptr<LogicalOperator>& op) {
    if (op->getNumChildren() == 0) {
        return;
    }
    op->setFirstChild(rewrite(op->getFirstChild()));
    if (op->getNumChildren() == 2) {
        op->setSecondChild(rewrite(op->getSecondChild()));
    }
}

void PropertyScanPushDown::addPropertyScan(
    const string& nodeID, const shared_ptr<LogicalOperator>& op) {
    if (!nodeIDToPropertyScansMap.contains(nodeID)) {
        nodeIDToPropertyScansMap.insert({nodeID, vector<shared_ptr<LogicalOperator>>()});
    }
    nodeIDToPropertyScansMap.at(nodeID).push_back(op);
}

} // namespace planner
} // namespace graphflow

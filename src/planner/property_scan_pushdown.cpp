#include "src/planner/include/property_scan_pushdown.h"

#include "src/planner/include/logical_plan/operator/aggregate/logical_aggregate.h"
#include "src/planner/include/logical_plan/operator/exists/logical_exist.h"
#include "src/planner/include/logical_plan/operator/extend/logical_extend.h"
#include "src/planner/include/logical_plan/operator/hash_join/logical_hash_join.h"
#include "src/planner/include/logical_plan/operator/nested_loop_join/logical_left_nested_loop_join.h"
#include "src/planner/include/logical_plan/operator/scan_node_id/logical_scan_node_id.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_node_property.h"
#include "src/planner/include/logical_plan/operator/scan_property/logical_scan_rel_property.h"

namespace graphflow {
namespace planner {

shared_ptr<LogicalOperator> PropertyScanPushDown::rewrite(
    shared_ptr<LogicalOperator> op, Schema& schema) {
    switch (op->getLogicalOperatorType()) {
    case LOGICAL_SCAN_NODE_ID:
        return rewriteScanNodeID(op, schema);
    case LOGICAL_EXTEND:
        return rewriteExtend(op, schema);
    case LOGICAL_SCAN_NODE_PROPERTY:
        return rewriteScanNodeProperty(op, schema);
    case LOGICAL_SCAN_REL_PROPERTY:
        return rewriteScanRelProperty(op, schema);
    case LOGICAL_AGGREGATE:
        return rewriteAggregate(op, schema);
    case LOGICAL_HASH_JOIN:
        return rewriteHashJoin(op, schema);
    case LOGICAL_EXISTS:
        return rewriteExists(op, schema);
    case LOGICAL_LEFT_NESTED_LOOP_JOIN:
        return rewriteLeftNestedLoopJoin(op, schema);
    default:
        rewriteChildrenOperators(op, schema);
        return op;
    }
}

shared_ptr<LogicalOperator> PropertyScanPushDown::rewriteScanNodeID(
    const shared_ptr<LogicalOperator>& op, Schema& schema) {
    auto& scanNodeID = (LogicalScanNodeID&)*op;
    return applyPropertyScansIfNecessary(scanNodeID.nodeID, op, schema);
}

shared_ptr<LogicalOperator> PropertyScanPushDown::rewriteExtend(
    const shared_ptr<LogicalOperator>& op, Schema& schema) {
    auto& extend = (LogicalExtend&)*op;
    return applyPropertyScansIfNecessary(extend.nbrNodeID, op, schema);
}

shared_ptr<LogicalOperator> PropertyScanPushDown::rewriteScanNodeProperty(
    const shared_ptr<LogicalOperator>& op, Schema& schema) {
    auto& scanNodeProperty = (LogicalScanNodeProperty&)*op;
    addPropertyScan(scanNodeProperty.nodeID, op);
    schema.removeExpression(scanNodeProperty.getPropertyExpressionName());
    return rewrite(scanNodeProperty.getFirstChild(), schema);
}

shared_ptr<LogicalOperator> PropertyScanPushDown::rewriteScanRelProperty(
    const shared_ptr<LogicalOperator>& op, Schema& schema) {
    auto& scanRelProperty = (LogicalScanRelProperty&)*op;
    addPropertyScan(scanRelProperty.nbrNodeID, op);
    schema.removeExpression(scanRelProperty.getPropertyExpressionName());
    return rewrite(scanRelProperty.getFirstChild(), schema);
}

shared_ptr<LogicalOperator> PropertyScanPushDown::rewriteAggregate(
    const shared_ptr<LogicalOperator>& op, Schema& schema) {
    auto& logicalAggregate = (LogicalAggregate&)*op;
    op->setFirstChild(rewrite(op->getFirstChild(), *logicalAggregate.getSchemaBeforeAggregate()));
    return op;
}

shared_ptr<LogicalOperator> PropertyScanPushDown::rewriteHashJoin(
    const shared_ptr<LogicalOperator>& op, Schema& schema) {
    auto& logicalHashJoin = (LogicalHashJoin&)*op;
    op->setFirstChild(rewrite(op->getFirstChild(), schema));
    addRemainingPropertyScansToLeftSchema(schema);
    op->setSecondChild(rewrite(op->getSecondChild(), *logicalHashJoin.buildSideSchema));
    return op;
}

shared_ptr<LogicalOperator> PropertyScanPushDown::rewriteExists(
    const shared_ptr<LogicalOperator>& op, Schema& schema) {
    auto& logicalExists = (LogicalExists&)*op;
    op->setFirstChild(rewrite(op->getFirstChild(), schema));
    op->setSecondChild(rewrite(op->getSecondChild(), *logicalExists.subPlanSchema));
    return op;
}

shared_ptr<LogicalOperator> PropertyScanPushDown::rewriteLeftNestedLoopJoin(
    const shared_ptr<LogicalOperator>& op, Schema& schema) {
    auto& logicalLeftNLJ = (LogicalLeftNestedLoopJoin&)*op;
    op->setFirstChild(rewrite(op->getFirstChild(), schema));
    addRemainingPropertyScansToLeftSchema(schema);
    op->setSecondChild(rewrite(op->getSecondChild(), *logicalLeftNLJ.subPlanSchema));
    return op;
}

shared_ptr<LogicalOperator> PropertyScanPushDown::applyPropertyScansIfNecessary(
    const string& nodeID, const shared_ptr<LogicalOperator>& op, Schema& schema) {
    if (!nodeIDToPropertyScansMap.contains(nodeID)) {
        // nothing needs to be applied
        rewriteChildrenOperators(op, schema);
        return op;
    }
    auto& propertyScans = nodeIDToPropertyScansMap.at(nodeID);
    // chain property scans
    for (auto i = 0u; i < propertyScans.size() - 1; ++i) {
        propertyScans[i]->setFirstChild(propertyScans[i + 1]);
    }
    propertyScans.back()->setFirstChild(op);
    auto result = propertyScans[0];
    // update schema
    auto groupPos = schema.getGroupPos(nodeID);
    for (auto& propertyScan : propertyScans) {
        auto& scanProperty = (LogicalScanProperty&)*propertyScan;
        schema.insertToGroup(scanProperty.getPropertyExpressionName(), groupPos);
    }
    nodeIDToPropertyScansMap.erase(nodeID);
    rewriteChildrenOperators(op, schema);
    return result;
}

void PropertyScanPushDown::rewriteChildrenOperators(
    const shared_ptr<LogicalOperator>& op, Schema& schema) {
    if (op->getNumChildren() == 0) {
        return;
    }
    // If any property scanner is successfully pushed down, it will be removed from
    // nodeIDtoPropertyScanMap.
    op->setFirstChild(rewrite(op->getFirstChild(), schema));
    if (op->getNumChildren() == 2) {
        op->setSecondChild(rewrite(op->getSecondChild(), schema));
    }
}

void PropertyScanPushDown::addPropertyScan(
    const string& nodeID, const shared_ptr<LogicalOperator>& op) {
    if (!nodeIDToPropertyScansMap.contains(nodeID)) {
        nodeIDToPropertyScansMap.insert({nodeID, vector<shared_ptr<LogicalOperator>>()});
    }
    nodeIDToPropertyScansMap.at(nodeID).push_back(op);
}

void PropertyScanPushDown::addRemainingPropertyScansToLeftSchema(Schema& schema) {
    for (auto& [nodeID, propertyScanners] : nodeIDToPropertyScansMap) {
        auto groupPos = schema.getGroupPos(nodeID);
        for (auto& propertyScanner : propertyScanners) {
            auto& scanNodeProperty = (LogicalScanProperty&)*propertyScanner;
            schema.insertToGroup(scanNodeProperty.getPropertyExpressionName(), groupPos);
        }
    }
}

} // namespace planner
} // namespace graphflow

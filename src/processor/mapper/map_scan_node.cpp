#include "include/plan_mapper.h"

#include "src/binder/expression/include/literal_expression.h"
#include "src/planner/logical_plan/logical_operator/include/logical_scan_node.h"
#include "src/processor/operator/include/index_scan.h"
#include "src/processor/operator/include/scan_node_id.h"

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodeToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalScan = (LogicalScanNode*)logicalOperator;
    auto node = logicalScan->getNode();
    auto& nodesStore = storageManager.getNodesStore();
    auto nodeTable = nodesStore.getNodeTable(node->getTableID());
    auto dataPos = mapperContext.getDataPos(node->getIDProperty());
    mapperContext.addComputedExpressions(node->getIDProperty());
    auto sharedState = make_shared<ScanNodeIDSharedState>(
        &nodesStore.getNodesStatisticsAndDeletedIDs(), node->getTableID());
    return make_unique<ScanNodeID>(mapperContext.getResultSetDescriptor()->copy(),
        node->getUniqueName(), nodeTable, dataPos, sharedState, getOperatorID(),
        logicalScan->getExpressionsForPrinting());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalIndexScanNodeToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalIndexScan = (LogicalIndexScanNode*)logicalOperator;
    auto node = logicalIndexScan->getNode();
    auto nodeTable = storageManager.getNodesStore().getNodeTable(node->getTableID());
    auto dataPos = mapperContext.getDataPos(node->getIDProperty());
    auto indexExpression = (LiteralExpression*)logicalIndexScan->getIndexExpression().get();
    return make_unique<IndexScan>(mapperContext.getResultSetDescriptor()->copy(),
        nodeTable->getTableID(), nodeTable->getIDIndex(), *indexExpression->literal, dataPos,
        getOperatorID(), logicalIndexScan->getExpressionsForPrinting());
}

} // namespace processor
} // namespace graphflow

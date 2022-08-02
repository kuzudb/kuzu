#include "src/planner/logical_plan/logical_operator/include/logical_scan_node_id.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/scan_node_id.h"

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodeIDToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalScan = (LogicalScanNodeID*)logicalOperator;
    auto nodeExpression = logicalScan->getNodeExpression();
    auto& nodesStore = storageManager.getNodesStore();
    auto sharedState = make_shared<ScanNodeIDSharedState>(
        &nodesStore.getNodesMetadata(), nodeExpression->getLabel());
    auto dataPos = mapperContext.getDataPos(nodeExpression->getIDProperty());
    mapperContext.addComputedExpressions(nodeExpression->getIDProperty());
    return make_unique<ScanNodeID>(mapperContext.getResultSetDescriptor()->copy(),
        nodeExpression->getIDProperty(), nodesStore.getNode(nodeExpression->getLabel()), dataPos,
        sharedState, getOperatorID(), logicalScan->getExpressionsForPrinting());
}

} // namespace processor
} // namespace graphflow

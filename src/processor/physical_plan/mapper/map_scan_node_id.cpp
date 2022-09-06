#include "src/planner/logical_plan/logical_operator/include/logical_scan_node_id.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/scan_node_id.h"

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodeIDToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalScanNodeID = (LogicalScanNodeID*)logicalOperator;
    auto nodeExpression = logicalScanNodeID->getNodeExpression();
    auto& nodesStore = storageManager.getNodesStore();
    auto sharedState = make_shared<ScanNodeIDSharedState>(
        &nodesStore.getNodesMetadata(), nodeExpression->getLabel());
    auto dataPos = mapperContext.getDataPos(nodeExpression->getIDProperty());
    mapperContext.addComputedExpressions(nodeExpression->getIDProperty());
    auto scanNodeID =
        make_unique<ScanNodeID>(logicalScanNodeID->getNodeExpression()->getIDProperty(),
            logicalScanNodeID->getFilter(), mapperContext.getResultSetDescriptor()->copy(),
            nodesStore.getNode(nodeExpression->getLabel()), dataPos, sharedState, getOperatorID(),
            logicalScanNodeID->getExpressionsForPrinting());
    return scanNodeID;
}

} // namespace processor
} // namespace graphflow

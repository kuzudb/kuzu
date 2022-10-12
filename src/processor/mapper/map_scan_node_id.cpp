#include "include/plan_mapper.h"

#include "src/planner/logical_plan/logical_operator/include/logical_scan_node_id.h"
#include "src/processor/operator/include/scan_node_id.h"

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodeIDToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalScan = (LogicalScanNodeID*)logicalOperator;
    auto node = logicalScan->getNode();
    auto& nodesStore = storageManager.getNodesStore();
    auto sharedState = make_shared<ScanNodeIDSharedState>(
        &nodesStore.getNodesStatisticsAndDeletedIDs(), node->getTableID());
    auto dataPos = mapperContext.getDataPos(node->getIDProperty());
    mapperContext.addComputedExpressions(node->getIDProperty());
    return make_unique<ScanNodeID>(mapperContext.getResultSetDescriptor()->copy(),
        node->getUniqueName(), nodesStore.getNodeTable(node->getTableID()), dataPos, sharedState,
        getOperatorID(), logicalScan->getExpressionsForPrinting());
}

} // namespace processor
} // namespace graphflow

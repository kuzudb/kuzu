#include "planner/operator/scan/logical_scan_node.h"
#include "processor/operator/scan_node_id.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapScanNode(LogicalOperator* logicalOperator) {
    auto logicalScan = (LogicalScanNode*)logicalOperator;
    auto outSchema = logicalScan->getSchema();
    auto node = logicalScan->getNode();
    auto& nodesStore = storageManager.getNodesStore();
    auto dataPos = DataPos(outSchema->getExpressionPos(*node->getInternalIDProperty()));
    auto sharedState = std::make_shared<ScanNodeIDSharedState>();
    for (auto& tableID : node->getTableIDs()) {
        auto nodeTable = nodesStore.getNodeTable(tableID);
        sharedState->addTableState(nodeTable);
    }
    return make_unique<ScanNodeID>(
        dataPos, sharedState, getOperatorID(), logicalScan->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

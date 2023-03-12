#include "planner/logical_plan/logical_operator/logical_scan_node.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/index_scan.h"
#include "processor/operator/scan_node_id.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodeToPhysical(
    LogicalOperator* logicalOperator) {
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

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalIndexScanNodeToPhysical(
    LogicalOperator* logicalOperator) {
    auto logicalIndexScan = (LogicalIndexScanNode*)logicalOperator;
    auto inSchema = logicalIndexScan->getChild(0)->getSchema();
    auto outSchema = logicalIndexScan->getSchema();
    auto node = logicalIndexScan->getNode();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto nodeTable = storageManager.getNodesStore().getNodeTable(node->getSingleTableID());
    auto indexDataPos =
        DataPos(inSchema->getExpressionPos(*logicalIndexScan->getIndexExpression()));
    auto outDataPos = DataPos(outSchema->getExpressionPos(*node->getInternalIDProperty()));
    return make_unique<IndexScan>(nodeTable->getTableID(), nodeTable->getPKIndex(), indexDataPos,
        outDataPos, std::move(prevOperator), getOperatorID(),
        logicalIndexScan->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

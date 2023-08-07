#include "planner/logical_plan/scan/logical_scan_node.h"
#include "processor/operator/index_scan.h"
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

std::unique_ptr<PhysicalOperator> PlanMapper::mapIndexScanNode(LogicalOperator* logicalOperator) {
    auto logicalIndexScan = (LogicalIndexScanNode*)logicalOperator;
    auto inSchema = logicalIndexScan->getChild(0)->getSchema();
    auto outSchema = logicalIndexScan->getSchema();
    auto node = logicalIndexScan->getNode();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto nodeTable = storageManager.getNodesStore().getNodeTable(node->getSingleTableID());
    auto evaluator =
        expressionMapper.mapExpression(logicalIndexScan->getIndexExpression(), *inSchema);
    auto outDataPos = DataPos(outSchema->getExpressionPos(*node->getInternalIDProperty()));
    return make_unique<IndexScan>(nodeTable->getTableID(), nodeTable->getPKIndex(),
        std::move(evaluator), outDataPos, std::move(prevOperator), getOperatorID(),
        logicalIndexScan->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

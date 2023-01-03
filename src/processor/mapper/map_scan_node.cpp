#include "binder/expression/literal_expression.h"
#include "planner/logical_plan/logical_operator/logical_scan_node.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/index_scan.h"
#include "processor/operator/scan_node_id.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodeToPhysical(
    LogicalOperator* logicalOperator) {
    auto logicalScan = (LogicalScanNode*)logicalOperator;
    auto outSchema = logicalScan->getSchema();
    auto node = logicalScan->getNode();
    auto& nodesStore = storageManager.getNodesStore();
    auto dataPos = DataPos(outSchema->getExpressionPos(*node->getInternalIDProperty()));
    auto sharedState = make_shared<ScanNodeIDSharedState>();
    for (auto& tableID : node->getTableIDs()) {
        auto nodeTable = nodesStore.getNodeTable(tableID);
        sharedState->addTableState(nodeTable);
    }
    return make_unique<ScanNodeID>(node->getInternalIDPropertyName(), dataPos, sharedState,
        getOperatorID(), logicalScan->getExpressionsForPrinting());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalIndexScanNodeToPhysical(
    LogicalOperator* logicalOperator) {
    auto logicalIndexScan = (LogicalIndexScanNode*)logicalOperator;
    auto outSchema = logicalIndexScan->getSchema();
    auto inSchema = make_unique<Schema>();
    auto node = logicalIndexScan->getNode();
    auto nodeTable = storageManager.getNodesStore().getNodeTable(node->getSingleTableID());
    auto dataPos = DataPos(outSchema->getExpressionPos(*node->getInternalIDProperty()));
    auto evaluator =
        expressionMapper.mapExpression(logicalIndexScan->getIndexExpression(), *inSchema);
    return make_unique<IndexScan>(nodeTable->getTableID(), nodeTable->getPKIndex(),
        std::move(evaluator), dataPos, getOperatorID(),
        logicalIndexScan->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

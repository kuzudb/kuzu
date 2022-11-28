#include "binder/expression/literal_expression.h"
#include "planner/logical_plan/logical_operator/logical_scan_node.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/index_scan.h"
#include "processor/operator/scan_node_id.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodeToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalScan = (LogicalScanNode*)logicalOperator;
    auto node = logicalScan->getNode();
    auto& nodesStore = storageManager.getNodesStore();

    auto dataPos = mapperContext.getDataPos(node->getInternalIDPropertyName());
    mapperContext.addComputedExpressions(node->getInternalIDPropertyName());
    auto sharedState = make_shared<ScanNodeIDSharedState>();
    for (auto& tableID : node->getTableIDs()) {
        auto nodeTable = nodesStore.getNodeTable(tableID);
        sharedState->addTableState(nodeTable);
    }
    return make_unique<ScanNodeID>(mapperContext.getResultSetDescriptor()->copy(),
        node->getUniqueName(), dataPos, sharedState, getOperatorID(),
        logicalScan->getExpressionsForPrinting());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalIndexScanNodeToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalIndexScan = (LogicalIndexScanNode*)logicalOperator;
    auto node = logicalIndexScan->getNode();
    auto nodeTable = storageManager.getNodesStore().getNodeTable(node->getTableID());
    auto dataPos = mapperContext.getDataPos(node->getInternalIDPropertyName());
    auto evaluator =
        expressionMapper.mapExpression(logicalIndexScan->getIndexExpression(), mapperContext);
    mapperContext.addComputedExpressions(node->getInternalIDPropertyName());
    return make_unique<IndexScan>(mapperContext.getResultSetDescriptor()->copy(),
        nodeTable->getTableID(), nodeTable->getPKIndex(), std::move(evaluator), dataPos,
        getOperatorID(), logicalIndexScan->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

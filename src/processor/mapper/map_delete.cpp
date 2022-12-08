#include "binder/expression/node_expression.h"
#include "planner/logical_plan/logical_operator/logical_delete.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/update/delete.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalDeleteNodeToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalDeleteNode = (LogicalDeleteNode*)logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto& nodesStore = storageManager.getNodesStore();
    vector<unique_ptr<DeleteNodeInfo>> deleteNodeInfos;
    for (auto& [node, primaryKey] : logicalDeleteNode->getNodeAndPrimaryKeys()) {
        auto nodeTable = nodesStore.getNodeTable(node->getTableID());
        auto nodeIDPos = mapperContext.getDataPos(node->getInternalIDPropertyName());
        auto primaryKeyPos = mapperContext.getDataPos(primaryKey->getUniqueName());
        deleteNodeInfos.push_back(make_unique<DeleteNodeInfo>(nodeTable, nodeIDPos, primaryKeyPos));
    }
    return make_unique<DeleteNode>(std::move(deleteNodeInfos), std::move(prevOperator),
        getOperatorID(), logicalDeleteNode->getExpressionsForPrinting());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalDeleteRelToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalDeleteRel = (LogicalDeleteRel*)logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto& relStore = storageManager.getRelsStore();
    vector<unique_ptr<DeleteRelInfo>> createRelInfos;
    for (auto i = 0u; i < logicalDeleteRel->getNumRels(); ++i) {
        auto rel = logicalDeleteRel->getRel(i);
        auto table = relStore.getRelTable(rel->getTableID());
        auto srcNodePos = mapperContext.getDataPos(rel->getSrcNode()->getInternalIDPropertyName());
        auto srcNodeTableID = rel->getSrcNode()->getTableID();
        auto dstNodePos = mapperContext.getDataPos(rel->getDstNode()->getInternalIDPropertyName());
        auto dstNodeTableID = rel->getDstNode()->getTableID();
        auto relIDPos = mapperContext.getDataPos(rel->getInternalIDProperty()->getUniqueName());
        createRelInfos.push_back(make_unique<DeleteRelInfo>(
            table, srcNodePos, srcNodeTableID, dstNodePos, dstNodeTableID, relIDPos));
    }
    return make_unique<DeleteRel>(relStore.getRelsStatistics(), std::move(createRelInfos),
        std::move(prevOperator), getOperatorID(), logicalOperator->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

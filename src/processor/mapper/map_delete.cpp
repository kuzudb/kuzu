#include "include/plan_mapper.h"

#include "src/binder/expression/include/node_expression.h"
#include "src/planner/logical_plan/logical_operator/include/logical_delete.h"
#include "src/processor/operator/update/include/delete.h"

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
        auto nodeIDPos = mapperContext.getDataPos(node->getIDProperty());
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
        auto srcNodePos = mapperContext.getDataPos(rel->getSrcNode()->getIDProperty());
        auto srcNodeTableID = rel->getSrcNode()->getTableID();
        auto dstNodePos = mapperContext.getDataPos(rel->getDstNode()->getIDProperty());
        auto dstNodeTableID = rel->getDstNode()->getTableID();
        createRelInfos.push_back(make_unique<DeleteRelInfo>(
            table, srcNodePos, srcNodeTableID, dstNodePos, dstNodeTableID));
    }
    return make_unique<DeleteRel>(relStore.getRelsStatistics(), std::move(createRelInfos),
        std::move(prevOperator), getOperatorID(), logicalOperator->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

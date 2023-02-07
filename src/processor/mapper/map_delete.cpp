#include "binder/expression/node_expression.h"
#include "planner/logical_plan/logical_operator/logical_delete.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/update/delete.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalDeleteNodeToPhysical(
    LogicalOperator* logicalOperator) {
    auto logicalDeleteNode = (LogicalDeleteNode*)logicalOperator;
    auto inSchema = logicalDeleteNode->getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto& nodesStore = storageManager.getNodesStore();
    std::vector<std::unique_ptr<DeleteNodeInfo>> deleteNodeInfos;
    for (auto i = 0u; i < logicalDeleteNode->getNumNodes(); ++i) {
        auto node = logicalDeleteNode->getNode(i);
        auto primaryKey = logicalDeleteNode->getPrimaryKey(i);
        auto nodeTable = nodesStore.getNodeTable(node->getSingleTableID());
        auto nodeIDPos = DataPos(inSchema->getExpressionPos(*node->getInternalIDProperty()));
        auto primaryKeyPos = DataPos(inSchema->getExpressionPos(*primaryKey));
        deleteNodeInfos.push_back(
            std::make_unique<DeleteNodeInfo>(nodeTable, nodeIDPos, primaryKeyPos));
    }
    return std::make_unique<DeleteNode>(std::move(deleteNodeInfos), std::move(prevOperator),
        getOperatorID(), logicalDeleteNode->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalDeleteRelToPhysical(
    LogicalOperator* logicalOperator) {
    auto logicalDeleteRel = (LogicalDeleteRel*)logicalOperator;
    auto inSchema = logicalDeleteRel->getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto& relStore = storageManager.getRelsStore();
    std::vector<std::unique_ptr<DeleteRelInfo>> createRelInfos;
    for (auto i = 0u; i < logicalDeleteRel->getNumRels(); ++i) {
        auto rel = logicalDeleteRel->getRel(i);
        auto table = relStore.getRelTable(rel->getSingleTableID());
        auto srcNodePos =
            DataPos(inSchema->getExpressionPos(*rel->getSrcNode()->getInternalIDProperty()));
        auto srcNodeTableID = rel->getSrcNode()->getSingleTableID();
        auto dstNodePos =
            DataPos(inSchema->getExpressionPos(*rel->getDstNode()->getInternalIDProperty()));
        auto dstNodeTableID = rel->getDstNode()->getSingleTableID();
        auto relIDPos = DataPos(inSchema->getExpressionPos(*rel->getInternalIDProperty()));
        createRelInfos.push_back(std::make_unique<DeleteRelInfo>(
            table, srcNodePos, srcNodeTableID, dstNodePos, dstNodeTableID, relIDPos));
    }
    return std::make_unique<DeleteRel>(relStore.getRelsStatistics(), std::move(createRelInfos),
        std::move(prevOperator), getOperatorID(), logicalOperator->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

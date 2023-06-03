#include "binder/expression/node_expression.h"
#include "planner/logical_plan/logical_operator/logical_create.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/update/create.h"

using namespace kuzu::evaluator;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCreateNodeToPhysical(
    LogicalOperator* logicalOperator) {
    auto logicalCreateNode = (LogicalCreateNode*)logicalOperator;
    auto outSchema = logicalCreateNode->getSchema();
    auto inSchema = logicalCreateNode->getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto& nodesStore = storageManager.getNodesStore();
    auto catalogContent = catalog->getReadOnlyVersion();
    std::vector<std::unique_ptr<CreateNodeInfo>> createNodeInfos;
    for (auto i = 0u; i < logicalCreateNode->getNumNodes(); ++i) {
        auto node = logicalCreateNode->getNode(i);
        auto primaryKey = logicalCreateNode->getPrimaryKey(i);
        auto nodeTableID = node->getSingleTableID();
        auto table = nodesStore.getNodeTable(nodeTableID);
        auto primaryKeyEvaluator =
            primaryKey != nullptr ? expressionMapper.mapExpression(primaryKey, *inSchema) : nullptr;
        std::vector<RelTable*> relTablesToInit;
        for (auto& [relTableID, relTableSchema] : catalogContent->getRelTableSchemas()) {
            if (relTableSchema->isSrcOrDstTable(nodeTableID)) {
                relTablesToInit.push_back(storageManager.getRelsStore().getRelTable(relTableID));
            }
        }
        auto outDataPos = DataPos(outSchema->getExpressionPos(*node->getInternalIDProperty()));
        createNodeInfos.push_back(make_unique<CreateNodeInfo>(
            table, std::move(primaryKeyEvaluator), relTablesToInit, outDataPos));
    }
    return make_unique<CreateNode>(std::move(createNodeInfos), std::move(prevOperator),
        getOperatorID(), logicalCreateNode->getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCreateRelToPhysical(
    LogicalOperator* logicalOperator) {
    auto logicalCreateRel = (LogicalCreateRel*)logicalOperator;
    auto inSchema = logicalCreateRel->getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto& relStore = storageManager.getRelsStore();
    std::vector<std::unique_ptr<CreateRelInfo>> createRelInfos;
    for (auto i = 0u; i < logicalCreateRel->getNumRels(); ++i) {
        auto rel = logicalCreateRel->getRel(i);
        auto table = relStore.getRelTable(rel->getSingleTableID());
        auto srcNodePos =
            DataPos(inSchema->getExpressionPos(*rel->getSrcNode()->getInternalIDProperty()));
        auto srcNodeTableID = rel->getSrcNode()->getSingleTableID();
        auto dstNodePos =
            DataPos(inSchema->getExpressionPos(*rel->getDstNode()->getInternalIDProperty()));
        auto dstNodeTableID = rel->getDstNode()->getSingleTableID();
        std::vector<std::unique_ptr<BaseExpressionEvaluator>> evaluators;
        uint32_t relIDEvaluatorIdx = UINT32_MAX;
        auto setItems = logicalCreateRel->getSetItems(i);
        for (auto j = 0u; j < setItems.size(); ++j) {
            auto& [lhs, rhs] = setItems[j];
            auto propertyExpression = static_pointer_cast<binder::PropertyExpression>(lhs);
            if (propertyExpression->isInternalID()) {
                relIDEvaluatorIdx = j;
            }
            evaluators.push_back(expressionMapper.mapExpression(rhs, *inSchema));
        }
        assert(relIDEvaluatorIdx != UINT32_MAX);
        createRelInfos.push_back(make_unique<CreateRelInfo>(table, srcNodePos, srcNodeTableID,
            dstNodePos, dstNodeTableID, std::move(evaluators), relIDEvaluatorIdx));
    }
    return make_unique<CreateRel>(relStore.getRelsStatistics(), std::move(createRelInfos),
        std::move(prevOperator), getOperatorID(), logicalOperator->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

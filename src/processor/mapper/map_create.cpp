#include "binder/expression/node_expression.h"
#include "planner/logical_plan/logical_operator/logical_create.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/update/create.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCreateNodeToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalCreateNode = (LogicalCreateNode*)logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto& nodesStore = storageManager.getNodesStore();
    auto catalogContent = catalog->getReadOnlyVersion();
    vector<unique_ptr<CreateNodeInfo>> createNodeInfos;
    for (auto& [node, primaryKey] : logicalCreateNode->getNodeAndPrimaryKeys()) {
        auto nodeTableID = node->getTableID();
        auto table = nodesStore.getNodeTable(nodeTableID);
        auto primaryKeyEvaluator = expressionMapper.mapExpression(primaryKey, mapperContext);
        vector<RelTable*> relTablesToInit;
        for (auto& [relTableID, relTableSchema] : catalogContent->getRelTableSchemas()) {
            if (relTableSchema->edgeContainsNodeTable(nodeTableID)) {
                relTablesToInit.push_back(storageManager.getRelsStore().getRelTable(relTableID));
            }
        }
        auto outDataPos = mapperContext.getDataPos(node->getInternalIDPropertyName());
        createNodeInfos.push_back(make_unique<CreateNodeInfo>(
            table, std::move(primaryKeyEvaluator), relTablesToInit, outDataPos));
    }
    return make_unique<CreateNode>(std::move(createNodeInfos), std::move(prevOperator),
        getOperatorID(), logicalCreateNode->getExpressionsForPrinting());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCreateRelToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalCreateRel = (LogicalCreateRel*)logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto& relStore = storageManager.getRelsStore();
    vector<unique_ptr<CreateRelInfo>> createRelInfos;
    for (auto i = 0u; i < logicalCreateRel->getNumRels(); ++i) {
        auto rel = logicalCreateRel->getRel(i);
        auto table = relStore.getRelTable(rel->getTableID());
        auto srcNodePos = mapperContext.getDataPos(rel->getSrcNode()->getInternalIDPropertyName());
        auto srcNodeTableID = rel->getSrcNode()->getTableID();
        auto dstNodePos = mapperContext.getDataPos(rel->getDstNode()->getInternalIDPropertyName());
        auto dstNodeTableID = rel->getDstNode()->getTableID();
        vector<unique_ptr<BaseExpressionEvaluator>> evaluators;
        uint32_t relIDEvaluatorIdx = UINT32_MAX;
        auto setItems = logicalCreateRel->getSetItems(i);
        for (auto j = 0u; j < setItems.size(); ++j) {
            auto& [lhs, rhs] = setItems[j];
            auto propertyExpression = static_pointer_cast<PropertyExpression>(lhs);
            if (propertyExpression->isInternalID()) {
                relIDEvaluatorIdx = j;
            }
            evaluators.push_back(expressionMapper.mapExpression(rhs, mapperContext));
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

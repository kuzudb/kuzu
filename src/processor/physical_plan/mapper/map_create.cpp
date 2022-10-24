#include "src/binder/expression/include/node_expression.h"
#include "src/planner/logical_plan/logical_operator/include/logical_create.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/update/create.h"

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalCreateNodeToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalCreateNode = (LogicalCreateNode*)logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto& nodesStore = storageManager.getNodesStore();
    vector<unique_ptr<CreateNodeInfo>> createNodeInfos;
    for (auto& node : logicalCreateNode->getNodes()) {
        auto table = nodesStore.getNodeTable(node->getTableID());
        auto outDataPos = mapperContext.getDataPos(node->getIDProperty());
        createNodeInfos.push_back(make_unique<CreateNodeInfo>(table, outDataPos));
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
        auto srcNodePos = mapperContext.getDataPos(rel->getSrcNode()->getIDProperty());
        auto dstNodePos = mapperContext.getDataPos(rel->getDstNode()->getIDProperty());
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
        createRelInfos.push_back(make_unique<CreateRelInfo>(
            table, srcNodePos, dstNodePos, std::move(evaluators), relIDEvaluatorIdx));
    }
    return make_unique<CreateRel>(relStore.getRelsStatistics(), std::move(createRelInfos),
        std::move(prevOperator), getOperatorID(), logicalOperator->getExpressionsForPrinting());
}

} // namespace processor
} // namespace graphflow

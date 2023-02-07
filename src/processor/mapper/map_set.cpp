#include "binder/expression/node_expression.h"
#include "planner/logical_plan/logical_operator/logical_set.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/update/set.h"

using namespace kuzu::binder;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSetNodePropertyToPhysical(
    LogicalOperator* logicalOperator) {
    auto& logicalSetNodeProperty = (LogicalSetNodeProperty&)*logicalOperator;
    auto inSchema = logicalSetNodeProperty.getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto& nodeStore = storageManager.getNodesStore();
    std::vector<std::unique_ptr<SetNodePropertyInfo>> infos;
    for (auto i = 0u; i < logicalSetNodeProperty.getNumNodes(); ++i) {
        auto node = logicalSetNodeProperty.getNode(i);
        auto [lhs, rhs] = logicalSetNodeProperty.getSetItem(i);
        auto nodeIDPos = DataPos(inSchema->getExpressionPos(*node->getInternalIDProperty()));
        auto propertyExpression = static_pointer_cast<PropertyExpression>(lhs);
        auto nodeTableID = node->getSingleTableID();
        auto column = nodeStore.getNodePropertyColumn(
            nodeTableID, propertyExpression->getPropertyID(nodeTableID));
        auto evaluator = expressionMapper.mapExpression(rhs, *inSchema);
        infos.push_back(make_unique<SetNodePropertyInfo>(column, nodeIDPos, std::move(evaluator)));
    }
    return std::make_unique<SetNodeProperty>(std::move(infos), std::move(prevOperator),
        getOperatorID(), logicalSetNodeProperty.getExpressionsForPrinting());
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSetRelPropertyToPhysical(
    LogicalOperator* logicalOperator) {
    auto& logicalSetRelProperty = (LogicalSetRelProperty&)*logicalOperator;
    auto inSchema = logicalSetRelProperty.getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto& relStore = storageManager.getRelsStore();
    std::vector<std::unique_ptr<SetRelPropertyInfo>> infos;
    for (auto i = 0u; i < logicalSetRelProperty.getNumRels(); ++i) {
        auto rel = logicalSetRelProperty.getRel(i);
        auto [lhs, rhs] = logicalSetRelProperty.getSetItem(i);
        auto srcNodePos =
            DataPos(inSchema->getExpressionPos(*rel->getSrcNode()->getInternalIDProperty()));
        auto dstNodePos =
            DataPos(inSchema->getExpressionPos(*rel->getDstNode()->getInternalIDProperty()));
        auto relIDPos = DataPos(inSchema->getExpressionPos(*rel->getInternalIDProperty()));
        auto relTableID = rel->getSingleTableID();
        auto table = relStore.getRelTable(rel->getSingleTableID());
        auto propertyExpression = static_pointer_cast<PropertyExpression>(lhs);
        auto propertyId = propertyExpression->getPropertyID(relTableID);
        auto evaluator = expressionMapper.mapExpression(rhs, *inSchema);
        infos.push_back(make_unique<SetRelPropertyInfo>(
            table, srcNodePos, dstNodePos, relIDPos, propertyId, std::move(evaluator)));
    }
    return make_unique<SetRelProperty>(std::move(infos), std::move(prevOperator), getOperatorID(),
        logicalSetRelProperty.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

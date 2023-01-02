#include "binder/expression/node_expression.h"
#include "planner/logical_plan/logical_operator/logical_set.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/update/set.h"

using namespace kuzu::binder;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSetToPhysical(LogicalOperator* logicalOperator) {
    auto& logicalSetNodeProperty = (LogicalSetNodeProperty&)*logicalOperator;
    auto inSchema = logicalSetNodeProperty.getChild(0)->getSchema();
    auto setItems = logicalSetNodeProperty.getSetItems();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto& nodeStore = storageManager.getNodesStore();
    vector<unique_ptr<BaseExpressionEvaluator>> expressionEvaluators;
    for (auto& [_, target] : setItems) {
        expressionEvaluators.push_back(expressionMapper.mapExpression(target, *inSchema));
    }
    vector<DataPos> nodeIDVectorPositions;
    vector<Column*> propertyColumns;
    for (auto& [expr, _] : setItems) {
        auto property = static_pointer_cast<PropertyExpression>(expr);
        auto node = static_pointer_cast<NodeExpression>(property->getChild(0));
        auto nodeTableID = node->getSingleTableID();
        nodeIDVectorPositions.emplace_back(
            inSchema->getExpressionPos(*node->getInternalIDProperty()));
        propertyColumns.push_back(
            nodeStore.getNodePropertyColumn(nodeTableID, property->getPropertyID(nodeTableID)));
    }
    return make_unique<SetNodeStructuredProperty>(std::move(nodeIDVectorPositions),
        std::move(expressionEvaluators), std::move(propertyColumns), std::move(prevOperator),
        getOperatorID(), logicalSetNodeProperty.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

#include "include/plan_mapper.h"

#include "src/binder/expression/include/node_expression.h"
#include "src/planner/logical_plan/logical_operator/include/logical_set.h"
#include "src/processor/operator/update/include/set.h"

using namespace kuzu::binder;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSetToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalSetNodeProperty = (LogicalSetNodeProperty&)*logicalOperator;
    auto setItems = logicalSetNodeProperty.getSetItems();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto& nodeStore = storageManager.getNodesStore();
    vector<unique_ptr<BaseExpressionEvaluator>> expressionEvaluators;
    for (auto& [_, target] : setItems) {
        expressionEvaluators.push_back(expressionMapper.mapExpression(target, mapperContext));
    }
    vector<DataPos> nodeIDVectorPositions;
    if (logicalSetNodeProperty.getIsUnstructured()) {
        vector<pair<uint32_t, UnstructuredPropertyLists*>> propertyKeyListPairs;
        for (auto& [expr, _] : setItems) {
            auto property = static_pointer_cast<PropertyExpression>(expr);
            auto node = static_pointer_cast<NodeExpression>(property->getChild(0));
            nodeIDVectorPositions.push_back(mapperContext.getDataPos(node->getIDProperty()));
            propertyKeyListPairs.emplace_back(
                property->getPropertyID(), nodeStore.getNodeUnstrPropertyLists(node->getTableID()));
        }
        return make_unique<SetNodeUnstructuredProperty>(std::move(nodeIDVectorPositions),
            std::move(expressionEvaluators), std::move(propertyKeyListPairs),
            std::move(prevOperator), getOperatorID(),
            logicalSetNodeProperty.getExpressionsForPrinting());
    } else {
        vector<Column*> propertyColumns;
        for (auto& [expr, _] : setItems) {
            auto property = static_pointer_cast<PropertyExpression>(expr);
            auto node = static_pointer_cast<NodeExpression>(property->getChild(0));
            nodeIDVectorPositions.push_back(mapperContext.getDataPos(node->getIDProperty()));
            propertyColumns.push_back(
                nodeStore.getNodePropertyColumn(node->getTableID(), property->getPropertyID()));
        }
        return make_unique<SetNodeStructuredProperty>(std::move(nodeIDVectorPositions),
            std::move(expressionEvaluators), std::move(propertyColumns), std::move(prevOperator),
            getOperatorID(), logicalSetNodeProperty.getExpressionsForPrinting());
    }
}

} // namespace processor
} // namespace kuzu

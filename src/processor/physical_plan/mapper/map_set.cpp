#include "src/binder/expression/include/node_expression.h"
#include "src/planner/logical_plan/logical_operator/include/logical_set.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/update/set.h"

using namespace graphflow::binder;
using namespace graphflow::planner;

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSetToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalSet = (LogicalSet&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto& nodeStore = storageManager.getNodesStore();
    vector<DataPos> nodeIDVectorPositions;
    vector<Column*> propertyColumns;
    vector<unique_ptr<BaseExpressionEvaluator>> expressionEvaluators;
    for (auto& [expr, target] : logicalSet.getSetItems()) {
        assert(expr->expressionType == PROPERTY);
        auto propertyExpression = static_pointer_cast<PropertyExpression>(expr);
        assert(propertyExpression->getChild(0)->dataType.typeID == NODE);
        auto nodeExpression = static_pointer_cast<NodeExpression>(propertyExpression->getChild(0));
        nodeIDVectorPositions.push_back(mapperContext.getDataPos(nodeExpression->getIDProperty()));
        propertyColumns.push_back(nodeStore.getNodePropertyColumn(
            nodeExpression->getLabel(), propertyExpression->getPropertyKey()));
        expressionEvaluators.push_back(expressionMapper.mapExpression(target, mapperContext));
    }
    return make_unique<SetNodeStructuredProperty>(move(nodeIDVectorPositions),
        move(propertyColumns), move(expressionEvaluators), move(prevOperator), getOperatorID(),
        logicalSet.getExpressionsForPrinting());
}

} // namespace processor
} // namespace graphflow

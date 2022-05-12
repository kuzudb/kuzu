#include "src/binder/expression/include/node_expression.h"
#include "src/planner/logical_plan/logical_operator/include/logical_set.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/factorized_table_scan.h"
#include "src/processor/include/physical_plan/operator/update/set.h"

using namespace graphflow::binder;
using namespace graphflow::planner;

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSetToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalSet = (LogicalSet&)*logicalOperator;
    // append result collector
    auto child = logicalSet.getChild(0);
    auto childSchema = logicalSet.getSchemaBeforeSet();
    auto childMapperContext = MapperContext(make_unique<ResultSetDescriptor>(*childSchema));
    auto prevOperator = mapLogicalOperatorToPhysical(child, childMapperContext);
    auto resultCollector = appendResultCollector(
        childSchema->getExpressionsInScope(), *childSchema, move(prevOperator), childMapperContext);
    // append factorized table scan
    vector<DataType> outVecDataTypes;
    vector<DataPos> outDataPoses;
    for (auto& expression : childSchema->getExpressionsInScope()) {
        auto expressionName = expression->getUniqueName();
        outDataPoses.emplace_back(mapperContext.getDataPos(expressionName));
        // TODO(Xiyang): solve this together with issue #244
        outVecDataTypes.push_back(expression->getDataType().typeID == NODE_ID ?
                                      DataType(NODE) :
                                      expression->getDataType());
        mapperContext.addComputedExpressions(expressionName);
    }
    auto sharedState = resultCollector->getSharedState();
    auto fTableScan = make_unique<FactorizedTableScan>(
        mapperContext.getResultSetDescriptor()->copy(), move(outDataPoses), move(outVecDataTypes),
        sharedState, move(resultCollector), mapperContext.getOperatorID());
    // append set
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
        move(propertyColumns), move(expressionEvaluators), move(fTableScan),
        mapperContext.getOperatorID());
}

} // namespace processor
} // namespace graphflow

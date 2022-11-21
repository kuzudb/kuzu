#include "processor/mapper/plan_mapper.h"

#include "planner/logical_plan/logical_operator/logical_scan_node_property.h"
#include "processor/operator/scan_column/scan_structured_property.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodePropertyToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& scanProperty = (const LogicalScanNodeProperty&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto node = scanProperty.getNode();
    auto inputNodeIDVectorPos = mapperContext.getDataPos(node->getIDProperty());
    auto& nodeStore = storageManager.getNodesStore();
    vector<DataPos> outputPropertyVectorsPos;
    vector<Column*> propertyColumns;
    for (auto& expression : scanProperty.getProperties()) {
        auto property = static_pointer_cast<PropertyExpression>(expression);
        outputPropertyVectorsPos.push_back(mapperContext.getDataPos(property->getUniqueName()));
        mapperContext.addComputedExpressions(property->getUniqueName());
        propertyColumns.push_back(
            nodeStore.getNodePropertyColumn(node->getTableID(), property->getPropertyID()));
    }
    return make_unique<ScanStructuredProperty>(inputNodeIDVectorPos, move(outputPropertyVectorsPos),
        move(propertyColumns), move(prevOperator), getOperatorID(),
        scanProperty.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

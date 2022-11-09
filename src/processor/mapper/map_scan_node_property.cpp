#include "include/plan_mapper.h"

#include "src/planner/logical_plan/logical_operator/include/logical_scan_node_property.h"
#include "src/processor/operator/scan_column/include/scan_structured_property.h"
#include "src/processor/operator/scan_column/include/scan_unstructured_property.h"

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanNodePropertyToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& scanProperty = (const LogicalScanNodeProperty&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto inputNodeIDVectorPos = mapperContext.getDataPos(scanProperty.getNodeID());
    auto paramsString = scanProperty.getExpressionsForPrinting();
    vector<DataPos> outputPropertyVectorsPos;
    for (auto& propertyName : scanProperty.getPropertyNames()) {
        outputPropertyVectorsPos.push_back(mapperContext.getDataPos(propertyName));
        mapperContext.addComputedExpressions(propertyName);
    }
    auto& nodeStore = storageManager.getNodesStore();
    if (scanProperty.getIsUnstructured()) {
        auto lists = nodeStore.getNodeUnstrPropertyLists(scanProperty.getTableID());
        return make_unique<ScanUnstructuredProperty>(inputNodeIDVectorPos,
            move(outputPropertyVectorsPos), scanProperty.getPropertyIDs(), lists,
            move(prevOperator), getOperatorID(), paramsString);
    }
    vector<Column*> propertyColumns;
    for (auto& propertyID : scanProperty.getPropertyIDs()) {
        propertyColumns.push_back(
            nodeStore.getNodePropertyColumn(scanProperty.getTableID(), propertyID));
    }
    return make_unique<ScanStructuredProperty>(inputNodeIDVectorPos, move(outputPropertyVectorsPos),
        move(propertyColumns), move(prevOperator), getOperatorID(), paramsString);
}

} // namespace processor
} // namespace graphflow

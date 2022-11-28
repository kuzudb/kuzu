#include "planner/logical_plan/logical_operator/logical_scan_rel_property.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/scan_column/scan_column_property.h"
#include "processor/operator/scan_list/scan_rel_property_list.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanRelPropertyToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto scanRelProperty = (LogicalScanRelProperty*)logicalOperator;
    auto boundNode = scanRelProperty->getBoundNodeExpression();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto inputNodeIDVectorPos = mapperContext.getDataPos(boundNode->getInternalIDPropertyName());
    auto propertyName = scanRelProperty->getPropertyName();
    auto propertyID = scanRelProperty->getPropertyID();
    auto outputPropertyVectorPos = mapperContext.getDataPos(propertyName);
    mapperContext.addComputedExpressions(propertyName);
    auto& relStore = storageManager.getRelsStore();
    auto paramsString = scanRelProperty->getExpressionsForPrinting();
    if (scanRelProperty->getIsColumn()) {
        auto column = relStore.getRelPropertyColumn(scanRelProperty->getDirection(),
            scanRelProperty->getRelTableID(), boundNode->getTableID(), propertyID);
        return make_unique<ScanSingleTableProperties>(inputNodeIDVectorPos,
            vector<DataPos>{outputPropertyVectorPos}, vector<DataType>{column->dataType},
            vector<Column*>{column}, move(prevOperator), getOperatorID(), paramsString);
    }
    auto lists = relStore.getRelPropertyLists(scanRelProperty->getDirection(),
        boundNode->getTableID(), scanRelProperty->getRelTableID(), propertyID);
    return make_unique<ScanRelPropertyList>(inputNodeIDVectorPos, move(outputPropertyVectorPos),
        lists, move(prevOperator), getOperatorID(), paramsString);
}

} // namespace processor
} // namespace kuzu

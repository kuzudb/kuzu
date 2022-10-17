#include "src/planner/logical_plan/logical_operator/include/logical_scan_rel_property.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/read_list/read_rel_property_list.h"
#include "src/processor/include/physical_plan/operator/scan_column/scan_structured_property.h"

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanRelPropertyToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto scanRelProperty = (LogicalScanRelProperty*)logicalOperator;
    auto boundNode = scanRelProperty->getBoundNodeExpression();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto inputNodeIDVectorPos = mapperContext.getDataPos(boundNode->getIDProperty());
    auto propertyName = scanRelProperty->getPropertyName();
    auto propertyKey = scanRelProperty->getPropertyKey();
    auto outputPropertyVectorPos = mapperContext.getDataPos(propertyName);
    mapperContext.addComputedExpressions(propertyName);
    auto& relStore = storageManager.getRelsStore();
    auto paramsString = scanRelProperty->getExpressionsForPrinting();
    if (scanRelProperty->getIsColumn()) {
        auto column = relStore.getRelPropertyColumn(scanRelProperty->getDirection(),
            scanRelProperty->getRelTableID(), boundNode->getTableID(), propertyKey);
        return make_unique<ScanStructuredProperty>(inputNodeIDVectorPos,
            vector<DataPos>{outputPropertyVectorPos}, vector<Column*>{column}, move(prevOperator),
            getOperatorID(), paramsString);
    }
    auto lists = relStore.getRelPropertyLists(scanRelProperty->getDirection(),
        boundNode->getTableID(), scanRelProperty->getRelTableID(), propertyKey);
    return make_unique<ReadRelPropertyList>(inputNodeIDVectorPos, move(outputPropertyVectorPos),
        lists, move(prevOperator), getOperatorID(), paramsString);
}

} // namespace processor
} // namespace graphflow

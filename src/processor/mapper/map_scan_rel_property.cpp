#include "planner/logical_plan/logical_operator/logical_scan_rel_property.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/scan_column/scan_column_property.h"
#include "processor/operator/scan_list/scan_rel_property_list.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanRelPropertyToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto scanRelProperty = (LogicalScanRelProperty*)logicalOperator;
    auto boundNode = scanRelProperty->getBoundNode();
    auto rel = scanRelProperty->getRel();
    assert(
        !rel->isVariableLength() && rel->getNumTableIDs() == 1 && boundNode->getNumTableIDs() == 1);
    auto relID = rel->getTableID();
    auto direction = scanRelProperty->getDirection();
    auto propertyExpression = (PropertyExpression*)scanRelProperty->getProperty().get();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto inputNodeIDVectorPos = mapperContext.getDataPos(boundNode->getInternalIDPropertyName());
    auto outputPropertyVectorPos = mapperContext.getDataPos(propertyExpression->getUniqueName());
    mapperContext.addComputedExpressions(propertyExpression->getUniqueName());
    auto& relStore = storageManager.getRelsStore();
    if (relStore.hasAdjColumn(direction, boundNode->getTableID(), relID)) {
        auto column = relStore.getRelPropertyColumn(
            direction, relID, boundNode->getTableID(), propertyExpression->getPropertyID(relID));
        return make_unique<ScanSingleTableProperties>(inputNodeIDVectorPos,
            vector<DataPos>{outputPropertyVectorPos}, vector<Column*>{column}, move(prevOperator),
            getOperatorID(), scanRelProperty->getExpressionsForPrinting());
    } else {
        auto lists = relStore.getRelPropertyLists(
            direction, boundNode->getTableID(), relID, propertyExpression->getPropertyID(relID));
        return make_unique<ScanRelPropertyList>(inputNodeIDVectorPos, move(outputPropertyVectorPos),
            lists, move(prevOperator), getOperatorID(),
            scanRelProperty->getExpressionsForPrinting());
    }
}

} // namespace processor
} // namespace kuzu

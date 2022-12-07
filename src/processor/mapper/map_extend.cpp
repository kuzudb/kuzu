#include "planner/logical_plan/logical_operator/logical_extend.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/generic_extend.h"
#include "processor/operator/scan_column/adj_column_extend.h"
#include "processor/operator/scan_list/adj_list_extend.h"
#include "processor/operator/var_length_extend/var_length_adj_list_extend.h"
#include "processor/operator/var_length_extend/var_length_column_extend.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalExtendToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto extend = (LogicalExtend*)logicalOperator;
    auto boundNode = extend->getBoundNode();
    auto nbrNode = extend->getNbrNode();
    auto rel = extend->getRel();
    auto direction = extend->getDirection();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto inDataPos = mapperContext.getDataPos(boundNode->getInternalIDPropertyName());
    auto outDataPos = mapperContext.getDataPos(nbrNode->getInternalIDPropertyName());
    mapperContext.addComputedExpressions(nbrNode->getInternalIDPropertyName());
    auto& relsStore = storageManager.getRelsStore();
    auto boundNodeTableID = boundNode->getTableID();
    assert(rel->getNumTableIDs() == 1);
    auto relTableID = *rel->getTableIDs().begin();
    if (relsStore.hasAdjColumn(direction, boundNodeTableID, relTableID)) {
        auto column = relsStore.getAdjColumn(direction, boundNodeTableID, relTableID);
        if (rel->isVariableLength()) {
            return make_unique<VarLengthColumnExtend>(inDataPos, outDataPos, column,
                rel->getLowerBound(), rel->getUpperBound(), std::move(prevOperator),
                getOperatorID(), extend->getExpressionsForPrinting());
        } else {
            return make_unique<AdjColumnExtend>(inDataPos, outDataPos, column,
                std::move(prevOperator), getOperatorID(), extend->getExpressionsForPrinting());
        }
    } else {
        assert(relsStore.hasAdjList(direction, boundNodeTableID, relTableID));
        auto list = relsStore.getAdjLists(direction, boundNodeTableID, relTableID);
        if (rel->isVariableLength()) {
            return make_unique<VarLengthAdjListExtend>(inDataPos, outDataPos, list,
                rel->getLowerBound(), rel->getUpperBound(), std::move(prevOperator),
                getOperatorID(), extend->getExpressionsForPrinting());
        } else {
            return make_unique<AdjListExtend>(inDataPos, outDataPos, list, std::move(prevOperator),
                getOperatorID(), extend->getExpressionsForPrinting());
        }
    }
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalGenericExtendToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto extend = (LogicalGenericExtend*)logicalOperator;
    auto boundNode = extend->getBoundNode();
    auto nbrNode = extend->getNbrNode();
    auto rel = extend->getRel();
    auto direction = extend->getDirection();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto inDataPos = mapperContext.getDataPos(boundNode->getInternalIDPropertyName());
    auto outNodePos = mapperContext.getDataPos(nbrNode->getInternalIDPropertyName());
    mapperContext.addComputedExpressions(nbrNode->getInternalIDPropertyName());
    vector<DataPos> outPropertyVectorsPos;
    for (auto& expression : extend->getProperties()) {
        outPropertyVectorsPos.push_back(mapperContext.getDataPos(expression->getUniqueName()));
        mapperContext.addComputedExpressions(expression->getUniqueName());
    }
    auto& relsStore = storageManager.getRelsStore();
    auto boundNodeTableID = boundNode->getTableID();
    ColumnAndListCollection adjColumnAndListCollection;
    for (auto relTableID : rel->getTableIDs()) {
        if (relsStore.hasAdjColumn(direction, boundNodeTableID, relTableID)) {
            adjColumnAndListCollection.columns.push_back(
                relsStore.getAdjColumn(direction, boundNodeTableID, relTableID));
        }
        if (relsStore.hasAdjList(direction, boundNodeTableID, relTableID)) {
            adjColumnAndListCollection.lists.push_back(
                relsStore.getAdjLists(direction, boundNodeTableID, relTableID));
        }
    }
    vector<ColumnAndListCollection> propertyColumnAndListCollections;
    for (auto& expression : extend->getProperties()) {
        ColumnAndListCollection propertyColumnAndListCollection;
        auto propertyExpression = (PropertyExpression*)expression.get();
        for (auto relTableID : rel->getTableIDs()) {
            if (relsStore.hasAdjColumn(direction, boundNodeTableID, relTableID)) {
                Column* propertyColumn;
                if (!propertyExpression->hasPropertyID(relTableID)) {
                    propertyColumn = nullptr;
                } else {
                    propertyColumn = relsStore.getRelPropertyColumn(direction, relTableID,
                        boundNodeTableID, propertyExpression->getPropertyID(relTableID));
                }
                propertyColumnAndListCollection.columns.push_back(propertyColumn);
            }
            if (relsStore.hasAdjList(direction, boundNodeTableID, relTableID)) {
                Lists* propertyList;
                if (!propertyExpression->hasPropertyID(relTableID)) {
                    propertyList = nullptr;
                } else {
                    propertyList = relsStore.getRelPropertyLists(direction, boundNodeTableID,
                        relTableID, propertyExpression->getPropertyID(relTableID));
                }
                propertyColumnAndListCollection.lists.push_back(propertyList);
            }
        }
        propertyColumnAndListCollections.push_back(std::move(propertyColumnAndListCollection));
    }
    return make_unique<GenericExtend>(inDataPos, outNodePos, std::move(adjColumnAndListCollection),
        outPropertyVectorsPos, std::move(propertyColumnAndListCollections), std::move(prevOperator),
        getOperatorID(), extend->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

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
    assert(rel->getNumTableIDs() == 1 && boundNode->getNumTableIDs() == 1);
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

static unique_ptr<ColumnAndListCollection> populateAdjCollection(table_id_t boundNodeTableID,
    const RelExpression& rel, RelDirection direction, const RelsStore& relsStore) {
    vector<Column*> adjColumns;
    vector<Lists*> adjLists;
    for (auto relTableID : rel.getTableIDs()) {
        if (relsStore.hasAdjColumn(direction, boundNodeTableID, relTableID)) {
            adjColumns.push_back(relsStore.getAdjColumn(direction, boundNodeTableID, relTableID));
        }
        if (relsStore.hasAdjList(direction, boundNodeTableID, relTableID)) {
            adjLists.push_back(relsStore.getAdjLists(direction, boundNodeTableID, relTableID));
        }
    }
    return make_unique<ColumnAndListCollection>(std::move(adjColumns), std::move(adjLists));
}

static unique_ptr<ColumnAndListCollection> populatePropertyCollection(table_id_t boundNodeTableID,
    const RelExpression& rel, RelDirection direction, const PropertyExpression& propertyExpression,
    const RelsStore& relsStore) {
    vector<Column*> propertyColumns;
    vector<Lists*> propertyLists;
    for (auto relTableID : rel.getTableIDs()) {
        if (relsStore.hasAdjColumn(direction, boundNodeTableID, relTableID)) {
            Column* propertyColumn = nullptr;
            if (propertyExpression.hasPropertyID(relTableID)) {
                propertyColumn = relsStore.getRelPropertyColumn(direction, relTableID,
                    boundNodeTableID, propertyExpression.getPropertyID(relTableID));
            }
            propertyColumns.push_back(propertyColumn);
        }
        if (relsStore.hasAdjList(direction, boundNodeTableID, relTableID)) {
            Lists* propertyList = nullptr;
            if (propertyExpression.hasPropertyID(relTableID)) {
                propertyList = relsStore.getRelPropertyLists(direction, boundNodeTableID,
                    relTableID, propertyExpression.getPropertyID(relTableID));
            }
            propertyLists.push_back(propertyList);
        }
    }
    return make_unique<ColumnAndListCollection>(
        std::move(propertyColumns), std::move(propertyLists));
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
    unordered_map<table_id_t, unique_ptr<AdjAndPropertyCollection>>
        adjAndPropertyCollectionPerNodeTable;
    for (auto boundNodeTableID : boundNode->getTableIDs()) {
        auto adjCollection = populateAdjCollection(boundNodeTableID, *rel, direction, relsStore);
        vector<unique_ptr<ColumnAndListCollection>> propertyCollections;
        for (auto& expression : extend->getProperties()) {
            auto propertyExpression = (PropertyExpression*)expression.get();
            propertyCollections.push_back(populatePropertyCollection(
                boundNodeTableID, *rel, direction, *propertyExpression, relsStore));
        }
        adjAndPropertyCollectionPerNodeTable.insert(
            {boundNodeTableID, make_unique<AdjAndPropertyCollection>(
                                   std::move(adjCollection), std::move(propertyCollections))});
    }
    return make_unique<GenericExtend>(inDataPos, outNodePos, outPropertyVectorsPos,
        std::move(adjAndPropertyCollectionPerNodeTable), std::move(prevOperator), getOperatorID(),
        extend->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

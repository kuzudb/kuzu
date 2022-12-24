#include "planner/logical_plan/logical_operator/logical_extend.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/generic_extend.h"
#include "processor/operator/scan_column/adj_column_extend.h"
#include "processor/operator/scan_list/scan_rel_table_lists.h"
#include "processor/operator/var_length_extend/var_length_adj_list_extend.h"
#include "processor/operator/var_length_extend/var_length_column_extend.h"

namespace kuzu {
namespace processor {

static vector<Column*> populatePropertyColumns(table_id_t boundNodeTableID, table_id_t relID,
    RelDirection direction, const expression_vector& properties, const RelsStore& relsStore) {
    vector<Column*> propertyColumns;
    for (auto& expression : properties) {
        auto propertyExpression = (PropertyExpression*)expression.get();
        auto column = relsStore.getRelPropertyColumn(
            direction, relID, boundNodeTableID, propertyExpression->getPropertyID(relID));
        propertyColumns.push_back(column);
    }
    return propertyColumns;
}

static vector<Lists*> populatePropertyLists(table_id_t boundNodeTableID, table_id_t relID,
    RelDirection direction, const expression_vector& properties, const RelsStore& relsStore) {
    vector<Lists*> propertyLists;
    for (auto& expression : properties) {
        auto propertyExpression = (PropertyExpression*)expression.get();
        auto list = relsStore.getRelPropertyLists(
            direction, boundNodeTableID, relID, propertyExpression->getPropertyID(relID));
        propertyLists.push_back(list);
    }
    return propertyLists;
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

static vector<unique_ptr<ColumnAndListCollection>> populatePropertyCollections(
    table_id_t boundNodeTableID, const RelExpression& rel, RelDirection direction,
    const expression_vector& properties, const RelsStore& relsStore) {
    vector<unique_ptr<ColumnAndListCollection>> propertyCollections;
    for (auto& expression : properties) {
        auto propertyExpression = (PropertyExpression*)expression.get();
        propertyCollections.push_back(populatePropertyCollection(
            boundNodeTableID, rel, direction, *propertyExpression, relsStore));
    }
    return propertyCollections;
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalExtendToPhysical(
    LogicalOperator* logicalOperator) {
    auto extend = (LogicalExtend*)logicalOperator;
    auto outSchema = extend->getSchema();
    auto inSchema = extend->getChild(0)->getSchema();
    auto boundNode = extend->getBoundNode();
    auto nbrNode = extend->getNbrNode();
    auto rel = extend->getRel();
    auto direction = extend->getDirection();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto inNodeIDVectorPos =
        DataPos(inSchema->getExpressionPos(*boundNode->getInternalIDProperty()));
    auto outNodeIDVectorPos =
        DataPos(outSchema->getExpressionPos(*nbrNode->getInternalIDProperty()));
    vector<DataPos> outPropertyVectorsPos;
    for (auto& expression : extend->getProperties()) {
        outPropertyVectorsPos.emplace_back(outSchema->getExpressionPos(*expression));
    }
    auto& relsStore = storageManager.getRelsStore();
    if (!rel->isMultiLabeled() && !boundNode->isMultiLabeled()) {
        auto boundNodeTableID = boundNode->getSingleTableID();
        auto relTableID = rel->getSingleTableID();
        if (relsStore.hasAdjColumn(direction, boundNodeTableID, relTableID)) {
            auto adjColumn = relsStore.getAdjColumn(direction, boundNodeTableID, relTableID);
            if (rel->isVariableLength()) {
                return make_unique<VarLengthColumnExtend>(inNodeIDVectorPos, outNodeIDVectorPos,
                    adjColumn, rel->getLowerBound(), rel->getUpperBound(), std::move(prevOperator),
                    getOperatorID(), extend->getExpressionsForPrinting());
            } else {
                auto propertyColumns = populatePropertyColumns(
                    boundNodeTableID, relTableID, direction, extend->getProperties(), relsStore);
                return make_unique<ColumnExtendAndScanRelProperties>(inNodeIDVectorPos,
                    outNodeIDVectorPos, std::move(outPropertyVectorsPos), adjColumn,
                    std::move(propertyColumns), std::move(prevOperator), getOperatorID(),
                    extend->getExpressionsForPrinting());
            }
        } else {
            assert(relsStore.hasAdjList(direction, boundNodeTableID, relTableID));
            auto adjList = relsStore.getAdjLists(direction, boundNodeTableID, relTableID);
            if (rel->isVariableLength()) {
                return make_unique<VarLengthAdjListExtend>(inNodeIDVectorPos, outNodeIDVectorPos,
                    adjList, rel->getLowerBound(), rel->getUpperBound(), std::move(prevOperator),
                    getOperatorID(), extend->getExpressionsForPrinting());
            } else {
                auto propertyLists = populatePropertyLists(
                    boundNodeTableID, relTableID, direction, extend->getProperties(), relsStore);
                return make_unique<ScanRelTableLists>(inNodeIDVectorPos, outNodeIDVectorPos,
                    std::move(outPropertyVectorsPos), adjList, std::move(propertyLists),
                    std::move(prevOperator), getOperatorID(), extend->getExpressionsForPrinting());
            }
        }
    } else { // map to generic extend
        unordered_map<table_id_t, unique_ptr<AdjAndPropertyCollection>>
            adjAndPropertyCollectionPerNodeTable;
        for (auto boundNodeTableID : boundNode->getTableIDs()) {
            auto adjCollection =
                populateAdjCollection(boundNodeTableID, *rel, direction, relsStore);
            auto propertyCollections = populatePropertyCollections(
                boundNodeTableID, *rel, direction, extend->getProperties(), relsStore);
            adjAndPropertyCollectionPerNodeTable.insert(
                {boundNodeTableID, make_unique<AdjAndPropertyCollection>(
                                       std::move(adjCollection), std::move(propertyCollections))});
        }
        return make_unique<GenericExtendAndScanRelProperties>(inNodeIDVectorPos, outNodeIDVectorPos,
            outPropertyVectorsPos, std::move(adjAndPropertyCollectionPerNodeTable),
            std::move(prevOperator), getOperatorID(), extend->getExpressionsForPrinting());
    }
}

} // namespace processor
} // namespace kuzu

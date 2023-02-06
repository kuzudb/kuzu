#include "planner/logical_plan/logical_operator/logical_extend.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/scan/generic_scan_rel_tables.h"
#include "processor/operator/scan/scan_rel_table_columns.h"
#include "processor/operator/scan/scan_rel_table_lists.h"
#include "processor/operator/var_length_extend/var_length_adj_list_extend.h"
#include "processor/operator/var_length_extend/var_length_column_extend.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

static std::vector<property_id_t> populatePropertyIds(
    table_id_t relID, const expression_vector& properties) {
    std::vector<property_id_t> outputColumns;
    for (auto& expression : properties) {
        auto propertyExpression = (PropertyExpression*)expression.get();
        outputColumns.push_back(propertyExpression->getPropertyID(relID));
    }
    return outputColumns;
}

static std::unique_ptr<RelTableCollection> populateRelTableCollection(table_id_t boundNodeTableID,
    const RelExpression& rel, RelDirection direction, const expression_vector& properties,
    const RelsStore& relsStore) {
    std::vector<DirectedRelTableData*> tables;
    std::vector<std::unique_ptr<RelTableScanState>> tableScanStates;
    for (auto relTableID : rel.getTableIDs()) {
        auto relTable = relsStore.getRelTable(relTableID);
        if (!relTable->hasAdjColumn(direction, boundNodeTableID) &&
            !relTable->hasAdjList(direction, boundNodeTableID)) {
            continue;
        }
        tables.push_back(relsStore.getRelTable(relTableID)->getDirectedTableData(direction));
        std::vector<property_id_t> propertyIds;
        for (auto& property : properties) {
            auto propertyExpression = reinterpret_cast<PropertyExpression*>(property.get());
            propertyIds.push_back(propertyExpression->hasPropertyID(relTableID) ?
                                      propertyExpression->getPropertyID(relTableID) :
                                      INVALID_PROPERTY_ID);
        }
        tableScanStates.push_back(
            make_unique<RelTableScanState>(boundNodeTableID, std::move(propertyIds),
                relsStore.hasAdjColumn(direction, boundNodeTableID, relTableID) ?
                    RelTableDataType::COLUMNS :
                    RelTableDataType::LISTS));
    }
    return std::make_unique<RelTableCollection>(std::move(tables), std::move(tableScanStates));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalExtendToPhysical(
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
    std::vector<DataPos> outputVectorsPos;
    outputVectorsPos.push_back(outNodeIDVectorPos);
    for (auto& expression : extend->getProperties()) {
        outputVectorsPos.emplace_back(outSchema->getExpressionPos(*expression));
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
                auto propertyIds = populatePropertyIds(relTableID, extend->getProperties());
                return std::make_unique<ScanRelTableColumns>(boundNodeTableID,
                    relsStore.getRelTable(relTableID)->getDirectedTableData(direction),
                    std::move(propertyIds), inNodeIDVectorPos, std::move(outputVectorsPos),
                    std::move(prevOperator), getOperatorID(), extend->getExpressionsForPrinting());
            }
        } else {
            assert(relsStore.hasAdjList(direction, boundNodeTableID, relTableID));
            auto adjList = relsStore.getAdjLists(direction, boundNodeTableID, relTableID);
            if (rel->isVariableLength()) {
                return make_unique<VarLengthAdjListExtend>(inNodeIDVectorPos, outNodeIDVectorPos,
                    adjList, rel->getLowerBound(), rel->getUpperBound(), std::move(prevOperator),
                    getOperatorID(), extend->getExpressionsForPrinting());
            } else {
                auto propertyIds = populatePropertyIds(relTableID, extend->getProperties());
                return make_unique<ScanRelTableLists>(boundNodeTableID,
                    relsStore.getRelTable(relTableID)->getDirectedTableData(direction),
                    std::move(propertyIds), inNodeIDVectorPos, std::move(outputVectorsPos),
                    std::move(prevOperator), getOperatorID(), extend->getExpressionsForPrinting());
            }
        }
    } else { // map to generic extend
        std::unordered_map<table_id_t, std::unique_ptr<RelTableCollection>>
            relTableCollectionPerNodeTable;
        for (auto boundNodeTableID : boundNode->getTableIDs()) {
            auto relTableCollection = populateRelTableCollection(
                boundNodeTableID, *rel, direction, extend->getProperties(), relsStore);
            if (relTableCollection->getNumTablesInCollection() > 0) {
                relTableCollectionPerNodeTable.insert(
                    {boundNodeTableID, std::move(relTableCollection)});
            }
        }
        return std::make_unique<GenericScanRelTables>(inNodeIDVectorPos, outputVectorsPos,
            std::move(relTableCollectionPerNodeTable), std::move(prevOperator), getOperatorID(),
            extend->getExpressionsForPrinting());
    }
}

} // namespace processor
} // namespace kuzu

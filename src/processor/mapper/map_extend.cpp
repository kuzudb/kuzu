#include "planner/logical_plan/logical_operator/logical_extend.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/scan/generic_scan_rel_tables.h"
#include "processor/operator/scan/scan_rel_table_columns.h"
#include "processor/operator/scan/scan_rel_table_lists.h"

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

static std::pair<DirectedRelTableData*, std::unique_ptr<RelTableScanState>>
getRelTableDataAndScanState(RelDataDirection direction, catalog::RelTableSchema* relTableSchema,
    table_id_t boundNodeTableID, RelsStore& relsStore, table_id_t relTableID,
    const expression_vector& properties) {
    if (relTableSchema->getBoundTableID(direction) != boundNodeTableID) {
        // No data stored for given direction and boundNode.
        return std::make_pair(nullptr, nullptr);
    }
    auto relData = relsStore.getRelTable(relTableID)->getDirectedTableData(direction);
    std::vector<property_id_t> propertyIds;
    for (auto& property : properties) {
        auto propertyExpression = reinterpret_cast<PropertyExpression*>(property.get());
        propertyIds.push_back(propertyExpression->hasPropertyID(relTableID) ?
                                  propertyExpression->getPropertyID(relTableID) :
                                  INVALID_PROPERTY_ID);
    }
    auto relStats = relsStore.getRelsStatistics().getRelStatistics(relTableID);
    auto scanState = make_unique<RelTableScanState>(relStats, std::move(propertyIds),
        relsStore.isSingleMultiplicityInDirection(direction, relTableID) ?
            RelTableDataType::COLUMNS :
            RelTableDataType::LISTS);
    return std::make_pair(relData, std::move(scanState));
}

static std::unique_ptr<RelTableDataCollection> populateRelTableDataCollection(
    table_id_t boundNodeTableID, const RelExpression& rel, ExtendDirection extendDirection,
    const expression_vector& properties, RelsStore& relsStore, const catalog::Catalog& catalog) {
    std::vector<DirectedRelTableData*> relTableDatas;
    std::vector<std::unique_ptr<RelTableScanState>> tableScanStates;
    for (auto relTableID : rel.getTableIDs()) {
        auto relTableSchema = catalog.getReadOnlyVersion()->getRelTableSchema(relTableID);
        switch (extendDirection) {
        case ExtendDirection::FWD: {
            auto [relTableData, scanState] = getRelTableDataAndScanState(RelDataDirection::FWD,
                relTableSchema, boundNodeTableID, relsStore, relTableID, properties);
            if (relTableData != nullptr && scanState != nullptr) {
                relTableDatas.push_back(relTableData);
                tableScanStates.push_back(std::move(scanState));
            }
        } break;
        case ExtendDirection::BWD: {
            auto [relTableData, scanState] = getRelTableDataAndScanState(RelDataDirection::BWD,
                relTableSchema, boundNodeTableID, relsStore, relTableID, properties);
            if (relTableData != nullptr && scanState != nullptr) {
                relTableDatas.push_back(relTableData);
                tableScanStates.push_back(std::move(scanState));
            }
        } break;
        case ExtendDirection::BOTH: {
            auto [relTableDataFWD, scanStateFWD] =
                getRelTableDataAndScanState(RelDataDirection::FWD, relTableSchema, boundNodeTableID,
                    relsStore, relTableID, properties);
            if (relTableDataFWD != nullptr && scanStateFWD != nullptr) {
                relTableDatas.push_back(relTableDataFWD);
                tableScanStates.push_back(std::move(scanStateFWD));
            }
            auto [relTableDataBWD, scanStateBWD] =
                getRelTableDataAndScanState(RelDataDirection::BWD, relTableSchema, boundNodeTableID,
                    relsStore, relTableID, properties);
            if (relTableDataBWD != nullptr && scanStateBWD != nullptr) {
                relTableDatas.push_back(relTableDataBWD);
                tableScanStates.push_back(std::move(scanStateBWD));
            }
        } break;
        default:
            throw common::NotImplementedException("populateRelTableDataCollection");
        }
    }
    return std::make_unique<RelTableDataCollection>(
        std::move(relTableDatas), std::move(tableScanStates));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalExtendToPhysical(
    LogicalOperator* logicalOperator) {
    auto extend = (LogicalExtend*)logicalOperator;
    auto outSchema = extend->getSchema();
    auto inSchema = extend->getChild(0)->getSchema();
    auto boundNode = extend->getBoundNode();
    auto nbrNode = extend->getNbrNode();
    auto rel = extend->getRel();
    auto extendDirection = extend->getDirection();
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
    if (!rel->isMultiLabeled() && !boundNode->isMultiLabeled() &&
        extendDirection != planner::ExtendDirection::BOTH) {
        auto relDataDirection = ExtendDirectionUtils::getRelDataDirection(extendDirection);
        auto relTableID = rel->getSingleTableID();
        auto relTableStats = relsStore.getRelsStatistics().getRelStatistics(relTableID);
        if (relsStore.isSingleMultiplicityInDirection(relDataDirection, relTableID)) {
            auto propertyIds = populatePropertyIds(relTableID, extend->getProperties());
            return make_unique<ScanRelTableColumns>(
                relsStore.getRelTable(relTableID)->getDirectedTableData(relDataDirection),
                relTableStats, std::move(propertyIds), inNodeIDVectorPos,
                std::move(outputVectorsPos), std::move(prevOperator), getOperatorID(),
                extend->getExpressionsForPrinting());
        } else {
            assert(!relsStore.isSingleMultiplicityInDirection(relDataDirection, relTableID));
            auto propertyIds = populatePropertyIds(relTableID, extend->getProperties());
            return make_unique<ScanRelTableLists>(
                relsStore.getRelTable(relTableID)->getDirectedTableData(relDataDirection),
                relTableStats, std::move(propertyIds), inNodeIDVectorPos,
                std::move(outputVectorsPos), std::move(prevOperator), getOperatorID(),
                extend->getExpressionsForPrinting());
        }
    } else { // map to generic extend
        std::unordered_map<table_id_t, std::unique_ptr<RelTableDataCollection>>
            relTableCollectionPerNodeTable;
        for (auto boundNodeTableID : boundNode->getTableIDs()) {
            auto relTableCollection = populateRelTableDataCollection(boundNodeTableID, *rel,
                extendDirection, extend->getProperties(), relsStore, *catalog);
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

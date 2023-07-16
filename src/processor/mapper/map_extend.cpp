#include "planner/logical_plan/logical_operator/logical_extend.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/filter.h"
#include "processor/operator/scan/generic_scan_rel_tables.h"
#include "processor/operator/scan/scan_rel_table_columns.h"
#include "processor/operator/scan/scan_rel_table_lists.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;

namespace kuzu {
namespace processor {

static std::unique_ptr<RelTableScanInfo> getRelTableScanInfo(RelDataDirection direction,
    RelsStore& relsStore, table_id_t relTableID, const expression_vector& properties) {
    auto relTableDataType = relsStore.isSingleMultiplicityInDirection(direction, relTableID) ?
                                RelTableDataType::COLUMNS :
                                RelTableDataType::LISTS;
    auto relData = relsStore.getRelTable(relTableID)->getDirectedTableData(direction);
    std::vector<property_id_t> propertyIds;
    for (auto& property : properties) {
        auto propertyExpression = reinterpret_cast<PropertyExpression*>(property.get());
        propertyIds.push_back(propertyExpression->hasPropertyID(relTableID) ?
                                  propertyExpression->getPropertyID(relTableID) :
                                  INVALID_PROPERTY_ID);
    }
    auto relStats = relsStore.getRelsStatistics().getRelStatistics(relTableID);
    return std::make_unique<RelTableScanInfo>(
        relTableDataType, relData, relStats, std::move(propertyIds));
}

static std::unique_ptr<RelTableCollectionScanner> populateRelTableCollectionScanner(
    table_id_t boundNodeTableID, const RelExpression& rel, ExtendDirection extendDirection,
    const expression_vector& properties, RelsStore& relsStore, const catalog::Catalog& catalog) {
    std::vector<std::unique_ptr<RelTableScanInfo>> scanInfos;
    for (auto relTableID : rel.getTableIDs()) {
        auto relTableSchema = catalog.getReadOnlyVersion()->getRelTableSchema(relTableID);
        switch (extendDirection) {
        case ExtendDirection::FWD: {
            if (relTableSchema->getBoundTableID(RelDataDirection::FWD) == boundNodeTableID) {
                auto scanInfo =
                    getRelTableScanInfo(RelDataDirection::FWD, relsStore, relTableID, properties);
                if (scanInfo != nullptr) {
                    scanInfos.push_back(std::move(scanInfo));
                }
            }
        } break;
        case ExtendDirection::BWD: {
            if (relTableSchema->getBoundTableID(RelDataDirection::BWD) == boundNodeTableID) {
                auto scanInfo =
                    getRelTableScanInfo(RelDataDirection::BWD, relsStore, relTableID, properties);
                if (scanInfo != nullptr) {
                    scanInfos.push_back(std::move(scanInfo));
                }
            }
        } break;
        case ExtendDirection::BOTH: {
            if (relTableSchema->getBoundTableID(RelDataDirection::FWD) == boundNodeTableID) {
                auto scanInfoFWD =
                    getRelTableScanInfo(RelDataDirection::FWD, relsStore, relTableID, properties);
                if (scanInfoFWD != nullptr) {
                    scanInfos.push_back(std::move(scanInfoFWD));
                }
            }
            if (relTableSchema->getBoundTableID(RelDataDirection::BWD) == boundNodeTableID) {
                auto scanInfoBWD =
                    getRelTableScanInfo(RelDataDirection::BWD, relsStore, relTableID, properties);
                if (scanInfoBWD != nullptr) {
                    scanInfos.push_back(std::move(scanInfoBWD));
                }
            }
        } break;
        default:
            throw common::NotImplementedException("populateRelTableCollectionScanner");
        }
    }
    if (scanInfos.empty()) {
        return nullptr;
    }
    return std::make_unique<RelTableCollectionScanner>(std::move(scanInfos));
}

static std::unordered_set<common::table_id_t> getNodeIDFilterSet(const NodeExpression& node,
    const RelExpression& rel, ExtendDirection extendDirection, const catalog::Catalog& catalog) {
    std::unordered_set<common::table_id_t> nodeTableIDSet = node.getTableIDsSet();
    std::unordered_set<common::table_id_t> extendedNodeTableIDSet;
    for (auto tableID : rel.getTableIDs()) {
        auto tableSchema = catalog.getReadOnlyVersion()->getRelTableSchema(tableID);
        switch (extendDirection) {
        case ExtendDirection::FWD: {
            extendedNodeTableIDSet.insert(tableSchema->getNbrTableID(RelDataDirection::FWD));
        } break;
        case ExtendDirection::BWD: {
            extendedNodeTableIDSet.insert(tableSchema->getNbrTableID(RelDataDirection::BWD));
        } break;
        case ExtendDirection::BOTH: {
            extendedNodeTableIDSet.insert(tableSchema->getNbrTableID(RelDataDirection::FWD));
            extendedNodeTableIDSet.insert(tableSchema->getNbrTableID(RelDataDirection::BWD));
        } break;
        default:
            throw common::NotImplementedException("getNbrTableIDFilterSet");
        }
    }
    for (auto& tableID : extendedNodeTableIDSet) {
        if (!nodeTableIDSet.contains(tableID)) {
            return nodeTableIDSet; // Two sets are not equal. A post extend filter is needed.
        }
    }
    return std::unordered_set<common::table_id_t>{};
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
    auto inNodeVectorPos = DataPos(inSchema->getExpressionPos(*boundNode->getInternalIDProperty()));
    auto outNodeVectorPos = DataPos(outSchema->getExpressionPos(*nbrNode->getInternalIDProperty()));
    std::vector<DataPos> outVectorsPos;
    outVectorsPos.push_back(outNodeVectorPos);
    for (auto& expression : extend->getProperties()) {
        outVectorsPos.emplace_back(outSchema->getExpressionPos(*expression));
    }
    auto posInfo =
        std::make_unique<ScanRelTalePosInfo>(inNodeVectorPos, outNodeVectorPos, outVectorsPos);
    auto& relsStore = storageManager.getRelsStore();
    if (!rel->isMultiLabeled() && !boundNode->isMultiLabeled() &&
        extendDirection != planner::ExtendDirection::BOTH) {
        auto relDataDirection = ExtendDirectionUtils::getRelDataDirection(extendDirection);
        auto scanInfo = getRelTableScanInfo(
            relDataDirection, relsStore, rel->getSingleTableID(), extend->getProperties());
        if (scanInfo->relTableDataType == storage::RelTableDataType::COLUMNS) {
            return make_unique<ScanRelTableColumns>(std::move(scanInfo), std::move(posInfo),
                std::move(prevOperator), getOperatorID(), extend->getExpressionsForPrinting());
        } else {
            assert(scanInfo->relTableDataType == storage::RelTableDataType::LISTS);
            return make_unique<ScanRelTableLists>(std::move(scanInfo), std::move(posInfo),
                std::move(prevOperator), getOperatorID(), extend->getExpressionsForPrinting());
        }
    } else { // map to generic extend
        std::unordered_map<table_id_t, std::unique_ptr<RelTableCollectionScanner>> scanners;
        for (auto boundNodeTableID : boundNode->getTableIDs()) {
            auto scanner = populateRelTableCollectionScanner(boundNodeTableID, *rel,
                extendDirection, extend->getProperties(), relsStore, *catalog);
            if (scanner != nullptr) {
                scanners.insert({boundNodeTableID, std::move(scanner)});
            }
        }
        auto scanRel = std::make_unique<ScanMultiRelTable>(std::move(posInfo), std::move(scanners),
            std::move(prevOperator), getOperatorID(), extend->getExpressionsForPrinting());
        auto nbrNodeIDFilterSet = getNodeIDFilterSet(*nbrNode, *rel, extendDirection, *catalog);
        if (!nbrNodeIDFilterSet.empty()) {
            auto nbrNodeVectorPos =
                DataPos(outSchema->getExpressionPos(*nbrNode->getInternalIDProperty()));
            auto filterInfo =
                std::make_unique<NodeLabelFilterInfo>(nbrNodeVectorPos, nbrNodeIDFilterSet);
            auto filter = std::make_unique<NodeLabelFiler>(
                std::move(filterInfo), std::move(scanRel), getOperatorID(), "");
            return filter;
        } else {
            return scanRel;
        }
    }
}

} // namespace processor
} // namespace kuzu

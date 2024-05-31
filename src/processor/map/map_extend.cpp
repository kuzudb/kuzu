#include "binder/expression/property_expression.h"
#include "common/enums/extend_direction.h"
#include "planner/operator/extend/logical_extend.h"
#include "processor/operator/scan/scan_multi_rel_tables.h"
#include "processor/operator/scan/scan_rel_table.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::storage;
using namespace kuzu::catalog;

namespace kuzu {
namespace processor {

static ScanRelTableInfo getRelTableScanInfo(const TableCatalogEntry& tableCatalogEntry,
    RelDataDirection direction, RelTable* relTable, const expression_vector& properties,
    const std::vector<storage::ColumnPredicateSet>& columnPredicates) {
    auto relTableID = tableCatalogEntry.getTableID();
    std::vector<column_id_t> columnIDs;
    for (auto& expr : properties) {
        auto& property = expr->constCast<PropertyExpression>();
        if (property.hasPropertyID(relTableID)) {
            auto propertyID = property.getPropertyID(relTableID);
            columnIDs.push_back(tableCatalogEntry.getColumnID(propertyID));
        } else {
            columnIDs.push_back(INVALID_COLUMN_ID);
        }
    }
    return ScanRelTableInfo(relTable, direction, std::move(columnIDs),
        copyVector(columnPredicates));
}

static RelTableCollectionScanner populateRelTableCollectionScanner(table_id_t boundNodeTableID,
    const RelExpression& rel, ExtendDirection extendDirection, const expression_vector& properties,
    const std::vector<storage::ColumnPredicateSet>& columnPredicates,
    const main::ClientContext& clientContext) {
    std::vector<ScanRelTableInfo> scanInfos;
    auto catalog = clientContext.getCatalog();
    auto storageManager = clientContext.getStorageManager();
    for (auto relTableID : rel.getTableIDs()) {
        auto entry = catalog->getTableCatalogEntry(clientContext.getTx(), relTableID);
        auto& relTableEntry = entry->constCast<RelTableCatalogEntry>();
        auto relTable = storageManager->getTable(relTableID)->ptrCast<RelTable>();
        switch (extendDirection) {
        case ExtendDirection::FWD: {
            if (relTableEntry.getBoundTableID(RelDataDirection::FWD) == boundNodeTableID) {
                scanInfos.push_back(getRelTableScanInfo(relTableEntry, RelDataDirection::FWD,
                    relTable, properties, columnPredicates));
            }
        } break;
        case ExtendDirection::BWD: {
            if (relTableEntry.getBoundTableID(RelDataDirection::BWD) == boundNodeTableID) {
                scanInfos.push_back(getRelTableScanInfo(relTableEntry, RelDataDirection::BWD,
                    relTable, properties, columnPredicates));
            }
        } break;
        case ExtendDirection::BOTH: {
            if (relTableEntry.getBoundTableID(RelDataDirection::FWD) == boundNodeTableID) {
                scanInfos.push_back(getRelTableScanInfo(relTableEntry, RelDataDirection::FWD,
                    relTable, properties, columnPredicates));
            }
            if (relTableEntry.getBoundTableID(RelDataDirection::BWD) == boundNodeTableID) {
                scanInfos.push_back(getRelTableScanInfo(relTableEntry, RelDataDirection::BWD,
                    relTable, properties, columnPredicates));
            }
        } break;
        default:
            KU_UNREACHABLE;
        }
    }
    return RelTableCollectionScanner(std::move(scanInfos));
}

static bool scanSingleRelTable(const RelExpression& rel, const NodeExpression& boundNode,
    ExtendDirection extendDirection) {
    return !rel.isMultiLabeled() && !boundNode.isMultiLabeled() &&
           extendDirection != ExtendDirection::BOTH;
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapExtend(LogicalOperator* logicalOperator) {
    auto extend = logicalOperator->constPtrCast<LogicalExtend>();
    auto outFSchema = extend->getSchema();
    auto inFSchema = extend->getChild(0)->getSchema();
    auto boundNode = extend->getBoundNode();
    auto nbrNode = extend->getNbrNode();
    auto rel = extend->getRel();
    auto extendDirection = extend->getDirection();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto inNodeIDPos = getDataPos(*boundNode->getInternalID(), *inFSchema);
    auto outNodeIDPos = getDataPos(*nbrNode->getInternalID(), *outFSchema);
    std::vector<DataPos> outVectorsPos;
    outVectorsPos.push_back(outNodeIDPos);
    for (auto& expression : extend->getProperties()) {
        outVectorsPos.push_back(getDataPos(*expression, *outFSchema));
    }
    auto scanInfo = ScanTableInfo(inNodeIDPos, outVectorsPos);
    auto printInfo = std::make_unique<OPPrintInfo>(extend->getExpressionsForPrinting());
    if (scanSingleRelTable(*rel, *boundNode, extendDirection)) {
        auto relTableID = rel->getSingleTableID();
        auto entry =
            clientContext->getCatalog()->getTableCatalogEntry(clientContext->getTx(), relTableID);
        auto relDataDirection = ExtendDirectionUtil::getRelDataDirection(extendDirection);
        auto relTable =
            clientContext->getStorageManager()->getTable(relTableID)->ptrCast<RelTable>();
        auto scanRelInfo = getRelTableScanInfo(*entry, relDataDirection, relTable,
            extend->getProperties(), extend->getPropertyPredicates());
        return std::make_unique<ScanRelTable>(std::move(scanInfo), std::move(scanRelInfo),
            std::move(prevOperator), getOperatorID(), printInfo->copy());
    }
    // map to generic extend
    auto directionInfo = DirectionInfo();
    directionInfo.extendFromSource = extend->extendFromSourceNode();
    if (rel->hasDirectionExpr()) {
        directionInfo.directionPos = getDataPos(*rel->getDirectionExpr(), *outFSchema);
    }
    common::table_id_map_t<RelTableCollectionScanner> scanners;
    for (auto boundNodeTableID : boundNode->getTableIDs()) {
        auto scanner = populateRelTableCollectionScanner(boundNodeTableID, *rel, extendDirection,
            extend->getProperties(), extend->getPropertyPredicates(), *clientContext);
        if (!scanner.empty()) {
            scanners.insert({boundNodeTableID, std::move(scanner)});
        }
    }
    return std::make_unique<ScanMultiRelTable>(std::move(scanInfo), std::move(directionInfo),
        std::move(scanners), std::move(prevOperator), getOperatorID(), printInfo->copy());
}

} // namespace processor
} // namespace kuzu

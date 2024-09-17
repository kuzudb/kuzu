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
    RelDataDirection direction, RelTable* relTable, bool shouldScanNbrID,
    const expression_vector& properties, const std::vector<ColumnPredicateSet>& columnPredicates) {
    auto relTableID = tableCatalogEntry.getTableID();
    std::vector<column_id_t> columnIDs;
    // We always should scan nbrID from relTable. This is not a property in the schema label, so
    // cannot be bound to a column in the front-end.
    columnIDs.push_back(shouldScanNbrID ? NBR_ID_COLUMN_ID : INVALID_COLUMN_ID);
    for (auto& expr : properties) {
        auto& property = expr->constCast<PropertyExpression>();
        if (property.hasProperty(relTableID)) {
            columnIDs.push_back(tableCatalogEntry.getColumnID(property.getPropertyName()));
        } else {
            columnIDs.push_back(INVALID_COLUMN_ID);
        }
    }
    return ScanRelTableInfo(relTable, direction, std::move(columnIDs),
        copyVector(columnPredicates));
}

static RelTableCollectionScanner populateRelTableCollectionScanner(table_id_t boundNodeTableID,
    const table_id_set_t& nbrTableIDs, const RelExpression& rel, ExtendDirection extendDirection,
    bool shouldScanNbrID, const expression_vector& properties,
    const std::vector<ColumnPredicateSet>& columnPredicates,
    const main::ClientContext& clientContext) {
    std::vector<ScanRelTableInfo> scanInfos;
    const auto storageManager = clientContext.getStorageManager();
    for (auto entry : rel.getEntries()) {
        auto& relTableEntry = entry->constCast<RelTableCatalogEntry>();
        auto relTable = storageManager->getTable(entry->getTableID())->ptrCast<RelTable>();
        switch (extendDirection) {
        case ExtendDirection::FWD: {
            if (relTableEntry.getBoundTableID(RelDataDirection::FWD) == boundNodeTableID &&
                nbrTableIDs.contains(relTableEntry.getNbrTableID(RelDataDirection::FWD))) {
                scanInfos.push_back(getRelTableScanInfo(relTableEntry, RelDataDirection::FWD,
                    relTable, shouldScanNbrID, properties, columnPredicates));
            }
        } break;
        case ExtendDirection::BWD: {
            if (relTableEntry.getBoundTableID(RelDataDirection::BWD) == boundNodeTableID &&
                nbrTableIDs.contains(relTableEntry.getNbrTableID(RelDataDirection::BWD))) {
                scanInfos.push_back(getRelTableScanInfo(relTableEntry, RelDataDirection::BWD,
                    relTable, shouldScanNbrID, properties, columnPredicates));
            }
        } break;
        case ExtendDirection::BOTH: {
            if (relTableEntry.getBoundTableID(RelDataDirection::FWD) == boundNodeTableID &&
                nbrTableIDs.contains(relTableEntry.getNbrTableID(RelDataDirection::FWD))) {
                scanInfos.push_back(getRelTableScanInfo(relTableEntry, RelDataDirection::FWD,
                    relTable, shouldScanNbrID, properties, columnPredicates));
            }
            if (relTableEntry.getBoundTableID(RelDataDirection::BWD) == boundNodeTableID &&
                nbrTableIDs.contains(relTableEntry.getNbrTableID(RelDataDirection::BWD))) {
                scanInfos.push_back(getRelTableScanInfo(relTableEntry, RelDataDirection::BWD,
                    relTable, shouldScanNbrID, properties, columnPredicates));
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
    std::vector<DataPos> outVectorsPos;
    auto outNodeIDPos = getDataPos(*nbrNode->getInternalID(), *outFSchema);
    outVectorsPos.push_back(outNodeIDPos);
    for (auto& expression : extend->getProperties()) {
        outVectorsPos.push_back(getDataPos(*expression, *outFSchema));
    }
    auto scanInfo = ScanTableInfo(inNodeIDPos, outVectorsPos);
    std::vector<std::string> tableNames;
    auto storageManager = clientContext->getStorageManager();
    for (auto entry : rel->getEntries()) {
        auto relTable = storageManager->getTable(entry->getTableID())->ptrCast<RelTable>();
        tableNames.push_back(relTable->getTableName());
    }
    auto printInfo = std::make_unique<ScanRelTablePrintInfo>(tableNames, extend->getProperties(),
        boundNode, rel, nbrNode, extendDirection);
    if (scanSingleRelTable(*rel, *boundNode, extendDirection)) {
        auto entry = rel->getSingleEntry();
        auto relDataDirection = ExtendDirectionUtil::getRelDataDirection(extendDirection);
        auto relTable = storageManager->getTable(entry->getTableID())->ptrCast<RelTable>();
        auto scanRelInfo = getRelTableScanInfo(*entry, relDataDirection, relTable,
            extend->shouldScanNbrID(), extend->getProperties(), extend->getPropertyPredicates());
        return std::make_unique<ScanRelTable>(std::move(scanInfo), std::move(scanRelInfo),
            std::move(prevOperator), getOperatorID(), printInfo->copy());
    }
    // map to generic extend
    auto directionInfo = DirectionInfo();
    directionInfo.extendFromSource = extend->extendFromSourceNode();
    if (rel->hasDirectionExpr()) {
        directionInfo.directionPos = getDataPos(*rel->getDirectionExpr(), *outFSchema);
    }
    table_id_map_t<RelTableCollectionScanner> scanners;
    for (auto boundNodeTableID : boundNode->getTableIDs()) {
        auto scanner = populateRelTableCollectionScanner(boundNodeTableID,
            nbrNode->getTableIDsSet(), *rel, extendDirection, extend->shouldScanNbrID(),
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

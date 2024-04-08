#include "binder/expression/property_expression.h"
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

static std::unique_ptr<ScanRelTableInfo> getRelTableScanInfo(TableCatalogEntry* tableCatalogEntry,
    RelDataDirection direction, StorageManager* storageManager,
    const expression_vector& properties) {
    auto relTableID = tableCatalogEntry->getTableID();
    auto relTable = ku_dynamic_cast<Table*, RelTable*>(storageManager->getTable(relTableID));
    std::vector<column_id_t> columnIDs;
    for (auto& property : properties) {
        auto propertyExpression = ku_dynamic_cast<Expression*, PropertyExpression*>(property.get());
        columnIDs.push_back(
            propertyExpression->hasPropertyID(relTableID) ?
                tableCatalogEntry->getColumnID(propertyExpression->getPropertyID(relTableID)) :
                INVALID_COLUMN_ID);
    }
    return std::make_unique<ScanRelTableInfo>(relTable, direction, std::move(columnIDs));
}

static std::unique_ptr<RelTableCollectionScanner> populateRelTableCollectionScanner(
    table_id_t boundNodeTableID, const RelExpression& rel, ExtendDirection extendDirection,
    const expression_vector& properties, const main::ClientContext& clientContext) {
    std::vector<std::unique_ptr<ScanRelTableInfo>> scanInfos;
    auto catalog = clientContext.getCatalog();
    auto storageManager = clientContext.getStorageManager();
    for (auto relTableID : rel.getTableIDs()) {
        auto relTableEntry = ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(
            catalog->getTableCatalogEntry(clientContext.getTx(), relTableID));
        switch (extendDirection) {
        case ExtendDirection::FWD: {
            if (relTableEntry->getBoundTableID(RelDataDirection::FWD) == boundNodeTableID) {
                auto scanInfo = getRelTableScanInfo(relTableEntry, RelDataDirection::FWD,
                    storageManager, properties);
                if (scanInfo != nullptr) {
                    scanInfos.push_back(std::move(scanInfo));
                }
            }
        } break;
        case ExtendDirection::BWD: {
            if (relTableEntry->getBoundTableID(RelDataDirection::BWD) == boundNodeTableID) {
                auto scanInfo = getRelTableScanInfo(relTableEntry, RelDataDirection::BWD,
                    storageManager, properties);
                if (scanInfo != nullptr) {
                    scanInfos.push_back(std::move(scanInfo));
                }
            }
        } break;
        case ExtendDirection::BOTH: {
            if (relTableEntry->getBoundTableID(RelDataDirection::FWD) == boundNodeTableID) {
                auto scanInfoFWD = getRelTableScanInfo(relTableEntry, RelDataDirection::FWD,
                    storageManager, properties);
                if (scanInfoFWD != nullptr) {
                    scanInfos.push_back(std::move(scanInfoFWD));
                }
            }
            if (relTableEntry->getBoundTableID(RelDataDirection::BWD) == boundNodeTableID) {
                auto scanInfoBWD = getRelTableScanInfo(relTableEntry, RelDataDirection::BWD,
                    storageManager, properties);
                if (scanInfoBWD != nullptr) {
                    scanInfos.push_back(std::move(scanInfoBWD));
                }
            }
        } break;
        default:
            KU_UNREACHABLE;
        }
    }
    if (scanInfos.empty()) {
        return nullptr;
    }
    return std::make_unique<RelTableCollectionScanner>(std::move(scanInfos));
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapExtend(LogicalOperator* logicalOperator) {
    auto extend = (LogicalExtend*)logicalOperator;
    auto outFSchema = extend->getSchema();
    auto inFSchema = extend->getChild(0)->getSchema();
    auto boundNode = extend->getBoundNode();
    auto nbrNode = extend->getNbrNode();
    auto rel = extend->getRel();
    auto extendDirection = extend->getDirection();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto inNodeVectorPos = DataPos(inFSchema->getExpressionPos(*boundNode->getInternalID()));
    auto outNodeVectorPos = DataPos(outFSchema->getExpressionPos(*nbrNode->getInternalID()));
    std::vector<DataPos> outVectorsPos;
    outVectorsPos.push_back(outNodeVectorPos);
    for (auto& expression : extend->getProperties()) {
        outVectorsPos.emplace_back(outFSchema->getExpressionPos(*expression));
    }
    if (!rel->isMultiLabeled() && !boundNode->isMultiLabeled() &&
        extendDirection != ExtendDirection::BOTH) {
        auto relTableEntry = ku_dynamic_cast<TableCatalogEntry*, RelTableCatalogEntry*>(
            clientContext->getCatalog()->getTableCatalogEntry(clientContext->getTx(),
                rel->getSingleTableID()));
        auto relDataDirection = ExtendDirectionUtils::getRelDataDirection(extendDirection);
        auto scanInfo = getRelTableScanInfo(relTableEntry, relDataDirection,
            clientContext->getStorageManager(), extend->getProperties());
        return std::make_unique<ScanRelTable>(std::move(scanInfo), inNodeVectorPos, outVectorsPos,
            std::move(prevOperator), getOperatorID(), extend->getExpressionsForPrinting());
    } else { // map to generic extend
        std::unordered_map<table_id_t, std::unique_ptr<RelTableCollectionScanner>> scanners;
        for (auto boundNodeTableID : boundNode->getTableIDs()) {
            auto scanner = populateRelTableCollectionScanner(boundNodeTableID, *rel,
                extendDirection, extend->getProperties(), *clientContext);
            if (scanner != nullptr) {
                scanners.insert({boundNodeTableID, std::move(scanner)});
            }
        }
        return std::make_unique<ScanMultiRelTable>(std::move(scanners), inNodeVectorPos,
            outVectorsPos, std::move(prevOperator), getOperatorID(),
            extend->getExpressionsForPrinting());
    }
}

} // namespace processor
} // namespace kuzu

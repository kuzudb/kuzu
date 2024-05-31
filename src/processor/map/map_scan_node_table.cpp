#include "binder/expression/property_expression.h"
#include "planner/operator/scan/logical_scan_node_table.h"
#include "processor/operator/scan/offset_scan_node_table.h"
#include "processor/operator/scan/primary_key_scan_node_table.h"
#include "processor/operator/scan/scan_node_table.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapScanNodeTable(LogicalOperator* logicalOperator) {
    auto catalog = clientContext->getCatalog();
    auto storageManager = clientContext->getStorageManager();
    auto transaction = clientContext->getTx();
    auto& scan = logicalOperator->constCast<LogicalScanNodeTable>();
    const auto outSchema = scan.getSchema();
    auto nodeIDPos = getDataPos(*scan.getNodeID(), *outSchema);
    std::vector<DataPos> outVectorsPos;
    for (auto& expression : scan.getProperties()) {
        outVectorsPos.emplace_back(getDataPos(*expression, *outSchema));
    }
    auto scanInfo = ScanTableInfo(nodeIDPos, outVectorsPos);
    const auto tableIDs = scan.getTableIDs();
    std::vector<ScanNodeTableInfo> tableInfos;
    std::vector<std::shared_ptr<ScanNodeTableSharedState>> sharedStates;
    for (const auto& tableID : tableIDs) {
        std::vector<column_id_t> columnIDs;
        for (auto& expression : scan.getProperties()) {
            auto& property = expression->constCast<PropertyExpression>();
            if (!property.hasPropertyID(tableID)) {
                columnIDs.push_back(UINT32_MAX);
            } else {
                auto propertyID = property.getPropertyID(tableID);
                auto tableEntry = catalog->getTableCatalogEntry(transaction, tableID);
                columnIDs.push_back(tableEntry->getColumnID(propertyID));
            }
        }
        auto table = storageManager->getTable(tableID)->ptrCast<storage::NodeTable>();
        tableInfos.emplace_back(table, std::move(columnIDs),
            copyVector(scan.getPropertyPredicates()));
        sharedStates.push_back(std::make_shared<ScanNodeTableSharedState>());
    }

    switch (scan.getScanType()) {
    case LogicalScanNodeTableType::SCAN: {
        return std::make_unique<ScanNodeTable>(std::move(scanInfo), std::move(tableInfos),
            std::move(sharedStates), getOperatorID(), scan.getExpressionsForPrinting());
    }
    case LogicalScanNodeTableType::OFFSET_SCAN: {
        common::table_id_map_t<ScanNodeTableInfo> tableInfosMap;
        for (auto& info : tableInfos) {
            tableInfosMap.insert({info.table->getTableID(), info.copy()});
        }
        return std::make_unique<OffsetScanNodeTable>(std::move(scanInfo), std::move(tableInfosMap),
            getOperatorID(), scan.getExpressionsForPrinting());
    }
    case LogicalScanNodeTableType::PRIMARY_KEY_SCAN: {
        auto& primaryKeyScanInfo = scan.getExtraInfo()->constCast<PrimaryKeyScanInfo>();
        auto evaluator = ExpressionMapper::getEvaluator(primaryKeyScanInfo.key, outSchema);
        auto sharedState = std::make_shared<PrimaryKeyScanSharedState>(tableInfos.size());
        return std::make_unique<PrimaryKeyScanNodeTable>(std::move(scanInfo), std::move(tableInfos),
            std::move(evaluator), std::move(sharedState), getOperatorID(),
            scan.getExpressionsForPrinting());
    }
    default:
        KU_UNREACHABLE;
    }
}

} // namespace processor
} // namespace kuzu

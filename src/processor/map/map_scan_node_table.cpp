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
    std::vector<std::string> tableNames;
    for (const auto& tableID : tableIDs) {
        auto tableEntry = catalog->getTableCatalogEntry(transaction, tableID);
        std::vector<column_id_t> columnIDs;
        for (auto& expr : scan.getProperties()) {
            columnIDs.push_back(expr->constCast<PropertyExpression>().getColumnID(*tableEntry));
        }
        auto table = storageManager->getTable(tableID)->ptrCast<storage::NodeTable>();
        tableInfos.emplace_back(table, std::move(columnIDs),
            copyVector(scan.getPropertyPredicates()));
        tableNames.push_back(table->getTableName());
    }
    std::vector<std::shared_ptr<ScanNodeTableSharedState>> sharedStates;
    for (auto& tableID : tableIDs) {
        auto table = storageManager->getTable(tableID)->ptrCast<storage::NodeTable>();
        auto semiMask = std::make_unique<NodeVectorLevelSemiMask>(tableID, table->getNumRows());
        sharedStates.push_back(std::make_shared<ScanNodeTableSharedState>(std::move(semiMask)));
    }

    switch (scan.getScanType()) {
    case LogicalScanNodeTableType::SCAN: {
        auto printInfo = std::make_unique<ScanNodeTablePrintInfo>(tableNames, scan.getProperties());
        auto progressSharedState = std::make_shared<ScanNodeTableProgressSharedState>();
        return std::make_unique<ScanNodeTable>(std::move(scanInfo), std::move(tableInfos),
            std::move(sharedStates), getOperatorID(), std::move(printInfo), progressSharedState);
    }
    case LogicalScanNodeTableType::OFFSET_SCAN: {
        common::table_id_map_t<ScanNodeTableInfo> tableInfosMap;
        for (auto& info : tableInfos) {
            tableInfosMap.insert({info.table->getTableID(), info.copy()});
        }
        auto printInfo = std::make_unique<OPPrintInfo>();
        return std::make_unique<OffsetScanNodeTable>(std::move(scanInfo), std::move(tableInfosMap),
            getOperatorID(), std::move(printInfo));
    }
    case LogicalScanNodeTableType::PRIMARY_KEY_SCAN: {
        auto& primaryKeyScanInfo = scan.getExtraInfo()->constCast<PrimaryKeyScanInfo>();
        auto exprMapper = ExpressionMapper(outSchema);
        auto evaluator = exprMapper.getEvaluator(primaryKeyScanInfo.key);
        auto sharedState = std::make_shared<PrimaryKeyScanSharedState>(tableInfos.size());
        auto printInfo = std::make_unique<PrimaryKeyScanPrintInfo>(scan.getProperties(),
            primaryKeyScanInfo.key->toString());
        return std::make_unique<PrimaryKeyScanNodeTable>(std::move(scanInfo), std::move(tableInfos),
            std::move(evaluator), std::move(sharedState), getOperatorID(), std::move(printInfo));
    }
    default:
        KU_UNREACHABLE;
    }
}

} // namespace processor
} // namespace kuzu

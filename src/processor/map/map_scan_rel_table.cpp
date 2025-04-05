#include "binder/expression/property_expression.h"
#include "common/mask.h"
#include "planner/operator/scan/logical_scan_rel_table.h"
#include "processor/expression_mapper.h"
#include "processor/operator/scan/scan_rel_table.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapScanRelTable(
    const LogicalOperator* logicalOperator) {
    auto catalog = clientContext->getCatalog();
    auto storageManager = clientContext->getStorageManager();
    auto transaction = clientContext->getTransaction();
    auto& scan = logicalOperator->constCast<LogicalScanRelTable>();
    const auto outSchema = scan.getSchema();
    auto nodeIDPos = getDataPos(*scan.getNodeID(), *outSchema);
    std::vector<DataPos> outVectorsPos;
    for (auto& expression : scan.getProperties()) {
        outVectorsPos.emplace_back(getDataPos(*expression, *outSchema));
    }
    auto scanInfo = ScanTableInfo(nodeIDPos, outVectorsPos);
    const auto tableIDs = scan.getTableIDs();
    std::vector<ScanRelTableInfo> tableInfos;
    std::vector<std::string> tableNames;
    for (const auto& tableID : tableIDs) {
        auto tableEntry = catalog->getTableCatalogEntry(transaction, tableID);
        std::vector<column_id_t> columnIDs;
        for (auto& expr : scan.getProperties()) {
            auto columnID = expr->constCast<PropertyExpression>().getColumnID(*tableEntry) - 2;
            columnIDs.push_back(columnID);
        }
        auto table = storageManager->getTable(tableID)->ptrCast<storage::RelTable>();
        tableInfos.emplace_back(table, std::move(columnIDs),
            copyVector(scan.getPropertyPredicates()));
        tableNames.push_back(table->getTableName());
    }
    std::vector<std::shared_ptr<ScanRelTableSharedState>> sharedStates;
    for (auto& tableID : tableIDs) {
        auto table = storageManager->getTable(tableID)->ptrCast<storage::RelTable>();
        auto semiMask = SemiMaskUtil::createMask(table->getNumTotalRows(transaction));
        sharedStates.push_back(std::make_shared<ScanRelTableSharedState>(std::move(semiMask)));
    }
    auto alias = scan.getNodeID()->cast<PropertyExpression>().getRawVariableName();
    auto printInfo =
        std::make_unique<ScanRelTablePrintInfo>(tableNames, alias, scan.getProperties());
    auto progressSharedState = std::make_shared<ScanRelTableProgressSharedState>();
    return std::make_unique<ScanRelTable>(std::move(scanInfo), std::move(tableInfos),
        std::move(sharedStates), getOperatorID(), std::move(printInfo), progressSharedState);
}

} // namespace processor
} // namespace kuzu

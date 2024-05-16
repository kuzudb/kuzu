#include "binder/expression/property_expression.h"
#include "planner/operator/scan/logical_scan_node_table.h"
#include "processor/operator/scan/scan_node_table.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapScanNodeTable(LogicalOperator* logicalOperator) {
    const auto scanProperty =
        ku_dynamic_cast<LogicalOperator*, LogicalScanNodeTable*>(logicalOperator);
    const auto outSchema = scanProperty->getSchema();
    auto inputNodeIDVectorPos = DataPos(outSchema->getExpressionPos(*scanProperty->getNodeID()));
    std::vector<DataPos> outVectorsPos;
    for (auto& expression : scanProperty->getProperties()) {
        outVectorsPos.emplace_back(outSchema->getExpressionPos(*expression));
    }
    const auto tableIDs = scanProperty->getTableIDs();
    std::vector<std::unique_ptr<ScanNodeTableInfo>> tableInfos;
    std::vector<std::shared_ptr<ScanNodeTableSharedState>> sharedStates;
    for (const auto& tableID : tableIDs) {
        std::vector<column_id_t> columnIDs;
        for (auto& expression : scanProperty->getProperties()) {
            const auto property = static_pointer_cast<PropertyExpression>(expression);
            if (!property->hasPropertyID(tableID)) {
                columnIDs.push_back(UINT32_MAX);
            } else {
                columnIDs.push_back(clientContext->getCatalog()
                                        ->getTableCatalogEntry(clientContext->getTx(), tableID)
                                        ->getColumnID(property->getPropertyID(tableID)));
            }
        }
        tableInfos.push_back(std::make_unique<ScanNodeTableInfo>(
            ku_dynamic_cast<storage::Table*, storage::NodeTable*>(
                clientContext->getStorageManager()->getTable(tableID)),
            std::move(columnIDs)));
        sharedStates.push_back(std::make_shared<ScanNodeTableSharedState>());
    }
    return std::make_unique<ScanNodeTable>(inputNodeIDVectorPos, std::move(outVectorsPos),
        std::move(tableInfos), std::move(sharedStates), getOperatorID(),
        scanProperty->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

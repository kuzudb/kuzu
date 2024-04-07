#include "common/cast.h"
#include "planner/operator/scan/logical_scan_internal_id.h"
#include "processor/operator/scan_node_id.h"
#include "processor/plan_mapper.h"
#include "storage/storage_manager.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapScanInternalID(LogicalOperator* logicalOperator) {
    auto scan = common::ku_dynamic_cast<LogicalOperator*, LogicalScanInternalID*>(logicalOperator);
    auto outSchema = scan->getSchema();
    auto dataPos = DataPos(outSchema->getExpressionPos(*scan->getInternalID()));
    auto sharedState = std::make_shared<ScanNodeIDSharedState>();
    for (auto& tableID : scan->getTableIDs()) {
        auto nodeTable = common::ku_dynamic_cast<storage::Table*, storage::NodeTable*>(
            clientContext->getStorageManager()->getTable(tableID));
        sharedState->addTableState(nodeTable);
    }
    return std::make_unique<ScanNodeID>(dataPos, sharedState, getOperatorID(),
        scan->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

#include "planner/operator/scan/logical_scan_internal_id.h"
#include "processor/operator/scan_node_id.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapScanInternalID(LogicalOperator* logicalOperator) {
    auto scan = reinterpret_cast<LogicalScanInternalID*>(logicalOperator);
    auto outSchema = scan->getSchema();
    auto dataPos = DataPos(outSchema->getExpressionPos(*scan->getInternalID()));
    auto sharedState = std::make_shared<ScanNodeIDSharedState>();
    for (auto& tableID : scan->getTableIDs()) {
        auto nodeTable = storageManager.getNodeTable(tableID);
        sharedState->addTableState(nodeTable);
    }
    return std::make_unique<ScanNodeID>(
        dataPos, sharedState, getOperatorID(), scan->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

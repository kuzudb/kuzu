#include "planner/logical_plan/logical_operator/logical_recursive_extend.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/recursive_extend/scan_frontier.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalScanFrontierToPhysical(
    planner::LogicalOperator* logicalOperator) {
    auto scanFrontier = (LogicalScanFrontier*)logicalOperator;
    auto nodeID = scanFrontier->getNode()->getInternalIDProperty();
    auto nodeIDPos = DataPos(scanFrontier->getSchema()->getExpressionPos(*nodeID));
    return std::make_unique<ScanFrontier>(nodeIDPos, getOperatorID(), std::string());
}

} // namespace processor
} // namespace kuzu

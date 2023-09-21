#include "planner/operator/extend/logical_recursive_extend.h"
#include "processor/operator/recursive_extend/scan_frontier.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapScanFrontier(
    planner::LogicalOperator* logicalOperator) {
    auto scanFrontier = (LogicalScanFrontier*)logicalOperator;
    auto nodeID = scanFrontier->getNode()->getInternalID();
    auto nodeIDPos = DataPos(scanFrontier->getSchema()->getExpressionPos(*nodeID));
    return std::make_unique<ScanFrontier>(nodeIDPos, getOperatorID(), std::string());
}

} // namespace processor
} // namespace kuzu

#include "planner/operator/extend/logical_recursive_extend.h"
#include "processor/operator/recursive_extend/scan_frontier.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapScanFrontier(
    planner::LogicalOperator* logicalOperator) {
    auto scanFrontier = (LogicalScanFrontier*)logicalOperator;
    auto schema = scanFrontier->getSchema();
    auto nodeIDPos = DataPos(schema->getExpressionPos(*scanFrontier->getNodeID()));
    auto flagPos =
        DataPos(schema->getExpressionPos(*scanFrontier->getNodePredicateExecutionFlag()));
    return std::make_unique<ScanFrontier>(
        ScanFrontierInfo{nodeIDPos, flagPos}, getOperatorID(), std::string());
}

} // namespace processor
} // namespace kuzu

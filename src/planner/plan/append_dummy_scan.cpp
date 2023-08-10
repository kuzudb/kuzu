#include "planner/logical_plan/scan/logical_dummy_scan.h"
#include "planner/query_planner.h"

namespace kuzu {
namespace planner {

void QueryPlanner::appendDummyScan(LogicalPlan& plan) {
    assert(plan.isEmpty());
    auto dummyScan = std::make_shared<LogicalDummyScan>();
    dummyScan->computeFactorizedSchema();
    plan.setLastOperator(std::move(dummyScan));
}

} // namespace planner
} // namespace kuzu

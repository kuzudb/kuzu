#include "planner/operator/scan/logical_dummy_scan.h"
#include "planner/planner.h"

namespace kuzu {
namespace planner {

void Planner::appendDummyScan(LogicalPlan& plan) {
    KU_ASSERT(plan.isEmpty());
    auto printInfo = std::make_unique<OPPrintInfo>();
    auto dummyScan = std::make_shared<LogicalDummyScan>(std::move(printInfo));
    dummyScan->computeFactorizedSchema();
    plan.setLastOperator(std::move(dummyScan));
}

} // namespace planner
} // namespace kuzu

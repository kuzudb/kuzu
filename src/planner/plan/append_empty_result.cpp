#include "planner/operator/logical_empty_result.h"
#include "planner/planner.h"

namespace kuzu {
namespace planner {

void Planner::appendEmptyResult(LogicalPlan& plan) {
    auto printInfo = std::make_unique<OPPrintInfo>();
    auto op = std::make_shared<LogicalEmptyResult>(*plan.getSchema(), std::move(printInfo));
    op->computeFactorizedSchema();
    plan.setLastOperator(std::move(op));
}

} // namespace planner
} // namespace kuzu

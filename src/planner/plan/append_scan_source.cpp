#include "planner/operator/scan/logical_scan_source.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

std::shared_ptr<LogicalOperator> Planner::getScanSource(
    const binder::BoundTableScanSourceInfo& info) {
    return std::make_shared<LogicalScanSource>(info.copy(), nullptr /* offset */);
}

void Planner::appendScanSource(const BoundTableScanSourceInfo& info, LogicalPlan& plan) {
    appendScanSource(info, nullptr /* offset */, plan);
}

void Planner::appendScanSource(const binder::BoundTableScanSourceInfo& info,
    std::shared_ptr<Expression> offset, LogicalPlan& plan) {
    KU_ASSERT(plan.isEmpty());
    auto scanFile = std::make_shared<LogicalScanSource>(info.copy(), offset);
    scanFile->computeFactorizedSchema();
    plan.setLastOperator(std::move(scanFile));
}

} // namespace planner
} // namespace kuzu

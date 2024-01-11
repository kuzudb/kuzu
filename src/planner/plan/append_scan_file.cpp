#include "planner/operator/scan/logical_scan_file.h"
#include "planner/planner.h"

using namespace kuzu::binder;

namespace kuzu {
namespace planner {

void Planner::appendScanFile(const BoundFileScanInfo* fileScanInfo, LogicalPlan& plan) {
    KU_ASSERT(plan.isEmpty());
    auto scanFile = std::make_shared<LogicalScanFile>(fileScanInfo->copy());
    scanFile->computeFactorizedSchema();
    plan.setLastOperator(std::move(scanFile));
}

} // namespace planner
} // namespace kuzu

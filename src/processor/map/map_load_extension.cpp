#include "common/cast.h"
#include "planner/operator/logical_load_extension.h"
#include "processor/operator/load_extension.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLoadExtension(LogicalOperator* logicalOperator) {
    auto logicalLoadExtension =
        common::ku_dynamic_cast<LogicalOperator*, LogicalLoadExtension*>(logicalOperator);
    return std::make_unique<LoadExtension>(logicalLoadExtension->getPath(), getOperatorID(),
        logicalLoadExtension->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

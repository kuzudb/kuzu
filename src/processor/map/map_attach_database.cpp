#include "planner/operator/logical_attach_database.h"
#include "processor/operator/attach_database.h"
#include "processor/plan_mapper.h"

namespace kuzu {
namespace processor {

using namespace kuzu::planner;

std::unique_ptr<PhysicalOperator> PlanMapper::mapAttachDatabase(
    planner::LogicalOperator* logicalOperator) {
    auto attachDatabase =
        common::ku_dynamic_cast<LogicalOperator*, LogicalAttachDatabase*>(logicalOperator);
    return std::make_unique<AttachDatabase>(attachDatabase->getAttachInfo(), getOperatorID(),
        attachDatabase->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

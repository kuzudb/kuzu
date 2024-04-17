#include "planner/operator/logical_attach_database.h"
#include "processor/operator/attach_database.h"
#include "processor/plan_mapper.h"

namespace kuzu {
namespace processor {

using namespace kuzu::planner;

std::unique_ptr<PhysicalOperator> PlanMapper::mapAttachDatabase(
    planner::LogicalOperator* logicalOperator) {
    auto attachDatabase = logicalOperator->constPtrCast<LogicalAttachDatabase>();
    return std::make_unique<AttachDatabase>(attachDatabase->getAttachInfo(), getOperatorID(),
        attachDatabase->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

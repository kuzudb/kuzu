#include "planner/operator/logical_detach_database.h"
#include "processor/operator/detach_database.h"
#include "processor/plan_mapper.h"

namespace kuzu {
namespace processor {

using namespace kuzu::planner;

std::unique_ptr<PhysicalOperator> PlanMapper::mapDetachDatabase(
    planner::LogicalOperator* logicalOperator) {
    auto detachDatabase =
        common::ku_dynamic_cast<LogicalOperator*, LogicalDetachDatabase*>(logicalOperator);
    return std::make_unique<DetachDatabase>(detachDatabase->getDBName(), getOperatorID(),
        detachDatabase->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

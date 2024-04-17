#include "planner/operator/logical_use_database.h"
#include "processor/operator/use_database.h"
#include "processor/plan_mapper.h"

namespace kuzu {
namespace processor {

using namespace kuzu::planner;

std::unique_ptr<PhysicalOperator> PlanMapper::mapUseDatabase(
    planner::LogicalOperator* logicalOperator) {
    auto useDatabase = logicalOperator->constPtrCast<LogicalUseDatabase>();
    return std::make_unique<UseDatabase>(useDatabase->getDBName(), getOperatorID(),
        useDatabase->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

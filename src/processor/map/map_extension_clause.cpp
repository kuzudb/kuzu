#include "extension/extension_clause.h"
#include "processor/plan_mapper.h"

namespace kuzu {
namespace processor {

using namespace kuzu::planner;
using namespace kuzu::common;
using namespace kuzu::storage;

std::unique_ptr<PhysicalOperator> PlanMapper::mapExtensionClause(
    planner::LogicalOperator* logicalOperator) {
    auto logicalExtension = reinterpret_cast<extension::LogicalExtensionClause*>(logicalOperator);
    return logicalExtension->getExtensionClauseHandler().mapFunc(*logicalExtension,
        getOperatorID());
}

} // namespace processor
} // namespace kuzu

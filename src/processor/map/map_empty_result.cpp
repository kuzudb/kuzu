#include "processor/operator/empty_result.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapEmptyResult(LogicalOperator*) {
    return std::make_unique<EmptyResult>(getOperatorID(), "");
}

} // namespace processor
} // namespace kuzu

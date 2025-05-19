#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapDummySink(const LogicalOperator* logicalOperator) {
    auto child = mapOperator(logicalOperator->getChild(0).get());
    auto descriptor = std::make_unique<ResultSetDescriptor>(logicalOperator->getSchema());
    return std::make_unique<DummySink>(std::move(descriptor), std::move(child), getOperatorID());
}

} // namespace processor
} // namespace kuzu

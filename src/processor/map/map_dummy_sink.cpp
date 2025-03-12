#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapDummySink(const LogicalOperator* logicalOperator) {
    auto child = mapOperator(logicalOperator->getChild(0).get());
    return std::make_unique<DummySink>(
        std::make_unique<ResultSetDescriptor>(logicalOperator->getSchema()), std::move(child),
        getOperatorID(), std::make_unique<OPPrintInfo>());
}

} // namespace processor
} // namespace kuzu

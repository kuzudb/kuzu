#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::createDummySink(Schema* schema,
    std::unique_ptr<PhysicalOperator> prevOperator) {
    return std::make_unique<DummySink>(std::make_unique<ResultSetDescriptor>(schema),
        std::move(prevOperator), getOperatorID(), std::make_unique<OPPrintInfo>());
}

} // namespace processor
} // namespace kuzu

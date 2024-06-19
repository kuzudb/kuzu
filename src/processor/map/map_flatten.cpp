#include "planner/operator/logical_flatten.h"
#include "processor/operator/flatten.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapFlatten(LogicalOperator* logicalOperator) {
    auto& flatten = logicalOperator->constCast<LogicalFlatten>();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto printInfo = std::make_unique<OPPrintInfo>(flatten.getExpressionsForPrinting());
    return make_unique<Flatten>(flatten.getGroupPos(), std::move(prevOperator), getOperatorID(),
        std::move(printInfo));
}

} // namespace processor
} // namespace kuzu

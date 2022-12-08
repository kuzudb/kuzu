#include "planner/logical_plan/logical_operator/logical_skip.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/skip.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSkipToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalSkip = (const LogicalSkip&)*logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto dataChunkToSelectPos = logicalSkip.getGroupPosToSelect();
    return make_unique<Skip>(logicalSkip.getSkipNumber(), make_shared<atomic_uint64_t>(0),
        dataChunkToSelectPos, logicalSkip.getGroupsPosToSkip(), move(prevOperator), getOperatorID(),
        logicalSkip.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

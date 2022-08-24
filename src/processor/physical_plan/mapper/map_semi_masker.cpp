#include "src/planner/logical_plan/logical_operator/include/logical_semi_masker.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/scan_node_id.h"
#include "src/processor/include/physical_plan/operator/semi_masker.h"

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalSemiMaskerToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto logicalSemiMasker = (LogicalSemiMasker*)logicalOperator;
    auto node = logicalSemiMasker->getNode();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContext);
    auto keyDataPos = mapperContext.getDataPos(node->getIDProperty());
    return make_unique<SemiMasker>(node->getIDProperty(), keyDataPos, move(prevOperator),
        getOperatorID(), logicalSemiMasker->getExpressionsForPrinting());
}

} // namespace processor
} // namespace graphflow

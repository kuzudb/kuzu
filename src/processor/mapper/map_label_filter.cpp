#include "planner/logical_plan/logical_operator/logical_node_label_filter.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/filter.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalNodeLabelFilterToPhysical(
    LogicalOperator* logicalOperator) {
    auto logicalLabelFilter = (LogicalNodeLabelFilter*)logicalOperator;
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto schema = logicalOperator->getSchema();
    auto nbrNodeVectorPos = DataPos(schema->getExpressionPos(*logicalLabelFilter->getNodeID()));
    auto filterInfo = std::make_unique<NodeLabelFilterInfo>(
        nbrNodeVectorPos, logicalLabelFilter->getTableIDSet());
    return std::make_unique<NodeLabelFiler>(std::move(filterInfo), std::move(prevOperator),
        getOperatorID(), logicalLabelFilter->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

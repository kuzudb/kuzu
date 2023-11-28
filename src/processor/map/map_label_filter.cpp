#include <memory>
#include <utility>

#include "planner/operator/logical_node_label_filter.h"
#include "planner/operator/logical_operator.h"
#include "processor/data_pos.h"
#include "processor/operator/filter.h"
#include "processor/operator/physical_operator.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapNodeLabelFilter(LogicalOperator* logicalOperator) {
    auto logicalLabelFilter = (LogicalNodeLabelFilter*)logicalOperator;
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto schema = logicalOperator->getSchema();
    auto nbrNodeVectorPos = DataPos(schema->getExpressionPos(*logicalLabelFilter->getNodeID()));
    auto filterInfo = std::make_unique<NodeLabelFilterInfo>(
        nbrNodeVectorPos, logicalLabelFilter->getTableIDSet());
    return std::make_unique<NodeLabelFiler>(std::move(filterInfo), std::move(prevOperator),
        getOperatorID(), logicalLabelFilter->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

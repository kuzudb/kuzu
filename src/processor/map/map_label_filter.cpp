#include "planner/operator/logical_node_label_filter.h"
#include "processor/operator/filter.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapNodeLabelFilter(LogicalOperator* logicalOperator) {
    auto& logicalLabelFilter = logicalOperator->constCast<LogicalNodeLabelFilter>();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto schema = logicalOperator->getSchema();
    auto nbrNodeVectorPos = DataPos(schema->getExpressionPos(*logicalLabelFilter.getNodeID()));
    auto filterInfo =
        std::make_unique<NodeLabelFilterInfo>(nbrNodeVectorPos, logicalLabelFilter.getTableIDSet());
    auto printInfo = std::make_unique<OPPrintInfo>(logicalOperator->getExpressionsForPrinting());
    return std::make_unique<NodeLabelFiler>(std::move(filterInfo), std::move(prevOperator),
        getOperatorID(), std::move(printInfo));
}

} // namespace processor
} // namespace kuzu

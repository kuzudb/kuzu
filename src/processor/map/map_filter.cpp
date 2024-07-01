#include "planner/operator/logical_filter.h"
#include "processor/operator/filter.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapFilter(LogicalOperator* logicalOperator) {
    auto& logicalFilter = logicalOperator->constCast<LogicalFilter>();
    auto inSchema = logicalFilter.getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto physicalRootExpr = ExpressionMapper::getEvaluator(logicalFilter.getPredicate(), inSchema);
    auto printInfo = std::make_unique<OPPrintInfo>(logicalFilter.getExpressionsForPrinting());
    return make_unique<Filter>(std::move(physicalRootExpr), logicalFilter.getGroupPosToSelect(),
        std::move(prevOperator), getOperatorID(), std::move(printInfo));
}

} // namespace processor
} // namespace kuzu

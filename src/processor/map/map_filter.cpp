#include <memory>
#include <utility>

#include "planner/operator/logical_filter.h"
#include "planner/operator/logical_operator.h"
#include "processor/expression_mapper.h"
#include "processor/operator/filter.h"
#include "processor/operator/physical_operator.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapFilter(LogicalOperator* logicalOperator) {
    auto& logicalFilter = (const LogicalFilter&)*logicalOperator;
    auto inSchema = logicalFilter.getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto physicalRootExpr = ExpressionMapper::getEvaluator(logicalFilter.getPredicate(), inSchema);
    return make_unique<Filter>(std::move(physicalRootExpr), logicalFilter.getGroupPosToSelect(),
        std::move(prevOperator), getOperatorID(), logicalFilter.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

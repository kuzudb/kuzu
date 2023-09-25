#include "planner/operator/logical_fill_table_id.h"
#include "processor/operator/fill_table_id.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapFillTableID(LogicalOperator* logicalOperator) {
    auto fill = reinterpret_cast<LogicalFillTableID*>(logicalOperator);
    auto inSchema = fill->getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto internalIDPos = DataPos(inSchema->getExpressionPos(*fill->getInternalID()));
    return std::make_unique<FillTableID>(internalIDPos, fill->getTableID(), std::move(prevOperator),
        getOperatorID(), fill->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

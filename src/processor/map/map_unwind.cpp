#include "planner/operator/logical_unwind.h"
#include "processor/operator/physical_operator.h"
#include "processor/operator/unwind.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapUnwind(LogicalOperator* logicalOperator) {
    auto& unwind = logicalOperator->constCast<LogicalUnwind>();
    auto outSchema = unwind.getSchema();
    auto inSchema = unwind.getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto dataPos = DataPos(outSchema->getExpressionPos(*unwind.getOutExpr()));
    auto expressionEvaluator = ExpressionMapper::getEvaluator(unwind.getInExpr(), inSchema);
    DataPos idPos;
    if (unwind.hasIDExpr()) {
        idPos = getDataPos(*unwind.getIDExpr(), *outSchema);
    }
    auto printInfo = std::make_unique<OPPrintInfo>(unwind.getExpressionsForPrinting());
    return std::make_unique<Unwind>(dataPos, idPos, std::move(expressionEvaluator),
        std::move(prevOperator), getOperatorID(), std::move(printInfo));
}

} // namespace processor
} // namespace kuzu

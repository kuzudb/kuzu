#include "planner/operator/logical_cross_product.h"
#include "processor/operator/cross_product.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapCrossProduct(LogicalOperator* logicalOperator) {
    auto logicalCrossProduct = (LogicalCrossProduct*)logicalOperator;
    auto outSchema = logicalCrossProduct->getSchema();
    auto buildChild = logicalCrossProduct->getChild(1);
    // map build side
    auto buildSchema = buildChild->getSchema();
    auto buildSidePrevOperator = mapOperator(buildChild.get());
    auto expressions = buildSchema->getExpressionsInScope();
    auto resultCollector = createResultCollector(logicalCrossProduct->getAccumulateType(),
        expressions, buildSchema, std::move(buildSidePrevOperator));
    // map probe side
    auto probeSidePrevOperator = mapOperator(logicalCrossProduct->getChild(0).get());
    std::vector<DataPos> outVecPos;
    std::vector<uint32_t> colIndicesToScan;
    for (auto i = 0u; i < expressions.size(); ++i) {
        auto expression = expressions[i];
        outVecPos.emplace_back(outSchema->getExpressionPos(*expression));
        colIndicesToScan.push_back(i);
    }
    auto info =
        std::make_unique<CrossProductInfo>(std::move(outVecPos), std::move(colIndicesToScan));
    auto table = resultCollector->getResultFactorizedTable();
    auto maxMorselSize = table->hasUnflatCol() ? 1 : DEFAULT_VECTOR_CAPACITY;
    auto localState = std::make_unique<CrossProductLocalState>(table, maxMorselSize);
    return make_unique<CrossProduct>(std::move(info), std::move(localState),
        std::move(probeSidePrevOperator), std::move(resultCollector), getOperatorID(),
        logicalCrossProduct->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

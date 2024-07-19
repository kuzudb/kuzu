#include "planner/operator/logical_union.h"
#include "processor/operator/table_scan/union_all_scan.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapUnionAll(LogicalOperator* logicalOperator) {
    auto& logicalUnionAll = logicalOperator->constCast<LogicalUnion>();
    auto outSchema = logicalUnionAll.getSchema();
    // append result collectors to each child
    std::vector<std::unique_ptr<PhysicalOperator>> prevOperators;
    std::vector<std::shared_ptr<FactorizedTable>> tables;
    for (auto i = 0u; i < logicalOperator->getNumChildren(); ++i) {
        auto child = logicalOperator->getChild(i);
        auto childSchema = logicalUnionAll.getSchemaBeforeUnion(i);
        auto prevOperator = mapOperator(child.get());
        auto resultCollector = createResultCollector(AccumulateType::REGULAR,
            childSchema->getExpressionsInScope(), childSchema, std::move(prevOperator));
        tables.push_back(resultCollector->getResultFactorizedTable());
        prevOperators.push_back(std::move(resultCollector));
    }
    // append union all
    std::vector<DataPos> outputPositions;
    std::vector<uint32_t> columnIndices;
    auto expressionsToUnion = logicalUnionAll.getExpressionsToUnion();
    for (auto i = 0u; i < expressionsToUnion.size(); ++i) {
        auto expression = expressionsToUnion[i];
        outputPositions.emplace_back(outSchema->getExpressionPos(*expression));
        columnIndices.push_back(i);
    }
    auto info =
        std::make_unique<UnionAllScanInfo>(std::move(outputPositions), std::move(columnIndices));
    auto maxMorselSize = tables[0]->hasUnflatCol() ? 1 : DEFAULT_VECTOR_CAPACITY;
    auto unionSharedState = make_shared<UnionAllScanSharedState>(std::move(tables), maxMorselSize);
    auto printInfo = std::make_unique<UnionAllScanPrintInfo>(expressionsToUnion);
    return make_unique<UnionAllScan>(std::move(info), unionSharedState, std::move(prevOperators),
        getOperatorID(), std::move(printInfo));
}

} // namespace processor
} // namespace kuzu

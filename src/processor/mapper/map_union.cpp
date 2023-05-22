#include "planner/logical_plan/logical_operator/logical_union.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/table_scan/union_all_scan.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalUnionAllToPhysical(
    LogicalOperator* logicalOperator) {
    auto& logicalUnionAll = (LogicalUnion&)*logicalOperator;
    auto outSchema = logicalUnionAll.getSchema();
    // append result collectors to each child
    std::vector<std::unique_ptr<PhysicalOperator>> prevOperators;
    std::vector<std::shared_ptr<FTableSharedState>> resultCollectorSharedStates;
    for (auto i = 0u; i < logicalOperator->getNumChildren(); ++i) {
        auto child = logicalOperator->getChild(i);
        auto childSchema = logicalUnionAll.getSchemaBeforeUnion(i);
        auto prevOperator = mapLogicalOperatorToPhysical(child);
        auto resultCollector = appendResultCollector(
            childSchema->getExpressionsInScope(), childSchema, std::move(prevOperator));
        resultCollectorSharedStates.push_back(resultCollector->getSharedState());
        prevOperators.push_back(std::move(resultCollector));
    }
    // append union all
    std::vector<DataPos> outDataPoses;
    std::vector<uint32_t> colIndicesToScan;
    auto expressionsToUnion = logicalUnionAll.getExpressionsToUnion();
    for (auto i = 0u; i < expressionsToUnion.size(); ++i) {
        auto expression = expressionsToUnion[i];
        outDataPoses.emplace_back(outSchema->getExpressionPos(*expression));
        colIndicesToScan.push_back(i);
    }
    auto unionSharedState =
        make_shared<UnionAllScanSharedState>(std::move(resultCollectorSharedStates));
    return make_unique<UnionAllScan>(std::move(outDataPoses), std::move(colIndicesToScan),
        unionSharedState, std::move(prevOperators), getOperatorID(),
        logicalUnionAll.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

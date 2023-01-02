#include "planner/logical_plan/logical_operator/logical_accumulate.h"
#include "planner/logical_plan/logical_operator/logical_ftable_scan.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/result_collector.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalAccumulateToPhysical(
    LogicalOperator* logicalOperator) {
    auto logicalAccumulate = (LogicalAccumulate*)logicalOperator;
    auto outSchema = logicalAccumulate->getSchema();
    auto inSchema = logicalAccumulate->getSchemaBeforeSink();
    // append result collector
    auto prevOperator = mapLogicalOperatorToPhysical(logicalAccumulate->getChild(0));
    auto resultCollector = appendResultCollector(
        inSchema->getExpressionsInScope(), *inSchema, std::move(prevOperator));
    // append factorized table scan
    vector<DataPos> outDataPoses;
    vector<uint32_t> colIndicesToScan;
    auto expressions = logicalAccumulate->getExpressions();
    for (auto i = 0u; i < expressions.size(); ++i) {
        auto expression = expressions[i];
        outDataPoses.emplace_back(outSchema->getExpressionPos(*expression));
        colIndicesToScan.push_back(i);
    }
    auto sharedState = resultCollector->getSharedState();
    return make_unique<FactorizedTableScan>(std::move(outDataPoses), std::move(colIndicesToScan),
        sharedState, std::move(resultCollector), getOperatorID(),
        logicalAccumulate->getExpressionsForPrinting());
}

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalFTableScanToPhysical(
    LogicalOperator* logicalOperator) {
    auto logicalFTableScan = (LogicalFTableScan*)logicalOperator;
    auto outSchema = logicalFTableScan->getSchema();
    vector<DataPos> outDataPoses;
    vector<uint32_t> colIndicesToScan;
    for (auto& expression : logicalFTableScan->getExpressionsToScan()) {
        outDataPoses.emplace_back(outSchema->getExpressionPos(*expression));
        auto colIdx =
            ExpressionUtil::find(expression.get(), logicalFTableScan->getExpressionsAccumulated());
        assert(colIdx != UINT32_MAX);
        colIndicesToScan.push_back(colIdx);
    }
    return make_unique<FactorizedTableScan>(std::move(outDataPoses), std::move(colIndicesToScan),
        getOperatorID(), logicalFTableScan->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

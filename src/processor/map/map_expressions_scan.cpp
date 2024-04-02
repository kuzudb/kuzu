#include "planner/operator/logical_accumulate.h"
#include "planner/operator/scan/logical_expressions_scan.h"
#include "processor/operator/result_collector.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::binder;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapExpressionsScan(
    planner::LogicalOperator* logicalOperator) {
    auto expressionsScan = (planner::LogicalExpressionsScan*)logicalOperator;
    auto outerAccumulate = (planner::LogicalAccumulate*)expressionsScan->getOuterAccumulate();
    expression_map<ft_col_idx_t> materializedExpressionToColIdx;
    auto materializedExpressions = outerAccumulate->getPayloads();
    for (auto i = 0u; i < materializedExpressions.size(); ++i) {
        materializedExpressionToColIdx.insert({materializedExpressions[i], i});
    }
    auto expressionsToScan = expressionsScan->getExpressions();
    std::vector<ft_col_idx_t> colIndicesToScan;
    for (auto& expression : expressionsToScan) {
        KU_ASSERT(materializedExpressionToColIdx.contains(expression));
        colIndicesToScan.push_back(materializedExpressionToColIdx.at(expression));
    }
    auto schema = expressionsScan->getSchema();
    KU_ASSERT(logicalOpToPhysicalOpMap.contains(outerAccumulate));
    auto physicalOp = logicalOpToPhysicalOpMap.at(outerAccumulate);
    KU_ASSERT(physicalOp->getOperatorType() == PhysicalOperatorType::IN_QUERY_CALL);
    KU_ASSERT(physicalOp->getChild(0)->getOperatorType() == PhysicalOperatorType::RESULT_COLLECTOR);
    auto resultCollector = (ResultCollector*)physicalOp->getChild(0);
    auto table = resultCollector->getResultFactorizedTable();
    return createFTableScan(expressionsToScan, colIndicesToScan, schema, table,
        DEFAULT_VECTOR_CAPACITY /* maxMorselSize */);
}

} // namespace processor
} // namespace kuzu

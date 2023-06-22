#include "planner/logical_plan/logical_operator/logical_expressions_scan.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalExpressionsScanToPhysical(
    LogicalOperator* logicalOperator) {
    auto& logicalExpressionsScan = (LogicalExpressionsScan&)*logicalOperator;
    auto outSchema = logicalExpressionsScan.getSchema();
    auto inSchema = std::make_unique<Schema>();
    auto expressions = logicalExpressionsScan.getExpressions();
    auto tableSchema = std::make_unique<FactorizedTableSchema>();
    // TODO(Ziyi): remove vectors when we have done the refactor of dataChunk.
    std::vector<std::shared_ptr<ValueVector>> vectors;
    std::vector<ValueVector*> vectorsToAppend;
    for (auto& expression : expressions) {
        tableSchema->appendColumn(
            std::make_unique<ColumnSchema>(false, 0 /* all expressions are in the same datachunk */,
                LogicalTypeUtils::getRowLayoutSize(expression->dataType)));
        auto expressionEvaluator = expressionMapper.mapExpression(expression, *inSchema);
        // expression can be evaluated statically and does not require an actual resultset to init
        expressionEvaluator->init(ResultSet(0) /* dummy resultset */, memoryManager);
        expressionEvaluator->evaluate();
        vectors.push_back(expressionEvaluator->resultVector);
        vectorsToAppend.push_back(expressionEvaluator->resultVector.get());
    }
    auto sharedState = std::make_shared<FTableSharedState>(
        memoryManager, tableSchema->copy(), common::DEFAULT_VECTOR_CAPACITY);
    auto table = sharedState->getTable();
    table->append(vectorsToAppend);
    // map factorized table scan
    std::vector<DataPos> outDataPoses;
    std::vector<uint32_t> colIndicesToScan;
    for (auto i = 0u; i < expressions.size(); ++i) {
        auto expression = expressions[i];
        outDataPoses.emplace_back(outSchema->getExpressionPos(*expression));
        colIndicesToScan.push_back(i);
    }
    return std::make_unique<FactorizedTableScan>(std::move(outDataPoses),
        std::move(colIndicesToScan), sharedState, getOperatorID(),
        logicalExpressionsScan.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

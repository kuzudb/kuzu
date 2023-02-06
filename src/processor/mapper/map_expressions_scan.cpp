#include "common/vector/value_vector_utils.h"
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
    auto sharedState = std::make_shared<FTableSharedState>();
    // populate static table
    std::unique_ptr<FactorizedTableSchema> tableSchema = std::make_unique<FactorizedTableSchema>();
    std::vector<std::shared_ptr<ValueVector>> vectors;
    for (auto& expression : expressions) {
        tableSchema->appendColumn(
            std::make_unique<ColumnSchema>(false, 0 /* all expressions are in the same datachunk */,
                Types::getDataTypeSize(expression->dataType)));
        auto expressionEvaluator = expressionMapper.mapExpression(expression, *inSchema);
        // expression can be evaluated statically and does not require an actual resultset to init
        expressionEvaluator->init(ResultSet(0) /* dummy resultset */, memoryManager);
        expressionEvaluator->evaluate();
        vectors.push_back(expressionEvaluator->resultVector);
    }
    sharedState->initTableIfNecessary(memoryManager, std::move(tableSchema));
    auto table = sharedState->getTable();
    table->append(vectors);
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

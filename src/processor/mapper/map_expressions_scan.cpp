#include "common/vector/value_vector_utils.h"
#include "planner/logical_plan/logical_operator/logical_expressions_scan.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalExpressionsScanToPhysical(
    LogicalOperator* logicalOperator) {
    auto& logicalExpressionsScan = (LogicalExpressionsScan&)*logicalOperator;
    auto outSchema = logicalExpressionsScan.getSchema();
    auto inSchema = make_unique<Schema>();
    auto expressions = logicalExpressionsScan.getExpressions();
    auto sharedState = make_shared<FTableSharedState>();
    // populate static table
    unique_ptr<FactorizedTableSchema> tableSchema = make_unique<FactorizedTableSchema>();
    vector<shared_ptr<ValueVector>> vectors;
    for (auto& expression : expressions) {
        tableSchema->appendColumn(
            make_unique<ColumnSchema>(false, 0 /* all expressions are in the same datachunk */,
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
    vector<DataPos> outDataPoses;
    vector<uint32_t> colIndicesToScan;
    for (auto i = 0u; i < expressions.size(); ++i) {
        auto expression = expressions[i];
        outDataPoses.emplace_back(outSchema->getExpressionPos(*expression));
        colIndicesToScan.push_back(i);
    }
    return make_unique<FactorizedTableScan>(std::move(outDataPoses), std::move(colIndicesToScan),
        sharedState, getOperatorID(), logicalExpressionsScan.getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu

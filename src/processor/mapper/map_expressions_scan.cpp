#include "planner/logical_plan/logical_operator/logical_expressions_scan.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/table_scan/factorized_table_scan.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapExpressionsScan(LogicalOperator* logicalOperator) {
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
    auto table = std::make_shared<FactorizedTable>(memoryManager, std::move(tableSchema));
    table->append(vectorsToAppend);
    return createFactorizedTableScan(expressions, outSchema, table, nullptr);
}

} // namespace processor
} // namespace kuzu

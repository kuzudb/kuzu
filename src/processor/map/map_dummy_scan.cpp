#include "planner/operator/scan/logical_dummy_scan.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapDummyScan(LogicalOperator* /*logicalOperator*/) {
    auto inSchema = std::make_unique<Schema>();
    auto expression = LogicalDummyScan::getDummyExpression();
    auto tableSchema = std::make_unique<FactorizedTableSchema>();
    // TODO(Ziyi): remove vectors when we have done the refactor of dataChunk.
    std::vector<std::shared_ptr<ValueVector>> vectors;
    std::vector<ValueVector*> vectorsToAppend;
    tableSchema->appendColumn(
        std::make_unique<ColumnSchema>(false, 0 /* all expressions are in the same datachunk */,
            LogicalTypeUtils::getRowLayoutSize(expression->dataType)));
    auto expressionEvaluator = ExpressionMapper::getEvaluator(expression, inSchema.get());
    // expression can be evaluated statically and does not require an actual resultset to init
    expressionEvaluator->init(ResultSet(0) /* dummy resultset */, memoryManager);
    expressionEvaluator->evaluate(clientContext);
    vectors.push_back(expressionEvaluator->resultVector);
    vectorsToAppend.push_back(expressionEvaluator->resultVector.get());
    auto table = std::make_shared<FactorizedTable>(memoryManager, std::move(tableSchema));
    table->append(vectorsToAppend);
    return createEmptyFTableScan(table, 1 /* maxMorselSize */);
}

} // namespace processor
} // namespace kuzu

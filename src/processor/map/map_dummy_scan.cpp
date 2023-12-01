#include "planner/operator/scan/logical_dummy_scan.h"
#include "processor/operator/table_scan/factorized_table_scan.h"
#include "processor/plan_mapper.h"

using namespace kuzu::common;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapDummyScan(LogicalOperator* logicalOperator) {
    auto logicalDummyScan = (LogicalDummyScan*)logicalOperator;
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
    expressionEvaluator->evaluate();
    vectors.push_back(expressionEvaluator->resultVector);
    vectorsToAppend.push_back(expressionEvaluator->resultVector.get());
    auto table = std::make_shared<FactorizedTable>(memoryManager, std::move(tableSchema));
    table->append(vectorsToAppend);
    auto info = std::make_unique<FactorizedTableScanInfo>(
        std::vector<DataPos>{}, std::vector<ft_col_idx_t>{});
    auto sharedState =
        std::make_shared<FactorizedTableScanSharedState>(table, 1 /*maxMorselSize */);
    return make_unique<FactorizedTableScan>(std::move(info), sharedState, getOperatorID(), "");
}

} // namespace processor
} // namespace kuzu

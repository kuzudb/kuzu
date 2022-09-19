#include "src/binder/expression/include/literal_expression.h"
#include "src/common/include/vector/value_vector_utils.h"
#include "src/planner/logical_plan/logical_operator/include/logical_static_table_scan.h"
#include "src/processor/include/physical_plan/mapper/plan_mapper.h"
#include "src/processor/include/physical_plan/operator/factorized_table_scan.h"

namespace graphflow {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalTableScanToPhysical(
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalTableScan = (LogicalStaticTableScan&)*logicalOperator;
    auto sharedState = make_shared<FTableSharedState>();
    // populate static table
    unique_ptr<FactorizedTableSchema> tableSchema = make_unique<FactorizedTableSchema>();
    vector<shared_ptr<ValueVector>> vectors;
    for (auto& expression : logicalTableScan.getExpressions()) {
        tableSchema->appendColumn(make_unique<ColumnSchema>(false,
            mapperContext.getDataPos(expression->getUniqueName()).dataChunkPos,
            Types::getDataTypeSize(expression->dataType)));
        auto expressionEvaluator = expressionMapper.mapExpression(expression, mapperContext);
        // expression can be evaluated statically and does not require an actual resultset to init
        expressionEvaluator->init(ResultSet(0) /* dummy resultset */, memoryManager);
        expressionEvaluator->evaluate();
        vectors.push_back(expressionEvaluator->resultVector);
    }
    sharedState->initTableIfNecessary(memoryManager, move(tableSchema));
    auto table = sharedState->getTable();
    table->append(vectors);
    // map factorized table scan
    vector<DataType> outVecDataTypes;
    vector<DataPos> outDataPoses;
    vector<uint32_t> colIndicesToScan;
    auto expressions = logicalTableScan.getExpressions();
    for (auto i = 0u; i < expressions.size(); ++i) {
        auto expression = expressions[i];
        auto expressionName = expression->getUniqueName();
        outDataPoses.emplace_back(mapperContext.getDataPos(expressionName));
        outVecDataTypes.push_back(expression->getDataType());
        mapperContext.addComputedExpressions(expressionName);
        colIndicesToScan.push_back(i);
    }
    return make_unique<FactorizedTableScan>(mapperContext.getResultSetDescriptor()->copy(),
        std::move(outDataPoses), std::move(outVecDataTypes), std::move(colIndicesToScan),
        sharedState, vector<uint64_t>{}, getOperatorID(),
        logicalTableScan.getExpressionsForPrinting());
}

} // namespace processor
} // namespace graphflow

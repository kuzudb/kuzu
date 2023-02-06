#include "binder/expression/function_expression.h"
#include "planner/logical_plan/logical_operator/logical_aggregate.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/aggregate/hash_aggregate.h"
#include "processor/operator/aggregate/hash_aggregate_scan.h"
#include "processor/operator/aggregate/simple_aggregate.h"
#include "processor/operator/aggregate/simple_aggregate_scan.h"

using namespace kuzu::binder;
using namespace kuzu::common;
using namespace kuzu::function;
using namespace kuzu::planner;

namespace kuzu {
namespace processor {

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalAggregateToPhysical(
    LogicalOperator* logicalOperator) {
    auto& logicalAggregate = (const LogicalAggregate&)*logicalOperator;
    auto outSchema = logicalAggregate.getSchema();
    auto inSchema = logicalAggregate.getSchemaBeforeAggregate();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto paramsString = logicalAggregate.getExpressionsForPrinting();
    std::vector<DataPos> inputAggVectorsPos;
    std::vector<DataPos> outputAggVectorsPos;
    std::vector<DataType> outputAggVectorsDataTypes;
    std::vector<std::unique_ptr<AggregateFunction>> aggregateFunctions;
    for (auto& expression : logicalAggregate.getExpressionsToAggregate()) {
        if (expression->getNumChildren() == 0) {
            inputAggVectorsPos.emplace_back(UINT32_MAX, UINT32_MAX);
        } else {
            auto child = expression->getChild(0);
            inputAggVectorsPos.emplace_back(inSchema->getExpressionPos(*child));
        }
        aggregateFunctions.push_back(
            ((AggregateFunctionExpression&)*expression).aggregateFunction->clone());
        outputAggVectorsPos.emplace_back(outSchema->getExpressionPos(*expression));
        outputAggVectorsDataTypes.push_back(expression->dataType);
    }
    if (logicalAggregate.hasExpressionsToGroupBy()) {
        return createHashAggregate(std::move(aggregateFunctions), inputAggVectorsPos,
            outputAggVectorsPos, outputAggVectorsDataTypes,
            logicalAggregate.getExpressionsToGroupBy(), std::move(prevOperator), *inSchema,
            *outSchema, paramsString);
    } else {
        auto sharedState = make_shared<SimpleAggregateSharedState>(aggregateFunctions);
        auto aggregate = make_unique<SimpleAggregate>(
            std::make_unique<ResultSetDescriptor>(*inSchema), sharedState, inputAggVectorsPos,
            std::move(aggregateFunctions), std::move(prevOperator), getOperatorID(), paramsString);
        auto aggregateScan = make_unique<SimpleAggregateScan>(sharedState, outputAggVectorsPos,
            outputAggVectorsDataTypes, std::move(aggregate), getOperatorID(), paramsString);
        return aggregateScan;
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::createHashAggregate(
    std::vector<std::unique_ptr<AggregateFunction>> aggregateFunctions,
    std::vector<DataPos> inputAggVectorsPos, std::vector<DataPos> outputAggVectorsPos,
    std::vector<DataType> outputAggVectorsDataType, const expression_vector& groupByExpressions,
    std::unique_ptr<PhysicalOperator> prevOperator, const Schema& inSchema, const Schema& outSchema,
    const std::string& paramsString) {
    std::vector<DataPos> inputGroupByHashKeyVectorsPos;
    std::vector<DataPos> inputGroupByNonHashKeyVectorsPos;
    std::vector<bool> isInputGroupByHashKeyVectorFlat;
    std::vector<DataPos> outputGroupByKeyVectorsPos;
    std::vector<DataType> outputGroupByKeyVectorsDataTypeId;
    expression_vector groupByHashExpressions;
    expression_vector groupByNonHashExpressions;
    std::unordered_set<std::string> HashPrimaryKeysNodeId;
    for (auto& expressionToGroupBy : groupByExpressions) {
        if (expressionToGroupBy->expressionType == PROPERTY) {
            auto& propertyExpression = (const PropertyExpression&)(*expressionToGroupBy);
            if (propertyExpression.isInternalID()) {
                groupByHashExpressions.push_back(expressionToGroupBy);
                HashPrimaryKeysNodeId.insert(propertyExpression.getVariableName());
            } else if (HashPrimaryKeysNodeId.contains(propertyExpression.getVariableName())) {
                groupByNonHashExpressions.push_back(expressionToGroupBy);
            } else {
                groupByHashExpressions.push_back(expressionToGroupBy);
            }
        } else {
            groupByHashExpressions.push_back(expressionToGroupBy);
        }
    }
    appendGroupByExpressions(groupByHashExpressions, inputGroupByHashKeyVectorsPos,
        outputGroupByKeyVectorsPos, outputGroupByKeyVectorsDataTypeId, inSchema, outSchema,
        isInputGroupByHashKeyVectorFlat);
    appendGroupByExpressions(groupByNonHashExpressions, inputGroupByNonHashKeyVectorsPos,
        outputGroupByKeyVectorsPos, outputGroupByKeyVectorsDataTypeId, inSchema, outSchema,
        isInputGroupByHashKeyVectorFlat);
    auto sharedState = make_shared<HashAggregateSharedState>(aggregateFunctions);
    auto aggregate = make_unique<HashAggregate>(std::make_unique<ResultSetDescriptor>(inSchema),
        sharedState, inputGroupByHashKeyVectorsPos, inputGroupByNonHashKeyVectorsPos,
        isInputGroupByHashKeyVectorFlat, std::move(inputAggVectorsPos),
        std::move(aggregateFunctions), std::move(prevOperator), getOperatorID(), paramsString);
    auto aggregateScan = std::make_unique<HashAggregateScan>(sharedState,
        outputGroupByKeyVectorsPos, outputGroupByKeyVectorsDataTypeId,
        std::move(outputAggVectorsPos), std::move(outputAggVectorsDataType), std::move(aggregate),
        getOperatorID(), paramsString);
    return aggregateScan;
}

void PlanMapper::appendGroupByExpressions(const expression_vector& groupByExpressions,
    std::vector<DataPos>& inputGroupByHashKeyVectorsPos,
    std::vector<DataPos>& outputGroupByKeyVectorsPos,
    std::vector<DataType>& outputGroupByKeyVectorsDataTypes, const Schema& inSchema,
    const Schema& outSchema, std::vector<bool>& isInputGroupByHashKeyVectorFlat) {
    for (auto& expression : groupByExpressions) {
        if (inSchema.getGroup(expression->getUniqueName())->isFlat()) {
            inputGroupByHashKeyVectorsPos.emplace_back(inSchema.getExpressionPos(*expression));
            outputGroupByKeyVectorsPos.emplace_back(outSchema.getExpressionPos(*expression));
            outputGroupByKeyVectorsDataTypes.push_back(expression->dataType);
            isInputGroupByHashKeyVectorFlat.push_back(true);
        }
    }

    for (auto& expression : groupByExpressions) {
        if (!inSchema.getGroup(expression->getUniqueName())->isFlat()) {
            inputGroupByHashKeyVectorsPos.emplace_back(inSchema.getExpressionPos(*expression));
            outputGroupByKeyVectorsPos.emplace_back(outSchema.getExpressionPos(*expression));
            outputGroupByKeyVectorsDataTypes.push_back(expression->dataType);
            isInputGroupByHashKeyVectorFlat.push_back(false);
        }
    }
}

} // namespace processor
} // namespace kuzu

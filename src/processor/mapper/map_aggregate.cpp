#include "binder/expression/function_expression.h"
#include "planner/logical_plan/logical_operator/logical_aggregate.h"
#include "processor/mapper/plan_mapper.h"
#include "processor/operator/aggregate/hash_aggregate.h"
#include "processor/operator/aggregate/hash_aggregate_scan.h"
#include "processor/operator/aggregate/simple_aggregate.h"
#include "processor/operator/aggregate/simple_aggregate_scan.h"

namespace kuzu {
namespace processor {

unique_ptr<PhysicalOperator> PlanMapper::mapLogicalAggregateToPhysical(
    LogicalOperator* logicalOperator) {
    auto& logicalAggregate = (const LogicalAggregate&)*logicalOperator;
    auto outSchema = logicalAggregate.getSchema();
    auto inSchema = logicalAggregate.getSchemaBeforeAggregate();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto paramsString = logicalAggregate.getExpressionsForPrinting();
    vector<DataPos> inputAggVectorsPos;
    vector<DataPos> outputAggVectorsPos;
    vector<DataType> outputAggVectorsDataTypes;
    vector<unique_ptr<AggregateFunction>> aggregateFunctions;
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
        auto aggregate = make_unique<SimpleAggregate>(make_unique<ResultSetDescriptor>(*inSchema),
            sharedState, inputAggVectorsPos, std::move(aggregateFunctions), std::move(prevOperator),
            getOperatorID(), paramsString);
        auto aggregateScan = make_unique<SimpleAggregateScan>(sharedState, outputAggVectorsPos,
            outputAggVectorsDataTypes, std::move(aggregate), getOperatorID(), paramsString);
        return aggregateScan;
    }
}

unique_ptr<PhysicalOperator> PlanMapper::createHashAggregate(
    vector<unique_ptr<AggregateFunction>> aggregateFunctions, vector<DataPos> inputAggVectorsPos,
    vector<DataPos> outputAggVectorsPos, vector<DataType> outputAggVectorsDataType,
    const expression_vector& groupByExpressions, unique_ptr<PhysicalOperator> prevOperator,
    const Schema& inSchema, const Schema& outSchema, const string& paramsString) {
    vector<DataPos> inputGroupByHashKeyVectorsPos;
    vector<DataPos> inputGroupByNonHashKeyVectorsPos;
    vector<bool> isInputGroupByHashKeyVectorFlat;
    vector<DataPos> outputGroupByKeyVectorsPos;
    vector<DataType> outputGroupByKeyVectorsDataTypeId;
    expression_vector groupByHashExpressions;
    expression_vector groupByNonHashExpressions;
    unordered_set<string> HashPrimaryKeysNodeId;
    for (auto& expressionToGroupBy : groupByExpressions) {
        if (expressionToGroupBy->expressionType == PROPERTY) {
            auto& propertyExpression = (const PropertyExpression&)(*expressionToGroupBy);
            if (propertyExpression.isInternalID()) {
                groupByHashExpressions.push_back(expressionToGroupBy);
                HashPrimaryKeysNodeId.insert(propertyExpression.getChild(0)->getUniqueName());
            } else if (HashPrimaryKeysNodeId.contains(
                           propertyExpression.getChild(0)->getUniqueName())) {
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
    auto aggregate = make_unique<HashAggregate>(make_unique<ResultSetDescriptor>(inSchema),
        sharedState, inputGroupByHashKeyVectorsPos, inputGroupByNonHashKeyVectorsPos,
        isInputGroupByHashKeyVectorFlat, std::move(inputAggVectorsPos),
        std::move(aggregateFunctions), std::move(prevOperator), getOperatorID(), paramsString);
    auto aggregateScan = make_unique<HashAggregateScan>(sharedState, outputGroupByKeyVectorsPos,
        outputGroupByKeyVectorsDataTypeId, std::move(outputAggVectorsPos),
        std::move(outputAggVectorsDataType), std::move(aggregate), getOperatorID(), paramsString);
    return aggregateScan;
}

void PlanMapper::appendGroupByExpressions(const expression_vector& groupByExpressions,
    vector<DataPos>& inputGroupByHashKeyVectorsPos, vector<DataPos>& outputGroupByKeyVectorsPos,
    vector<DataType>& outputGroupByKeyVectorsDataTypes, const Schema& inSchema,
    const Schema& outSchema, vector<bool>& isInputGroupByHashKeyVectorFlat) {
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

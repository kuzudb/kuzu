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
    LogicalOperator* logicalOperator, MapperContext& mapperContext) {
    auto& logicalAggregate = (const LogicalAggregate&)*logicalOperator;
    auto mapperContextBeforeAggregate = MapperContext(
        make_unique<ResultSetDescriptor>(*logicalAggregate.getSchemaBeforeAggregate()));
    auto prevOperator =
        mapLogicalOperatorToPhysical(logicalOperator->getChild(0), mapperContextBeforeAggregate);
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
            inputAggVectorsPos.push_back(
                mapperContextBeforeAggregate.getDataPos(child->getUniqueName()));
        }

        aggregateFunctions.push_back(
            ((AggregateFunctionExpression&)*expression).aggregateFunction->clone());
        outputAggVectorsPos.push_back(mapperContext.getDataPos(expression->getUniqueName()));
        outputAggVectorsDataTypes.push_back(expression->dataType);
        mapperContext.addComputedExpressions(expression->getUniqueName());
    }
    if (logicalAggregate.hasExpressionsToGroupBy()) {
        return createHashAggregate(move(aggregateFunctions), inputAggVectorsPos,
            outputAggVectorsPos, outputAggVectorsDataTypes,
            logicalAggregate.getExpressionsToGroupBy(), logicalAggregate.getSchemaBeforeAggregate(),
            move(prevOperator), mapperContextBeforeAggregate, mapperContext, paramsString);
    } else {
        auto sharedState = make_shared<SimpleAggregateSharedState>(aggregateFunctions);
        auto aggregate = make_unique<SimpleAggregate>(sharedState, inputAggVectorsPos,
            move(aggregateFunctions), move(prevOperator), getOperatorID(), paramsString);
        auto aggregateScan = make_unique<SimpleAggregateScan>(sharedState,
            mapperContext.getResultSetDescriptor()->copy(), outputAggVectorsPos,
            outputAggVectorsDataTypes, move(aggregate), getOperatorID(), paramsString);
        return aggregateScan;
    }
}

unique_ptr<PhysicalOperator> PlanMapper::createHashAggregate(
    vector<unique_ptr<AggregateFunction>> aggregateFunctions, vector<DataPos> inputAggVectorsPos,
    vector<DataPos> outputAggVectorsPos, vector<DataType> outputAggVectorsDataType,
    const expression_vector& groupByExpressions, Schema* schema,
    unique_ptr<PhysicalOperator> prevOperator, MapperContext& mapperContextBeforeAggregate,
    MapperContext& mapperContext, const string& paramsString) {
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
        outputGroupByKeyVectorsPos, outputGroupByKeyVectorsDataTypeId, mapperContextBeforeAggregate,
        mapperContext, schema, isInputGroupByHashKeyVectorFlat);
    appendGroupByExpressions(groupByNonHashExpressions, inputGroupByNonHashKeyVectorsPos,
        outputGroupByKeyVectorsPos, outputGroupByKeyVectorsDataTypeId, mapperContextBeforeAggregate,
        mapperContext, schema, isInputGroupByHashKeyVectorFlat);
    auto sharedState = make_shared<HashAggregateSharedState>(aggregateFunctions);
    auto aggregate = make_unique<HashAggregate>(sharedState, inputGroupByHashKeyVectorsPos,
        inputGroupByNonHashKeyVectorsPos, isInputGroupByHashKeyVectorFlat, move(inputAggVectorsPos),
        move(aggregateFunctions), move(prevOperator), getOperatorID(), paramsString);
    auto aggregateScan = make_unique<HashAggregateScan>(sharedState,
        mapperContext.getResultSetDescriptor()->copy(), outputGroupByKeyVectorsPos,
        outputGroupByKeyVectorsDataTypeId, move(outputAggVectorsPos),
        move(outputAggVectorsDataType), move(aggregate), getOperatorID(), paramsString);
    return aggregateScan;
}

void PlanMapper::appendGroupByExpressions(const expression_vector& groupByExpressions,
    vector<DataPos>& inputGroupByHashKeyVectorsPos, vector<DataPos>& outputGroupByKeyVectorsPos,
    vector<DataType>& outputGroupByKeyVectorsDataTypes, MapperContext& mapperContextBeforeAggregate,
    MapperContext& mapperContext, Schema* schema, vector<bool>& isInputGroupByHashKeyVectorFlat) {
    for (auto& expression : groupByExpressions) {
        if (schema->getGroup(expression->getUniqueName())->getIsFlat()) {
            inputGroupByHashKeyVectorsPos.push_back(
                mapperContextBeforeAggregate.getDataPos(expression->getUniqueName()));
            outputGroupByKeyVectorsPos.push_back(
                mapperContext.getDataPos(expression->getUniqueName()));
            outputGroupByKeyVectorsDataTypes.push_back(expression->dataType);
            mapperContext.addComputedExpressions(expression->getUniqueName());
            isInputGroupByHashKeyVectorFlat.push_back(true);
        }
    }

    for (auto& expression : groupByExpressions) {
        if (!schema->getGroup(expression->getUniqueName())->getIsFlat()) {
            inputGroupByHashKeyVectorsPos.push_back(
                mapperContextBeforeAggregate.getDataPos(expression->getUniqueName()));
            outputGroupByKeyVectorsPos.push_back(
                mapperContext.getDataPos(expression->getUniqueName()));
            outputGroupByKeyVectorsDataTypes.push_back(expression->dataType);
            mapperContext.addComputedExpressions(expression->getUniqueName());
            isInputGroupByHashKeyVectorFlat.push_back(false);
        }
    }
}

} // namespace processor
} // namespace kuzu

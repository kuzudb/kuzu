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

static std::vector<std::unique_ptr<AggregateInputInfo>> getAggregateInputInfos(
    const expression_vector& groupByExpressions, const expression_vector& aggregateExpressions,
    const Schema& schema) {
    // Collect unFlat groups from
    std::unordered_set<f_group_pos> groupByGroupPosSet;
    for (auto& expression : groupByExpressions) {
        groupByGroupPosSet.insert(schema.getGroupPos(*expression));
    }
    std::unordered_set<f_group_pos> unFlatAggregateGroupPosSet;
    for (auto groupPos : schema.getGroupsPosInScope()) {
        if (groupByGroupPosSet.contains(groupPos)) {
            continue;
        }
        if (schema.getGroup(groupPos)->isFlat()) {
            continue;
        }
        unFlatAggregateGroupPosSet.insert(groupPos);
    }
    std::vector<std::unique_ptr<AggregateInputInfo>> result;
    for (auto& expression : aggregateExpressions) {
        DataPos aggregateVectorPos{};
        if (expression->getNumChildren() != 0) { // COUNT(*) has no children
            auto child = expression->getChild(0);
            aggregateVectorPos = DataPos{schema.getExpressionPos(*child)};
        }
        std::vector<data_chunk_pos_t> multiplicityChunksPos;
        for (auto& groupPos : unFlatAggregateGroupPosSet) {
            if (groupPos != aggregateVectorPos.dataChunkPos) {
                multiplicityChunksPos.push_back(groupPos);
            }
        }
        result.emplace_back(std::make_unique<AggregateInputInfo>(
            aggregateVectorPos, std::move(multiplicityChunksPos)));
    }
    return result;
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapLogicalAggregateToPhysical(
    LogicalOperator* logicalOperator) {
    auto& logicalAggregate = (const LogicalAggregate&)*logicalOperator;
    auto outSchema = logicalAggregate.getSchema();
    auto inSchema = logicalAggregate.getChild(0)->getSchema();
    auto prevOperator = mapLogicalOperatorToPhysical(logicalOperator->getChild(0));
    auto paramsString = logicalAggregate.getExpressionsForPrinting();
    std::vector<DataPos> outputAggVectorsPos{logicalAggregate.getNumAggregateExpressions()};
    std::vector<std::unique_ptr<AggregateFunction>> aggregateFunctions{
        logicalAggregate.getNumAggregateExpressions()};
    for (auto i = 0u; i < logicalAggregate.getNumAggregateExpressions(); ++i) {
        auto expression = logicalAggregate.getAggregateExpression(i);
        aggregateFunctions[i] =
            ((AggregateFunctionExpression&)*expression).aggregateFunction->clone();
        outputAggVectorsPos[i] = DataPos(outSchema->getExpressionPos(*expression));
    }
    auto aggregateInputInfos = getAggregateInputInfos(logicalAggregate.getExpressionsToGroupBy(),
        logicalAggregate.getExpressionsToAggregate(), *inSchema);
    if (logicalAggregate.hasExpressionsToGroupBy()) {
        return createHashAggregate(std::move(aggregateFunctions), std::move(aggregateInputInfos),
            outputAggVectorsPos, logicalAggregate.getExpressionsToGroupBy(),
            std::move(prevOperator), *inSchema, *outSchema, paramsString);
    } else {
        auto sharedState = make_shared<SimpleAggregateSharedState>(aggregateFunctions);
        auto aggregate =
            make_unique<SimpleAggregate>(std::make_unique<ResultSetDescriptor>(*inSchema),
                sharedState, std::move(aggregateFunctions), std::move(aggregateInputInfos),
                std::move(prevOperator), getOperatorID(), paramsString);
        return make_unique<SimpleAggregateScan>(
            sharedState, outputAggVectorsPos, std::move(aggregate), getOperatorID(), paramsString);
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::createHashAggregate(
    std::vector<std::unique_ptr<AggregateFunction>> aggregateFunctions,
    std::vector<std::unique_ptr<AggregateInputInfo>> inputAggregateInfo,
    std::vector<DataPos> outputAggVectorsPos, const expression_vector& groupByExpressions,
    std::unique_ptr<PhysicalOperator> prevOperator, const Schema& inSchema, const Schema& outSchema,
    const std::string& paramsString) {
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
    std::vector<DataPos> inputGroupByHashKeyVectorsPos;
    std::vector<DataPos> inputGroupByNonHashKeyVectorsPos;
    std::vector<bool> isInputGroupByHashKeyVectorFlat;
    std::vector<DataPos> outputGroupByKeyVectorsPos;
    appendGroupByExpressions(groupByHashExpressions, inputGroupByHashKeyVectorsPos,
        outputGroupByKeyVectorsPos, inSchema, outSchema, isInputGroupByHashKeyVectorFlat);
    appendGroupByExpressions(groupByNonHashExpressions, inputGroupByNonHashKeyVectorsPos,
        outputGroupByKeyVectorsPos, inSchema, outSchema, isInputGroupByHashKeyVectorFlat);
    auto sharedState = make_shared<HashAggregateSharedState>(aggregateFunctions);
    auto aggregate = make_unique<HashAggregate>(std::make_unique<ResultSetDescriptor>(inSchema),
        sharedState, inputGroupByHashKeyVectorsPos, inputGroupByNonHashKeyVectorsPos,
        isInputGroupByHashKeyVectorFlat, std::move(aggregateFunctions),
        std::move(inputAggregateInfo), std::move(prevOperator), getOperatorID(), paramsString);
    auto aggregateScan =
        std::make_unique<HashAggregateScan>(sharedState, outputGroupByKeyVectorsPos,
            std::move(outputAggVectorsPos), std::move(aggregate), getOperatorID(), paramsString);
    return aggregateScan;
}

void PlanMapper::appendGroupByExpressions(const expression_vector& groupByExpressions,
    std::vector<DataPos>& inputGroupByHashKeyVectorsPos,
    std::vector<DataPos>& outputGroupByKeyVectorsPos, const Schema& inSchema,
    const Schema& outSchema, std::vector<bool>& isInputGroupByHashKeyVectorFlat) {
    for (auto& expression : groupByExpressions) {
        if (inSchema.getGroup(expression->getUniqueName())->isFlat()) {
            inputGroupByHashKeyVectorsPos.emplace_back(inSchema.getExpressionPos(*expression));
            outputGroupByKeyVectorsPos.emplace_back(outSchema.getExpressionPos(*expression));
            isInputGroupByHashKeyVectorFlat.push_back(true);
        }
    }

    for (auto& expression : groupByExpressions) {
        if (!inSchema.getGroup(expression->getUniqueName())->isFlat()) {
            inputGroupByHashKeyVectorsPos.emplace_back(inSchema.getExpressionPos(*expression));
            outputGroupByKeyVectorsPos.emplace_back(outSchema.getExpressionPos(*expression));
            isInputGroupByHashKeyVectorFlat.push_back(false);
        }
    }
}

} // namespace processor
} // namespace kuzu

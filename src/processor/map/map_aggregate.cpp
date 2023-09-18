#include "binder/expression/function_expression.h"
#include "planner/operator/logical_aggregate.h"
#include "processor/operator/aggregate/hash_aggregate.h"
#include "processor/operator/aggregate/hash_aggregate_scan.h"
#include "processor/operator/aggregate/simple_aggregate.h"
#include "processor/operator/aggregate/simple_aggregate_scan.h"
#include "processor/plan_mapper.h"

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

static binder::expression_vector getKeyExpressions(
    const binder::expression_vector& expressions, const Schema& schema, bool isFlat) {
    binder::expression_vector result;
    for (auto& expression : expressions) {
        if (schema.getGroup(schema.getGroupPos(*expression))->isFlat() == isFlat) {
            result.emplace_back(expression);
        }
    }
    return result;
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapAggregate(LogicalOperator* logicalOperator) {
    auto& logicalAggregate = (const LogicalAggregate&)*logicalOperator;
    auto outSchema = logicalAggregate.getSchema();
    auto inSchema = logicalAggregate.getChild(0)->getSchema();
    auto prevOperator = mapOperator(logicalOperator->getChild(0).get());
    auto paramsString = logicalAggregate.getExpressionsForPrinting();
    std::vector<std::unique_ptr<AggregateFunction>> aggregateFunctions;
    for (auto& expression : logicalAggregate.getAggregateExpressions()) {
        aggregateFunctions.push_back(
            ((AggregateFunctionExpression&)*expression).aggregateFunction->clone());
    }
    auto aggregatesOutputPos =
        getExpressionsDataPos(logicalAggregate.getAggregateExpressions(), *outSchema);
    auto aggregateInputInfos = getAggregateInputInfos(logicalAggregate.getAllKeyExpressions(),
        logicalAggregate.getAggregateExpressions(), *inSchema);
    if (logicalAggregate.hasKeyExpressions()) {
        return createHashAggregate(logicalAggregate.getKeyExpressions(),
            logicalAggregate.getDependentKeyExpressions(), std::move(aggregateFunctions),
            std::move(aggregateInputInfos), std::move(aggregatesOutputPos), inSchema, outSchema,
            std::move(prevOperator), paramsString);
    } else {
        auto sharedState = make_shared<SimpleAggregateSharedState>(aggregateFunctions);
        auto aggregate =
            make_unique<SimpleAggregate>(std::make_unique<ResultSetDescriptor>(inSchema),
                sharedState, std::move(aggregateFunctions), std::move(aggregateInputInfos),
                std::move(prevOperator), getOperatorID(), paramsString);
        return make_unique<SimpleAggregateScan>(
            sharedState, aggregatesOutputPos, std::move(aggregate), getOperatorID(), paramsString);
    }
}

std::unique_ptr<PhysicalOperator> PlanMapper::createHashAggregate(
    const binder::expression_vector& keyExpressions,
    const binder::expression_vector& dependentKeyExpressions,
    std::vector<std::unique_ptr<function::AggregateFunction>> aggregateFunctions,
    std::vector<std::unique_ptr<AggregateInputInfo>> aggregateInputInfos,
    std::vector<DataPos> aggregatesOutputPos, planner::Schema* inSchema, planner::Schema* outSchema,
    std::unique_ptr<PhysicalOperator> prevOperator, const std::string& paramsString) {
    auto sharedState = make_shared<HashAggregateSharedState>(aggregateFunctions);
    auto flatKeyExpressions = getKeyExpressions(keyExpressions, *inSchema, true /* isFlat */);
    auto unFlatKeyExpressions = getKeyExpressions(keyExpressions, *inSchema, false /* isFlat */);
    auto aggregate = make_unique<HashAggregate>(std::make_unique<ResultSetDescriptor>(inSchema),
        sharedState, getExpressionsDataPos(flatKeyExpressions, *inSchema),
        getExpressionsDataPos(unFlatKeyExpressions, *inSchema),
        getExpressionsDataPos(dependentKeyExpressions, *inSchema), std::move(aggregateFunctions),
        std::move(aggregateInputInfos), std::move(prevOperator), getOperatorID(), paramsString);
    binder::expression_vector outputExpressions;
    outputExpressions.insert(
        outputExpressions.end(), flatKeyExpressions.begin(), flatKeyExpressions.end());
    outputExpressions.insert(
        outputExpressions.end(), unFlatKeyExpressions.begin(), unFlatKeyExpressions.end());
    outputExpressions.insert(
        outputExpressions.end(), dependentKeyExpressions.begin(), dependentKeyExpressions.end());
    return std::make_unique<HashAggregateScan>(sharedState,
        getExpressionsDataPos(outputExpressions, *outSchema), std::move(aggregatesOutputPos),
        std::move(aggregate), getOperatorID(), paramsString);
}

} // namespace processor
} // namespace kuzu

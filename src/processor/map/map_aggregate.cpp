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

static std::vector<AggregateInfo> getAggregateInputInfos(const expression_vector& keys,
    const expression_vector& aggregates, const Schema& schema) {
    // Collect unFlat groups from
    std::unordered_set<f_group_pos> groupByGroupPosSet;
    for (auto& expression : keys) {
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
    std::vector<AggregateInfo> result;
    for (auto& expression : aggregates) {
        auto aggregateVectorPos = DataPos::getInvalidPos();
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
        auto aggExpr = expression->constPtrCast<AggregateFunctionExpression>();
        auto distinctAggKeyType = aggExpr->isDistinct() ?
                                      expression->getChild(0)->getDataType().copy() :
                                      LogicalType::ANY();
        result.emplace_back(aggregateVectorPos, std::move(multiplicityChunksPos),
            std::move(distinctAggKeyType));
    }
    return result;
}

static expression_vector getKeyExpressions(const expression_vector& expressions,
    const Schema& schema, bool isFlat) {
    expression_vector result;
    for (auto& expression : expressions) {
        if (schema.getGroup(schema.getGroupPos(*expression))->isFlat() == isFlat) {
            result.emplace_back(expression);
        }
    }
    return result;
}

static std::vector<std::unique_ptr<AggregateFunction>> getAggFunctions(
    const expression_vector& aggregates) {
    std::vector<std::unique_ptr<AggregateFunction>> aggregateFunctions;
    for (auto& expression : aggregates) {
        auto aggExpr = expression->constPtrCast<AggregateFunctionExpression>();
        aggregateFunctions.push_back(aggExpr->aggregateFunction->clone());
    }
    return aggregateFunctions;
}

std::unique_ptr<PhysicalOperator> PlanMapper::mapAggregate(LogicalOperator* logicalOperator) {
    auto& agg = logicalOperator->cast<LogicalAggregate>();
    auto aggregates = agg.getAggregates();
    auto outSchema = agg.getSchema();
    auto child = agg.getChild(0).get();
    auto inSchema = child->getSchema();
    auto prevOperator = mapOperator(child);
    if (agg.hasKeys()) {
        return createHashAggregate(agg.getKeys(), agg.getDependentKeys(), aggregates,
            nullptr /* mark */, inSchema, outSchema, std::move(prevOperator));
    }
    auto aggFunctions = getAggFunctions(aggregates);
    auto aggOutputPos = getDataPos(aggregates, *outSchema);
    auto aggregateInputInfos = getAggregateInputInfos(agg.getAllKeys(), aggregates, *inSchema);
    auto sharedState = make_shared<SimpleAggregateSharedState>(aggFunctions);
    auto printInfo = std::make_unique<SimpleAggregatePrintInfo>(aggregates);
    auto aggregate = make_unique<SimpleAggregate>(std::make_unique<ResultSetDescriptor>(inSchema),
        sharedState, std::move(aggFunctions), std::move(aggregateInputInfos),
        std::move(prevOperator), getOperatorID(), printInfo->copy());
    return make_unique<SimpleAggregateScan>(sharedState, aggOutputPos, std::move(aggregate),
        getOperatorID(), printInfo->copy());
}

static FactorizedTableSchema getFactorizedTableSchema(const expression_vector& flatKeys,
    const expression_vector& unFlatKeys, const expression_vector& payloads,
    std::vector<std::unique_ptr<function::AggregateFunction>>& aggregateFunctions,
    std::shared_ptr<Expression> markExpression) {
    auto isUnFlat = false;
    auto groupID = 0u;
    auto tableSchema = FactorizedTableSchema();
    for (auto& flatKey : flatKeys) {
        auto size = LogicalTypeUtils::getRowLayoutSize(flatKey->dataType);
        tableSchema.appendColumn(ColumnSchema(isUnFlat, groupID, size));
    }
    for (auto& unFlatKey : unFlatKeys) {
        auto size = LogicalTypeUtils::getRowLayoutSize(unFlatKey->dataType);
        tableSchema.appendColumn(ColumnSchema(isUnFlat, groupID, size));
    }
    for (auto& payload : payloads) {
        auto size = LogicalTypeUtils::getRowLayoutSize(payload->dataType);
        tableSchema.appendColumn(ColumnSchema(isUnFlat, groupID, size));
    }
    for (auto& aggregateFunc : aggregateFunctions) {
        tableSchema.appendColumn(
            ColumnSchema(isUnFlat, groupID, aggregateFunc->getAggregateStateSize()));
    }
    if (markExpression != nullptr) {
        tableSchema.appendColumn(ColumnSchema(isUnFlat, groupID,
            LogicalTypeUtils::getRowLayoutSize(markExpression->dataType)));
    }
    tableSchema.appendColumn(ColumnSchema(isUnFlat, groupID, sizeof(hash_t)));
    return tableSchema;
}

std::unique_ptr<PhysicalOperator> PlanMapper::createDistinctHashAggregate(
    const expression_vector& keys, const expression_vector& payloads, Schema* inSchema,
    Schema* outSchema, std::unique_ptr<PhysicalOperator> prevOperator) {
    return createHashAggregate(keys, payloads, expression_vector{} /* aggregates */,
        nullptr /* mark */, inSchema, outSchema, std::move(prevOperator));
}

std::unique_ptr<PhysicalOperator> PlanMapper::createMarkDistinctHashAggregate(
    const expression_vector& keys, const expression_vector& payloads,
    std::shared_ptr<binder::Expression> mark, Schema* inSchema, Schema* outSchema,
    std::unique_ptr<PhysicalOperator> prevOperator) {
    return createHashAggregate(keys, payloads, expression_vector{} /* aggregates */,
        std::move(mark), inSchema, outSchema, std::move(prevOperator));
}

// Payloads are also group by keys except that they are functional dependent on keys so we don't
// need to hash or compare payloads.
std::unique_ptr<PhysicalOperator> PlanMapper::createHashAggregate(const expression_vector& keys,
    const expression_vector& payloads, const expression_vector& aggregates,
    std::shared_ptr<binder::Expression> mark, Schema* inSchema, Schema* outSchema,
    std::unique_ptr<PhysicalOperator> prevOperator) {
    // Create hash aggregate
    auto aggFunctions = getAggFunctions(aggregates);
    expression_vector allKeys;
    allKeys.insert(allKeys.end(), keys.begin(), keys.end());
    allKeys.insert(allKeys.end(), payloads.begin(), payloads.end());
    auto aggregateInputInfos = getAggregateInputInfos(allKeys, aggregates, *inSchema);
    auto sharedState = std::make_shared<HashAggregateSharedState>(aggFunctions);
    auto flatKeys = getKeyExpressions(keys, *inSchema, true /* isFlat */);
    auto unFlatKeys = getKeyExpressions(keys, *inSchema, false /* isFlat */);
    auto tableSchema = getFactorizedTableSchema(flatKeys, unFlatKeys, payloads, aggFunctions, mark);
    auto hashTableType =
        mark == nullptr ? HashTableType::AGGREGATE_HASH_TABLE : HashTableType::MARK_HASH_TABLE;
    HashAggregateInfo aggregateInfo{getDataPos(flatKeys, *inSchema),
        getDataPos(unFlatKeys, *inSchema), getDataPos(payloads, *inSchema), std::move(tableSchema),
        hashTableType};
    auto printInfo = std::make_unique<HashAggregatePrintInfo>(allKeys, aggregates);
    auto aggregate =
        make_unique<HashAggregate>(std::make_unique<ResultSetDescriptor>(inSchema), sharedState,
            std::move(aggregateInfo), std::move(aggFunctions), std::move(aggregateInputInfos),
            std::move(prevOperator), getOperatorID(), printInfo->copy());
    // Create AggScan.
    expression_vector outputExpressions;
    outputExpressions.insert(outputExpressions.end(), flatKeys.begin(), flatKeys.end());
    outputExpressions.insert(outputExpressions.end(), unFlatKeys.begin(), unFlatKeys.end());
    outputExpressions.insert(outputExpressions.end(), payloads.begin(), payloads.end());
    if (mark != nullptr) {
        outputExpressions.emplace_back(mark);
    }
    auto aggOutputPos = getDataPos(aggregates, *outSchema);
    return std::make_unique<HashAggregateScan>(sharedState,
        getDataPos(outputExpressions, *outSchema), std::move(aggOutputPos), std::move(aggregate),
        getOperatorID(), printInfo->copy());
}

} // namespace processor
} // namespace kuzu

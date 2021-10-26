#include "gtest/gtest.h"

#include "src/function/include/aggregation/count.h"
#include "src/processor/include/physical_plan/operator/aggregate/aggregate_hash_table.h"

using ::testing::Test;

class AggregateHashTableTest : public Test {

public:
    void SetUp() override {
        memoryManager = make_unique<MemoryManager>();
        group1Vector = make_shared<ValueVector>(memoryManager.get(), INT64);
        group2Vector = make_shared<ValueVector>(memoryManager.get(), INT64);
        aggr1Vector = make_shared<ValueVector>(memoryManager.get(), INT64);
        aggr2Vector = make_shared<ValueVector>(memoryManager.get(), INT64);
        auto group1Values = (int64_t*)group1Vector->values;
        auto group2Values = (int64_t*)group2Vector->values;
        auto aggr1Values = (int64_t*)aggr1Vector->values;
        auto aggr2Values = (int64_t*)aggr2Vector->values;
        for (auto i = 0u; i < 100; i++) {
            group1Values[i] = i % 4;
            group2Values[i] = i;
            aggr1Values[i] = i * 2;
            aggr2Values[i] = i * 2;
        }
        dataChunk = make_shared<DataChunk>(4);
        dataChunk->state->selectedSize = 100;
        dataChunk->state->currIdx = 0;
        dataChunk->insert(0, group1Vector);
        dataChunk->insert(1, group2Vector);
        dataChunk->insert(2, aggr1Vector);
        dataChunk->insert(3, aggr2Vector);
        resultSet = make_shared<ResultSet>(1);
        resultSet->insert(0, dataChunk);
    }

public:
    unique_ptr<MemoryManager> memoryManager;
    unique_ptr<AggregateHashTable> ht;
    shared_ptr<ValueVector> group1Vector;
    shared_ptr<ValueVector> group2Vector;
    shared_ptr<ValueVector> aggr1Vector;
    shared_ptr<ValueVector> aggr2Vector;
    shared_ptr<DataChunk> dataChunk;
    shared_ptr<ResultSet> resultSet;
};

TEST_F(AggregateHashTableTest, SingleGroupTest) {
    vector<unique_ptr<AggregateExpressionEvaluator>> aggregates;
    auto count1Aggregate = make_unique<AggregateExpressionEvaluator>(COUNT_STAR_FUNC, INT64,
        AggregateExpressionEvaluator::getAggregationFunction(COUNT_STAR_FUNC, INT64));
    auto count2Aggregate = make_unique<AggregateExpressionEvaluator>(COUNT_STAR_FUNC, INT64,
        AggregateExpressionEvaluator::getAggregationFunction(COUNT_STAR_FUNC, INT64));
    auto count1State = count1Aggregate->getFunction()->initialize();
    auto count2State = count2Aggregate->getFunction()->initialize();
    auto groupsSize = sizeof(uint64_t);
    auto entrySize =
        sizeof(hash_t) + groupsSize + count1State->getValSize() + count2State->getValSize();
    aggregates.push_back(move(count1Aggregate));
    aggregates.push_back(move(count2Aggregate));
    auto ht =
        make_unique<AggregateHashTable>(*memoryManager, move(aggregates), groupsSize, entrySize);
    vector<shared_ptr<ValueVector>> groupVectors, aggregateVectors;
    groupVectors.push_back(group1Vector);
    aggregateVectors.push_back(aggr1Vector);
    aggregateVectors.push_back(aggr2Vector);
    while (dataChunk->state->currIdx < dataChunk->state->selectedSize) {
        ht->append(groupVectors, aggregateVectors);
        dataChunk->state->currIdx++;
    }
    dataChunk->state->currIdx = 0;
    while (dataChunk->state->currIdx < 4) {
        auto entry = ht->lookup(groupVectors);
        ASSERT_TRUE(entry != nullptr);
        auto count1AggrState =
            (CountFunction<true>::CountState*)(entry + sizeof(hash_t) + groupsSize);
        ASSERT_EQ(count1AggrState->val, 25);
        auto count2AggrState =
            (CountFunction<true>::CountState*)(entry + sizeof(hash_t) + groupsSize +
                                               count1AggrState->getValSize());
        ASSERT_EQ(count2AggrState->val, 25);
        dataChunk->state->currIdx++;
    }
}

TEST_F(AggregateHashTableTest, TwoGroupsTest) {
    vector<unique_ptr<AggregateExpressionEvaluator>> aggregates;
    auto count1Aggregate = make_unique<AggregateExpressionEvaluator>(COUNT_STAR_FUNC, INT64,
        AggregateExpressionEvaluator::getAggregationFunction(COUNT_STAR_FUNC, INT64));
    auto count2Aggregate = make_unique<AggregateExpressionEvaluator>(COUNT_STAR_FUNC, INT64,
        AggregateExpressionEvaluator::getAggregationFunction(COUNT_STAR_FUNC, INT64));
    auto count1State = count1Aggregate->getFunction()->initialize();
    auto count2State = count2Aggregate->getFunction()->initialize();
    auto groupsSize = sizeof(uint64_t) + sizeof(uint64_t);
    auto entrySize =
        sizeof(hash_t) + groupsSize + count1State->getValSize() + count2State->getValSize();
    aggregates.push_back(move(count1Aggregate));
    aggregates.push_back(move(count2Aggregate));
    auto ht =
        make_unique<AggregateHashTable>(*memoryManager, move(aggregates), groupsSize, entrySize);
    vector<shared_ptr<ValueVector>> groupVectors, aggregateVectors;
    groupVectors.push_back(group1Vector);
    groupVectors.push_back(group2Vector);
    aggregateVectors.push_back(aggr1Vector);
    aggregateVectors.push_back(aggr2Vector);
    while (dataChunk->state->currIdx < dataChunk->state->selectedSize) {
        ht->append(groupVectors, aggregateVectors);
        dataChunk->state->currIdx++;
    }
    dataChunk->state->currIdx = 0;
    while (dataChunk->state->currIdx < dataChunk->state->selectedSize) {
        auto entry = ht->lookup(groupVectors);
        ASSERT_TRUE(entry != nullptr);
        auto count1AggrState =
            (CountFunction<true>::CountState*)(entry + sizeof(hash_t) + groupsSize);
        ASSERT_EQ(count1AggrState->val, 1);
        auto count2AggrState =
            (CountFunction<true>::CountState*)(entry + sizeof(hash_t) + groupsSize +
                                               count1AggrState->getValSize());
        ASSERT_EQ(count2AggrState->val, 1);
        dataChunk->state->currIdx++;
    }
}

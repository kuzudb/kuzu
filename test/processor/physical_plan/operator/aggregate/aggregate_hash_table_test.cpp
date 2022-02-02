#include "gtest/gtest.h"

#include "src/function/include/aggregate/avg.h"
#include "src/function/include/aggregate/count.h"
#include "src/function/include/aggregate/sum.h"
#include "src/processor/include/physical_plan/operator/aggregate/base_aggregate_hash_table.h"
#include "src/processor/include/physical_plan/result/result_set.h"

using ::testing::Test;
using namespace graphflow::function;
using namespace graphflow::processor;

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
    vector<unique_ptr<AggregateFunction>> aggregates;
    auto countAggregate = AggregateFunctionUtil::getCountStarFunction();
    auto sumAggregate = AggregateFunctionUtil::getAggregateFunction(SUM_FUNC, INT64);
    auto countState = countAggregate->createInitialNullAggregateState();
    auto sumState = sumAggregate->createInitialNullAggregateState();
    aggregates.push_back(move(countAggregate));
    aggregates.push_back(move(sumAggregate));
    auto groupByVectorsDataType = vector<DataType>{INT64};
    auto ht = make_unique<AggregateHashTable>(
        *memoryManager, move(groupByVectorsDataType), aggregates, 0);
    vector<ValueVector*> groupVectors, aggregateVectors;
    groupVectors.push_back(group1Vector.get());
    aggregateVectors.push_back(nullptr);
    aggregateVectors.push_back(aggr2Vector.get());
    while (dataChunk->state->currIdx < dataChunk->state->selectedSize) {
        ht->append(groupVectors, aggregateVectors, 1 /* multiplicity */);
        dataChunk->state->currIdx++;
    }
    ht->finalizeAggregateStates();
    auto groupsSize = TypeUtils::getDataTypeSize(INT64);
    auto numGroupScanned = 0u;
    while (numGroupScanned < ht->getNumEntries()) {
        auto entry = ht->getEntry(numGroupScanned);
        assert(entry != nullptr);
        auto countAggrState = (BaseCountFunction::CountState*)(entry + sizeof(hash_t) + groupsSize);
        ASSERT_EQ(*(uint64_t*)countAggrState->getResult(), 25);
        auto sumAggrState = (SumFunction<int64_t>::SumState*)(entry + sizeof(hash_t) + groupsSize +
                                                              countAggrState->getStateSize());
        ASSERT_EQ(*(int64_t*)sumAggrState->getResult(), 2400 + numGroupScanned * 50);
        ++numGroupScanned;
    }
}

TEST_F(AggregateHashTableTest, TwoGroupsTest) {
    vector<unique_ptr<AggregateFunction>> aggregates;
    auto countAggregate = AggregateFunctionUtil::getAggregateFunction(COUNT_FUNC, INT64);
    auto avgAggregate = AggregateFunctionUtil::getAggregateFunction(AVG_FUNC, INT64);
    auto count1State = countAggregate->createInitialNullAggregateState();
    auto count2State = avgAggregate->createInitialNullAggregateState();
    aggregates.push_back(move(countAggregate));
    aggregates.push_back(move(avgAggregate));

    auto groupByVectorsDataType = vector<DataType>{INT64, INT64};
    auto ht = make_unique<AggregateHashTable>(
        *memoryManager, move(groupByVectorsDataType), aggregates, 0);
    vector<ValueVector*> groupVectors, aggregateVectors;
    groupVectors.push_back(group1Vector.get());
    groupVectors.push_back(group2Vector.get());
    aggregateVectors.push_back(aggr1Vector.get());
    aggregateVectors.push_back(aggr2Vector.get());
    while (dataChunk->state->currIdx < dataChunk->state->selectedSize) {
        ht->append(groupVectors, aggregateVectors, 1 /* multiplicity */);
        dataChunk->state->currIdx++;
    }
    ht->finalizeAggregateStates();
    auto groupsSize = sizeof(uint64_t) + sizeof(uint64_t);
    auto numGroupScanned = 0u;
    while (numGroupScanned < ht->getNumEntries()) {
        auto entry = ht->getEntry(numGroupScanned);
        assert(entry != nullptr);
        auto countAggrState = (BaseCountFunction::CountState*)(entry + sizeof(hash_t) + groupsSize);
        ASSERT_EQ(*(uint64_t*)countAggrState->getResult(), 1);
        auto avgAggrState = (AvgFunction<int64_t>::AvgState*)(entry + sizeof(hash_t) + groupsSize +
                                                              countAggrState->getStateSize());
        ASSERT_EQ(avgAggrState->avg, numGroupScanned * 2);
        ++numGroupScanned;
    }
}

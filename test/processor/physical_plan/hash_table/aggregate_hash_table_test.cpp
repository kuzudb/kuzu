#include "include/gtest/gtest.h"

#include "src/function/aggregate/include/avg.h"
#include "src/function/aggregate/include/count.h"
#include "src/function/aggregate/include/sum.h"
#include "src/processor/include/physical_plan/hash_table/aggregate_hash_table.h"
#include "src/processor/include/physical_plan/result/result_set.h"

using ::testing::Test;
using namespace graphflow::function;
using namespace graphflow::processor;

class AggregateHashTableTest : public Test {

public:
    void SetUp() override {
        bufferManager = make_unique<BufferManager>();
        memoryManager = make_unique<MemoryManager>(bufferManager.get());
        group1Vector = make_shared<ValueVector>(memoryManager.get(), INT64);
        group2Vector = make_shared<ValueVector>(memoryManager.get(), INT64);
        group3Vector = make_shared<ValueVector>(memoryManager.get(), INT64);
        aggr1Vector = make_shared<ValueVector>(memoryManager.get(), INT64);
        aggr2Vector = make_shared<ValueVector>(memoryManager.get(), INT64);
        auto group1Values = (int64_t*)group1Vector->values;
        auto group2Values = (int64_t*)group2Vector->values;
        auto group3Values = (int64_t*)group3Vector->values;
        auto aggr1Values = (int64_t*)aggr1Vector->values;
        auto aggr2Values = (int64_t*)aggr2Vector->values;
        for (auto i = 0u; i < 100; i++) {
            group1Values[i] = i % 4;
            group2Values[i] = i;
            aggr1Values[i] = i * 2;
            aggr2Values[i] = i * 2;
        }
        group3Values[0] = 20;
        dataChunk = make_shared<DataChunk>(4);
        dataChunk->state->selVector->selectedSize = 100;
        dataChunk->state->currIdx = 0;
        dataChunk1 = make_shared<DataChunk>(1);
        dataChunk1->state->selVector->selectedSize = 1;
        dataChunk1->state->currIdx = 0;
        dataChunk->insert(0, group1Vector);
        dataChunk->insert(1, group2Vector);
        dataChunk->insert(2, aggr1Vector);
        dataChunk->insert(3, aggr2Vector);
        dataChunk1->insert(0, group3Vector);
        resultSet = make_shared<ResultSet>(1);
        resultSet->insert(0, dataChunk);
    }

    void singleGroupByKeyTest(bool isGroupByKeyFlat) {
        vector<unique_ptr<AggregateFunction>> aggregates;
        auto countAggregate = AggregateFunctionUtil::getCountStarFunction();
        auto sumAggregate = AggregateFunctionUtil::getSumFunction(DataType(INT64), false);
        auto countState = countAggregate->createInitialNullAggregateState();
        auto sumState = sumAggregate->createInitialNullAggregateState();
        aggregates.push_back(move(countAggregate));
        aggregates.push_back(move(sumAggregate));
        auto groupByVectorsDataType = vector<DataType>{DataType(INT64)};
        auto ht = make_unique<AggregateHashTable>(
            *memoryManager, move(groupByVectorsDataType), aggregates, 0 /* numEntriesToAllocate */);
        vector<ValueVector*> groupVectors, aggregateVectors;
        groupVectors.push_back(group1Vector.get());
        aggregateVectors.push_back(nullptr);
        aggregateVectors.push_back(aggr2Vector.get());
        if (isGroupByKeyFlat) {
            while (dataChunk->state->currIdx < dataChunk->state->selVector->selectedSize) {
                ht->append(
                    groupVectors, vector<ValueVector*>(), aggregateVectors, 1 /* multiplicity */);
                dataChunk->state->currIdx++;
            }
        } else {
            dataChunk->state->currIdx = -1;
            ht->append(
                vector<ValueVector*>(), groupVectors, aggregateVectors, 1 /* multiplicity */);
        }
        ht->finalizeAggregateStates();
        auto groupsSize = Types::getDataTypeSize(INT64);
        auto numGroupScanned = 0ul;
        assert(ht->getNumEntries() == 4);
        while (numGroupScanned < ht->getNumEntries()) {
            auto entry = ht->getEntry(numGroupScanned);
            assert(entry != nullptr);
            auto countAggrState = (BaseCountFunction::CountState*)(entry + groupsSize);
            ASSERT_EQ(*(uint64_t*)countAggrState->getResult(), 25);
            auto sumAggrState = (SumFunction<int64_t>::SumState*)(entry + groupsSize +
                                                                  countAggrState->getStateSize());
            ASSERT_EQ(*(int64_t*)sumAggrState->getResult(), 2400 + numGroupScanned * 50);
            ++numGroupScanned;
        }
    }

    void twoGroupByKeyTest(bool isFirstGroupByKeyFlat, bool isSecondGroupByKeyFlat) {
        vector<unique_ptr<AggregateFunction>> aggregates;
        auto countAggregate = AggregateFunctionUtil::getCountFunction(DataType(INT64), false);
        auto avgAggregate = AggregateFunctionUtil::getAvgFunction(DataType(INT64), false);
        auto count1State = countAggregate->createInitialNullAggregateState();
        auto count2State = avgAggregate->createInitialNullAggregateState();
        aggregates.push_back(move(countAggregate));
        aggregates.push_back(move(avgAggregate));

        auto groupByVectorsDataType = vector<DataType>{DataType(INT64), DataType(INT64)};
        auto ht = make_unique<AggregateHashTable>(
            *memoryManager, move(groupByVectorsDataType), aggregates, 0 /* numEntriesToAllocate */);
        vector<ValueVector*> flatGroupByVector, unflatGroupByVector, aggregateVectors;

        aggregateVectors.push_back(aggr1Vector.get());
        aggregateVectors.push_back(aggr2Vector.get());

        if (isFirstGroupByKeyFlat && isSecondGroupByKeyFlat) {
            flatGroupByVector.push_back(group1Vector.get());
            flatGroupByVector.push_back(group2Vector.get());
            while (dataChunk->state->currIdx < dataChunk->state->selVector->selectedSize) {
                ht->append(
                    flatGroupByVector, unflatGroupByVector, aggregateVectors, 1 /* multiplicity */);
                dataChunk->state->currIdx++;
            }
        } else if (isFirstGroupByKeyFlat != isSecondGroupByKeyFlat) {
            flatGroupByVector.push_back(group3Vector.get());
            unflatGroupByVector.push_back(group1Vector.get());
            dataChunk->state->currIdx = -1;
            ht->append(
                flatGroupByVector, unflatGroupByVector, aggregateVectors, 1 /* multiplicity */);
        } else {
            unflatGroupByVector.push_back(group1Vector.get());
            unflatGroupByVector.push_back(group2Vector.get());
            dataChunk->state->currIdx = -1;
            ht->append(
                flatGroupByVector, unflatGroupByVector, aggregateVectors, 1 /* multiplicity */);
        }

        ht->finalizeAggregateStates();
        auto groupsSize = sizeof(uint64_t) + sizeof(uint64_t);
        auto numGroupScanned = 0ul;
        if (isFirstGroupByKeyFlat == isSecondGroupByKeyFlat) {
            ASSERT_EQ(ht->getNumEntries(), 100);
        } else {
            ASSERT_EQ(ht->getNumEntries(), 4);
        }
        while (numGroupScanned < ht->getNumEntries()) {
            auto entry = ht->getEntry(numGroupScanned);
            assert(entry != nullptr);
            auto countAggrState = (BaseCountFunction::CountState*)(entry + groupsSize);
            if (isFirstGroupByKeyFlat == isSecondGroupByKeyFlat) {
                ASSERT_EQ(*(uint64_t*)countAggrState->getResult(), 1);
            } else {
                ASSERT_EQ(*(uint64_t*)countAggrState->getResult(), 25);
            }
            auto avgAggrState = (AvgFunction<int64_t>::AvgState*)(entry + groupsSize +
                                                                  countAggrState->getStateSize());
            if (isFirstGroupByKeyFlat == isSecondGroupByKeyFlat) {
                ASSERT_EQ(avgAggrState->avg, numGroupScanned * 2);
            } else {
                ASSERT_EQ(avgAggrState->avg, 96 + numGroupScanned * 2);
            }
            ++numGroupScanned;
        }
    }

public:
    unique_ptr<BufferManager> bufferManager;
    unique_ptr<MemoryManager> memoryManager;
    unique_ptr<AggregateHashTable> ht;
    shared_ptr<ValueVector> group1Vector;
    shared_ptr<ValueVector> group2Vector;
    shared_ptr<ValueVector> group3Vector;
    shared_ptr<ValueVector> aggr1Vector;
    shared_ptr<ValueVector> aggr2Vector;
    shared_ptr<ValueVector> aggr3Vector;
    shared_ptr<DataChunk> dataChunk;
    shared_ptr<DataChunk> dataChunk1;
    shared_ptr<ResultSet> resultSet;
};

TEST_F(AggregateHashTableTest, SingleFlatGroupByKeyTest) {
    singleGroupByKeyTest(true /* isGroupByKeyFlat */);
}

TEST_F(AggregateHashTableTest, SingleUnflatGroupByKeyTest) {
    singleGroupByKeyTest(false /* isGroupByKeyFlat */);
}

TEST_F(AggregateHashTableTest, TwoFlatGroupTest) {
    twoGroupByKeyTest(true /* isFirstGroupByKeyFlat */, true /* isSecondGroupByKeyFlat */);
}

TEST_F(AggregateHashTableTest, TwoFlatUnflatGroupTest) {
    twoGroupByKeyTest(true /* isFirstGroupByKeyFlat */, false /* isSecondGroupByKeyFlat */);
}

TEST_F(AggregateHashTableTest, TwoUnflatUnflatGroupTest) {
    twoGroupByKeyTest(false /* isFirstGroupByKeyFlat */, false /* isSecondGroupByKeyFlat */);
}

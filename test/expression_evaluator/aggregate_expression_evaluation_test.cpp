#include "common/utils.h"
#include "function/aggregate/aggregate_function.h"
#include "function/aggregate/avg.h"
#include "function/aggregate/count.h"
#include "function/aggregate/min_max.h"
#include "function/aggregate/sum.h"
#include "gtest/gtest.h"
#include "processor/result/result_set.h"

using ::testing::Test;

using namespace kuzu::processor;
using namespace kuzu::function;

class AggrExpressionEvaluatorTest : public Test {

public:
    void SetUp() override {
        bufferManager =
            make_unique<BufferManager>(StorageConfig::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING);
        memoryManager = make_unique<MemoryManager>(bufferManager.get());
        int64ValueVector = make_shared<ValueVector>(INT64, memoryManager.get());
        doubleValueVector = make_shared<ValueVector>(DOUBLE, memoryManager.get());
        stringValueVector = make_shared<ValueVector>(STRING, memoryManager.get());
        dataChunk = make_shared<DataChunk>(3);
        for (auto i = 0u; i < 100; i++) {
            int64ValueVector->setValue(i, (int64_t)i);
            doubleValueVector->setValue(i, (double_t)(i * 1.5));
            stringValueVector->setValue(i, to_string(i));
            if (i % 2 == 0) {
                int64ValueVector->setNull(i, true /* isNull */);
                doubleValueVector->setNull(i, true /* isNull */);
                stringValueVector->setNull(i, true /* isNull */);
            }
        }
        dataChunk->state->selVector->selectedSize = 100;
        dataChunk->insert(0, int64ValueVector);
        dataChunk->insert(1, doubleValueVector);
        dataChunk->insert(2, stringValueVector);
        resultSet = make_shared<ResultSet>(1);
        resultSet->insert(0, dataChunk);
    }

public:
    unique_ptr<BufferManager> bufferManager;
    unique_ptr<MemoryManager> memoryManager;
    shared_ptr<ValueVector> int64ValueVector;
    shared_ptr<ValueVector> doubleValueVector;
    shared_ptr<ValueVector> stringValueVector;
    shared_ptr<DataChunk> dataChunk;
    shared_ptr<ResultSet> resultSet;
};

TEST_F(AggrExpressionEvaluatorTest, CountStarTest) {
    auto countFunction = AggregateFunctionUtil::getCountStarFunction();
    auto countStarState =
        ku_static_unique_pointer_cast<AggregateState, BaseCountFunction::CountState>(
            countFunction->createInitialNullAggregateState());
    countFunction->updateAllState((uint8_t*)countStarState.get(), nullptr,
        resultSet->getNumTuples(unordered_set<uint32_t>{0}));
    auto otherCountStarState =
        ku_static_unique_pointer_cast<AggregateState, BaseCountFunction::CountState>(
            countFunction->createInitialNullAggregateState());
    otherCountStarState->count = 10;
    countFunction->combineState(
        (uint8_t*)countStarState.get(), (uint8_t*)otherCountStarState.get());
    countFunction->finalizeState((uint8_t*)countStarState.get());
    ASSERT_EQ(countStarState->count, 110);
}

TEST_F(AggrExpressionEvaluatorTest, CountTest) {
    auto countFunction = AggregateFunctionUtil::getCountFunction(DataType(INT64), false);

    auto countState = ku_static_unique_pointer_cast<AggregateState, BaseCountFunction::CountState>(
        countFunction->createInitialNullAggregateState());
    countFunction->updateAllState(
        (uint8_t*)countState.get(), int64ValueVector.get(), 1 /* multiplicity */);
    auto otherCountState =
        ku_static_unique_pointer_cast<AggregateState, BaseCountFunction::CountState>(
            countFunction->createInitialNullAggregateState());
    otherCountState->count = 10;
    countFunction->combineState((uint8_t*)countState.get(), (uint8_t*)otherCountState.get());
    countFunction->finalizeState((uint8_t*)countState.get());
    ASSERT_EQ(countState->count, 60);
}

TEST_F(AggrExpressionEvaluatorTest, INT64SumTest) {
    auto sumFunction = AggregateFunctionUtil::getSumFunction(DataType(INT64), false);
    auto sumState = ku_static_unique_pointer_cast<AggregateState, SumFunction<int64_t>::SumState>(
        sumFunction->createInitialNullAggregateState());
    sumFunction->updateAllState(
        (uint8_t*)sumState.get(), int64ValueVector.get(), 1 /* multiplicity */);
    auto otherSumState =
        ku_static_unique_pointer_cast<AggregateState, SumFunction<int64_t>::SumState>(
            sumFunction->createInitialNullAggregateState());
    otherSumState->sum = 10;
    otherSumState->isNull = false;
    sumFunction->combineState((uint8_t*)sumState.get(), (uint8_t*)otherSumState.get());
    sumFunction->finalizeState((uint8_t*)sumState.get());
    auto sumValue = otherSumState->sum;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i;
        }
    }
    ASSERT_EQ(sumState->sum, sumValue);
}

TEST_F(AggrExpressionEvaluatorTest, DOUBLESumTest) {
    auto sumFunction = AggregateFunctionUtil::getSumFunction(DataType(DOUBLE), false);
    auto sumState = ku_static_unique_pointer_cast<AggregateState, SumFunction<double_t>::SumState>(
        sumFunction->createInitialNullAggregateState());
    sumFunction->updateAllState(
        (uint8_t*)sumState.get(), doubleValueVector.get(), 1 /* multiplicity */);
    auto otherSumState =
        ku_static_unique_pointer_cast<AggregateState, SumFunction<double_t>::SumState>(
            sumFunction->createInitialNullAggregateState());
    otherSumState->sum = 10.0;
    otherSumState->isNull = false;
    sumFunction->combineState((uint8_t*)sumState.get(), (uint8_t*)otherSumState.get());
    sumFunction->finalizeState((uint8_t*)sumState.get());
    auto sumValue = otherSumState->sum;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i * 1.5;
        }
    }
    ASSERT_EQ(sumState->sum, sumValue);
}

TEST_F(AggrExpressionEvaluatorTest, INT64AvgTest) {
    auto avgFunction = AggregateFunctionUtil::getAvgFunction(DataType(INT64), false);
    auto avgState = ku_static_unique_pointer_cast<AggregateState, AvgFunction<int64_t>::AvgState>(
        avgFunction->createInitialNullAggregateState());
    avgFunction->updateAllState(
        (uint8_t*)avgState.get(), int64ValueVector.get(), 1 /* multiplicity */);
    auto otherAvgState =
        ku_static_unique_pointer_cast<AggregateState, AvgFunction<int64_t>::AvgState>(
            avgFunction->createInitialNullAggregateState());
    otherAvgState->sum = 10;
    otherAvgState->count = 1;
    otherAvgState->isNull = false;
    avgFunction->combineState((uint8_t*)avgState.get(), (uint8_t*)otherAvgState.get());
    avgFunction->finalizeState((uint8_t*)avgState.get());
    auto sumValue = 10;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i;
        }
    }
    ASSERT_EQ(avgState->avg, (double_t)(sumValue) / (double_t)51);
}

TEST_F(AggrExpressionEvaluatorTest, DOUBLEAvgTest) {
    auto avgFunction = AggregateFunctionUtil::getAvgFunction(DataType(DOUBLE), false);
    auto avgState = ku_static_unique_pointer_cast<AggregateState, AvgFunction<double_t>::AvgState>(
        avgFunction->createInitialNullAggregateState());
    avgFunction->updateAllState(
        (uint8_t*)avgState.get(), doubleValueVector.get(), 1 /* multiplicity */);
    auto otherAvgState =
        ku_static_unique_pointer_cast<AggregateState, AvgFunction<double_t>::AvgState>(
            avgFunction->createInitialNullAggregateState());
    otherAvgState->sum = 10.0;
    otherAvgState->count = 1;
    otherAvgState->isNull = false;
    avgFunction->combineState((uint8_t*)avgState.get(), (uint8_t*)otherAvgState.get());
    avgFunction->finalizeState((uint8_t*)avgState.get());
    auto sumValue = 10.0;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i * 1.5;
        }
    }
    ASSERT_EQ(avgState->avg, (double_t)(sumValue) / (double_t)51);
}

TEST_F(AggrExpressionEvaluatorTest, INT64MaxTest) {
    auto maxFunction = AggregateFunctionUtil::getMaxFunction(DataType(INT64), false);
    auto maxState =
        ku_static_unique_pointer_cast<AggregateState, MinMaxFunction<int64_t>::MinMaxState>(
            maxFunction->createInitialNullAggregateState());
    maxFunction->updateAllState(
        (uint8_t*)maxState.get(), int64ValueVector.get(), 1 /* multiplicity */);
    auto otherMaxState =
        ku_static_unique_pointer_cast<AggregateState, MinMaxFunction<int64_t>::MinMaxState>(
            maxFunction->createInitialNullAggregateState());
    otherMaxState->val = 101;
    otherMaxState->isNull = false;
    maxFunction->combineState((uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    maxFunction->finalizeState((uint8_t*)maxState.get());
    ASSERT_EQ(maxState->val, 101);
}

TEST_F(AggrExpressionEvaluatorTest, DOUBLEMaxTest) {
    auto maxFunction = AggregateFunctionUtil::getMaxFunction(DataType(DOUBLE), false);
    auto maxState =
        ku_static_unique_pointer_cast<AggregateState, MinMaxFunction<double_t>::MinMaxState>(
            maxFunction->createInitialNullAggregateState());
    maxFunction->updateAllState(
        (uint8_t*)maxState.get(), doubleValueVector.get(), 1 /* multiplicity */);
    auto otherMaxState =
        ku_static_unique_pointer_cast<AggregateState, MinMaxFunction<double_t>::MinMaxState>(
            maxFunction->createInitialNullAggregateState());
    otherMaxState->val = 101.0;
    otherMaxState->isNull = false;
    maxFunction->combineState((uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    maxFunction->finalizeState((uint8_t*)maxState.get());
    ASSERT_EQ(maxState->val, 148.5);
}

TEST_F(AggrExpressionEvaluatorTest, STRINGMaxTest) {
    auto maxFunction = AggregateFunctionUtil::getMaxFunction(DataType(STRING), false);
    auto maxState =
        ku_static_unique_pointer_cast<AggregateState, MinMaxFunction<ku_string_t>::MinMaxState>(
            maxFunction->createInitialNullAggregateState());
    maxFunction->updateAllState(
        (uint8_t*)maxState.get(), stringValueVector.get(), 1 /* multiplicity */);
    auto otherMaxState =
        ku_static_unique_pointer_cast<AggregateState, MinMaxFunction<ku_string_t>::MinMaxState>(
            maxFunction->createInitialNullAggregateState());
    ku_string_t otherStr;
    otherStr.set("0");
    otherMaxState->val = otherStr;
    otherMaxState->isNull = false;
    maxFunction->combineState((uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    maxFunction->finalizeState((uint8_t*)maxState.get());
    ASSERT_EQ(maxState->val.getAsString(), "99");
}

TEST_F(AggrExpressionEvaluatorTest, INT64MinTest) {
    auto minFunction = AggregateFunctionUtil::getMinFunction(DataType(INT64), false);
    auto minState =
        ku_static_unique_pointer_cast<AggregateState, MinMaxFunction<int64_t>::MinMaxState>(
            minFunction->createInitialNullAggregateState());
    minFunction->updateAllState(
        (uint8_t*)minState.get(), int64ValueVector.get(), 1 /* multiplicity */);
    auto otherMinState =
        ku_static_unique_pointer_cast<AggregateState, MinMaxFunction<int64_t>::MinMaxState>(
            minFunction->createInitialNullAggregateState());
    otherMinState->val = -10;
    otherMinState->isNull = false;
    minFunction->combineState((uint8_t*)minState.get(), (uint8_t*)otherMinState.get());
    minFunction->finalizeState((uint8_t*)minState.get());
    ASSERT_EQ(minState->val, -10);
}

#include "gtest/gtest.h"

#include "src/common/include/utils.h"
#include "src/function/include/aggregate/aggregate_function.h"
#include "src/function/include/aggregate/avg.h"
#include "src/function/include/aggregate/count.h"
#include "src/function/include/aggregate/min_max.h"
#include "src/function/include/aggregate/sum.h"
#include "src/processor/include/physical_plan/result/result_set.h"

using ::testing::Test;

using namespace graphflow::processor;
using namespace graphflow::function;

class AggrExpressionEvaluatorTest : public Test {

public:
    void SetUp() override {
        memoryManager = make_unique<MemoryManager>();
        int64ValueVector = make_shared<ValueVector>(memoryManager.get(), INT64);
        doubleValueVector = make_shared<ValueVector>(memoryManager.get(), DOUBLE);
        stringValueVector = make_shared<ValueVector>(memoryManager.get(), STRING);
        unStrValueVector = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
        dataChunk = make_shared<DataChunk>(4);
        auto int64Values = (int64_t*)int64ValueVector->values;
        auto doubleValues = (double_t*)doubleValueVector->values;
        auto unStrValues = (Value*)unStrValueVector->values;
        for (auto i = 0u; i < 100; i++) {
            int64Values[i] = i;
            doubleValues[i] = (double)i * 1.5;
            stringValueVector->addString(i, to_string(i));
            unStrValues[i] = Value((int64_t)i);
            if (i % 2 == 0) {
                int64ValueVector->setNull(i, true /* isNull */);
                doubleValueVector->setNull(i, true /* isNull */);
                stringValueVector->setNull(i, true /* isNull */);
                unStrValueVector->setNull(i, true /* isNull */);
            }
        }
        dataChunk->state->selectedSize = 100;
        dataChunk->insert(0, int64ValueVector);
        dataChunk->insert(1, doubleValueVector);
        dataChunk->insert(2, stringValueVector);
        dataChunk->insert(3, unStrValueVector);
        resultSet = make_shared<ResultSet>(1);
        resultSet->insert(0, dataChunk);
    }

public:
    unique_ptr<MemoryManager> memoryManager;
    shared_ptr<ValueVector> int64ValueVector;
    shared_ptr<ValueVector> doubleValueVector;
    shared_ptr<ValueVector> stringValueVector;
    shared_ptr<ValueVector> unStrValueVector;
    shared_ptr<DataChunk> dataChunk;
    shared_ptr<ResultSet> resultSet;
};

TEST_F(AggrExpressionEvaluatorTest, CountStarTest) {
    auto countFunction = AggregateFunctionUtil::getCountStarFunction();
    auto countStarState = static_unique_pointer_cast<AggregateState, BaseCountFunction::CountState>(
        countFunction->createInitialNullAggregateState());
    countFunction->updateState((uint8_t*)countStarState.get(), nullptr,
        resultSet->getNumTuples(unordered_set<uint32_t>{0}));
    auto otherCountStarState =
        static_unique_pointer_cast<AggregateState, BaseCountFunction::CountState>(
            countFunction->createInitialNullAggregateState());
    otherCountStarState->val = 10;
    countFunction->combineState(
        (uint8_t*)countStarState.get(), (uint8_t*)otherCountStarState.get());
    countFunction->finalizeState((uint8_t*)countStarState.get());
    ASSERT_EQ(countStarState->val, 110);
}

TEST_F(AggrExpressionEvaluatorTest, CountTest) {
    auto countFunction = AggregateFunctionUtil::getAggregateFunction(COUNT_FUNC, INT64);
    auto countState = static_unique_pointer_cast<AggregateState, BaseCountFunction::CountState>(
        countFunction->createInitialNullAggregateState());
    countFunction->updateState(
        (uint8_t*)countState.get(), int64ValueVector.get(), 1 /* multiplicity */);
    auto otherCountState =
        static_unique_pointer_cast<AggregateState, BaseCountFunction::CountState>(
            countFunction->createInitialNullAggregateState());
    otherCountState->val = 10;
    countFunction->combineState((uint8_t*)countState.get(), (uint8_t*)otherCountState.get());
    countFunction->finalizeState((uint8_t*)countState.get());
    ASSERT_EQ(countState->val, 60);
}

TEST_F(AggrExpressionEvaluatorTest, INT64SumTest) {
    auto sumFunction = AggregateFunctionUtil::getAggregateFunction(SUM_FUNC, INT64);
    auto sumState = static_unique_pointer_cast<AggregateState, SumFunction<int64_t>::SumState>(
        sumFunction->createInitialNullAggregateState());
    sumFunction->updateState(
        (uint8_t*)sumState.get(), int64ValueVector.get(), 1 /* multiplicity */);
    auto otherSumState = static_unique_pointer_cast<AggregateState, SumFunction<int64_t>::SumState>(
        sumFunction->createInitialNullAggregateState());
    otherSumState->val = 10;
    otherSumState->isNull = false;
    sumFunction->combineState((uint8_t*)sumState.get(), (uint8_t*)otherSumState.get());
    sumFunction->finalizeState((uint8_t*)sumState.get());
    auto sumValue = otherSumState->val;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i;
        }
    }
    ASSERT_EQ(sumState->val, sumValue);
}

TEST_F(AggrExpressionEvaluatorTest, DOUBLESumTest) {
    auto sumFunction = AggregateFunctionUtil::getAggregateFunction(SUM_FUNC, DOUBLE);
    auto sumState = static_unique_pointer_cast<AggregateState, SumFunction<double_t>::SumState>(
        sumFunction->createInitialNullAggregateState());
    sumFunction->updateState(
        (uint8_t*)sumState.get(), doubleValueVector.get(), 1 /* multiplicity */);
    auto otherSumState =
        static_unique_pointer_cast<AggregateState, SumFunction<double_t>::SumState>(
            sumFunction->createInitialNullAggregateState());
    otherSumState->val = 10.0;
    otherSumState->isNull = false;
    sumFunction->combineState((uint8_t*)sumState.get(), (uint8_t*)otherSumState.get());
    sumFunction->finalizeState((uint8_t*)sumState.get());
    auto sumValue = otherSumState->val;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i * 1.5;
        }
    }
    ASSERT_EQ(sumState->val, sumValue);
}

TEST_F(AggrExpressionEvaluatorTest, UNSTRSumTest) {
    auto sumFunction = AggregateFunctionUtil::getAggregateFunction(SUM_FUNC, UNSTRUCTURED);
    auto sumState = static_unique_pointer_cast<AggregateState, SumFunction<Value>::SumState>(
        sumFunction->createInitialNullAggregateState());
    sumFunction->updateState(
        (uint8_t*)sumState.get(), unStrValueVector.get(), 1 /* multiplicity */);
    auto otherSumState = static_unique_pointer_cast<AggregateState, SumFunction<Value>::SumState>(
        sumFunction->createInitialNullAggregateState());
    otherSumState->val = Value((int64_t)10);
    otherSumState->isNull = false;
    sumFunction->combineState((uint8_t*)sumState.get(), (uint8_t*)otherSumState.get());
    sumFunction->finalizeState((uint8_t*)sumState.get());
    auto sumValue = otherSumState->val.val.int64Val;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i;
        }
    }
    ASSERT_EQ(sumState->val.val.int64Val, sumValue);
}

TEST_F(AggrExpressionEvaluatorTest, INT64AvgTest) {
    auto avgFunction = AggregateFunctionUtil::getAggregateFunction(AVG_FUNC, INT64);
    auto avgState = static_unique_pointer_cast<AggregateState, AvgFunction<int64_t>::AvgState>(
        avgFunction->createInitialNullAggregateState());
    avgFunction->updateState(
        (uint8_t*)avgState.get(), int64ValueVector.get(), 1 /* multiplicity */);
    auto otherAvgState = static_unique_pointer_cast<AggregateState, AvgFunction<int64_t>::AvgState>(
        avgFunction->createInitialNullAggregateState());
    otherAvgState->val = 10;
    otherAvgState->numValues = 1;
    otherAvgState->isNull = false;
    avgFunction->combineState((uint8_t*)avgState.get(), (uint8_t*)otherAvgState.get());
    avgFunction->finalizeState((uint8_t*)avgState.get());
    auto sumValue = 10;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i;
        }
    }
    ASSERT_EQ(avgState->val, (double_t)(sumValue) / (double_t)51);
}

TEST_F(AggrExpressionEvaluatorTest, DOUBLEAvgTest) {
    auto avgFunction = AggregateFunctionUtil::getAggregateFunction(AVG_FUNC, DOUBLE);
    auto avgState = static_unique_pointer_cast<AggregateState, AvgFunction<double_t>::AvgState>(
        avgFunction->createInitialNullAggregateState());
    avgFunction->updateState(
        (uint8_t*)avgState.get(), doubleValueVector.get(), 1 /* multiplicity */);
    auto otherAvgState =
        static_unique_pointer_cast<AggregateState, AvgFunction<double_t>::AvgState>(
            avgFunction->createInitialNullAggregateState());
    otherAvgState->val = 10.0;
    otherAvgState->numValues = 1;
    otherAvgState->isNull = false;
    avgFunction->combineState((uint8_t*)avgState.get(), (uint8_t*)otherAvgState.get());
    avgFunction->finalizeState((uint8_t*)avgState.get());
    auto sumValue = 10.0;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i * 1.5;
        }
    }
    ASSERT_EQ(avgState->val, (double_t)(sumValue) / (double_t)51);
}

TEST_F(AggrExpressionEvaluatorTest, INT64MaxTest) {
    auto maxFunction = AggregateFunctionUtil::getAggregateFunction(MAX_FUNC, INT64);
    auto maxState =
        static_unique_pointer_cast<AggregateState, MinMaxFunction<int64_t>::MinMaxState>(
            maxFunction->createInitialNullAggregateState());
    maxFunction->updateState(
        (uint8_t*)maxState.get(), int64ValueVector.get(), 1 /* multiplicity */);
    auto otherMaxState =
        static_unique_pointer_cast<AggregateState, MinMaxFunction<int64_t>::MinMaxState>(
            maxFunction->createInitialNullAggregateState());
    otherMaxState->val = 101;
    otherMaxState->isNull = false;
    maxFunction->combineState((uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    maxFunction->finalizeState((uint8_t*)maxState.get());
    ASSERT_EQ(maxState->val, 101);
}

TEST_F(AggrExpressionEvaluatorTest, DOUBLEMaxTest) {
    auto maxFunction = AggregateFunctionUtil::getAggregateFunction(MAX_FUNC, DOUBLE);
    auto maxState =
        static_unique_pointer_cast<AggregateState, MinMaxFunction<double_t>::MinMaxState>(
            maxFunction->createInitialNullAggregateState());
    maxFunction->updateState(
        (uint8_t*)maxState.get(), doubleValueVector.get(), 1 /* multiplicity */);
    auto otherMaxState =
        static_unique_pointer_cast<AggregateState, MinMaxFunction<double_t>::MinMaxState>(
            maxFunction->createInitialNullAggregateState());
    otherMaxState->val = 101.0;
    otherMaxState->isNull = false;
    maxFunction->combineState((uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    maxFunction->finalizeState((uint8_t*)maxState.get());
    ASSERT_EQ(maxState->val, 148.5);
}

TEST_F(AggrExpressionEvaluatorTest, STRINGMaxTest) {
    auto maxFunction = AggregateFunctionUtil::getAggregateFunction(MAX_FUNC, STRING);
    auto maxState =
        static_unique_pointer_cast<AggregateState, MinMaxFunction<gf_string_t>::MinMaxState>(
            maxFunction->createInitialNullAggregateState());
    maxFunction->updateState(
        (uint8_t*)maxState.get(), stringValueVector.get(), 1 /* multiplicity */);
    auto otherMaxState =
        static_unique_pointer_cast<AggregateState, MinMaxFunction<gf_string_t>::MinMaxState>(
            maxFunction->createInitialNullAggregateState());
    gf_string_t otherStr;
    otherStr.set("0");
    otherMaxState->val = otherStr;
    otherMaxState->isNull = false;
    maxFunction->combineState((uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    maxFunction->finalizeState((uint8_t*)maxState.get());
    ASSERT_EQ(maxState->val.getAsString(), "99");
}

TEST_F(AggrExpressionEvaluatorTest, UNSTRMaxTest) {
    auto maxFunction = AggregateFunctionUtil::getAggregateFunction(MAX_FUNC, UNSTRUCTURED);
    auto maxState = static_unique_pointer_cast<AggregateState, MinMaxFunction<Value>::MinMaxState>(
        maxFunction->createInitialNullAggregateState());
    maxFunction->updateState(
        (uint8_t*)maxState.get(), unStrValueVector.get(), 1 /* multiplicity */);
    auto otherMaxState =
        static_unique_pointer_cast<AggregateState, MinMaxFunction<Value>::MinMaxState>(
            maxFunction->createInitialNullAggregateState());
    otherMaxState->val = Value((int64_t)101);
    otherMaxState->isNull = false;
    maxFunction->combineState((uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    maxFunction->finalizeState((uint8_t*)maxState.get());
    ASSERT_EQ(maxState->val.val.int64Val, 101);
}

TEST_F(AggrExpressionEvaluatorTest, INT64MinTest) {
    auto minFunction = AggregateFunctionUtil::getAggregateFunction(MIN_FUNC, INT64);
    auto minState =
        static_unique_pointer_cast<AggregateState, MinMaxFunction<int64_t>::MinMaxState>(
            minFunction->createInitialNullAggregateState());
    minFunction->updateState(
        (uint8_t*)minState.get(), int64ValueVector.get(), 1 /* multiplicity */);
    auto otherMinState =
        static_unique_pointer_cast<AggregateState, MinMaxFunction<int64_t>::MinMaxState>(
            minFunction->createInitialNullAggregateState());
    otherMinState->val = -10;
    otherMinState->isNull = false;
    minFunction->combineState((uint8_t*)minState.get(), (uint8_t*)otherMinState.get());
    minFunction->finalizeState((uint8_t*)minState.get());
    ASSERT_EQ(minState->val, -10);
}

#include "gtest/gtest.h"

#include "src/common/include/utils.h"
#include "src/expression_evaluator/include/aggregate_expression_evaluator.h"
#include "src/function/include/aggregation/avg.h"
#include "src/function/include/aggregation/count.h"
#include "src/function/include/aggregation/min_max.h"
#include "src/function/include/aggregation/sum.h"

using ::testing::Test;

using namespace graphflow::processor;
using namespace graphflow::function;
using namespace graphflow::evaluator;

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
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(COUNT_STAR_FUNC, INT64,
        AggregateExpressionEvaluator::getAggregationFunction(COUNT_STAR_FUNC, INT64));
    auto countStarState =
        static_unique_pointer_cast<AggregationState, CountFunction<true>::CountState>(
            exprEvaluator->getFunction()->initialize());
    exprEvaluator->getFunction()->update((uint8_t*)countStarState.get(), nullptr,
        resultSet->getNumTuples(unordered_set<uint32_t>{0}));
    auto otherCountStarState =
        static_unique_pointer_cast<AggregationState, CountFunction<true>::CountState>(
            exprEvaluator->getFunction()->initialize());
    otherCountStarState->val = 10;
    exprEvaluator->getFunction()->combine(
        (uint8_t*)countStarState.get(), (uint8_t*)otherCountStarState.get());
    exprEvaluator->getFunction()->finalize((uint8_t*)countStarState.get());
    ASSERT_EQ(countStarState->val, 110);
}

TEST_F(AggrExpressionEvaluatorTest, CountTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        COUNT_FUNC, INT64, AggregateExpressionEvaluator::getAggregationFunction(COUNT_FUNC, INT64));
    auto countState =
        static_unique_pointer_cast<AggregationState, CountFunction<false>::CountState>(
            exprEvaluator->getFunction()->initialize());
    exprEvaluator->getFunction()->update(
        (uint8_t*)countState.get(), int64ValueVector.get(), int64ValueVector->state->selectedSize);
    auto otherCountState =
        static_unique_pointer_cast<AggregationState, CountFunction<false>::CountState>(
            exprEvaluator->getFunction()->initialize());
    otherCountState->val = 10;
    exprEvaluator->getFunction()->combine(
        (uint8_t*)countState.get(), (uint8_t*)otherCountState.get());
    exprEvaluator->getFunction()->finalize((uint8_t*)countState.get());
    ASSERT_EQ(countState->val, 60);
}

TEST_F(AggrExpressionEvaluatorTest, INT64SumTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        SUM_FUNC, INT64, AggregateExpressionEvaluator::getAggregationFunction(SUM_FUNC, INT64));
    auto sumState = static_unique_pointer_cast<AggregationState, SumFunction<int64_t>::SumState>(
        exprEvaluator->getFunction()->initialize());
    exprEvaluator->getFunction()->update(
        (uint8_t*)sumState.get(), int64ValueVector.get(), int64ValueVector->state->selectedSize);
    auto otherSumState =
        static_unique_pointer_cast<AggregationState, SumFunction<int64_t>::SumState>(
            exprEvaluator->getFunction()->initialize());
    otherSumState->val = 10;
    otherSumState->isNull = false;
    exprEvaluator->getFunction()->combine((uint8_t*)sumState.get(), (uint8_t*)otherSumState.get());
    exprEvaluator->getFunction()->finalize((uint8_t*)sumState.get());
    auto sumValue = otherSumState->val;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i;
        }
    }
    ASSERT_EQ(sumState->val, sumValue);
}

TEST_F(AggrExpressionEvaluatorTest, DOUBLESumTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        SUM_FUNC, DOUBLE, AggregateExpressionEvaluator::getAggregationFunction(SUM_FUNC, DOUBLE));
    auto sumState = static_unique_pointer_cast<AggregationState, SumFunction<double_t>::SumState>(
        exprEvaluator->getFunction()->initialize());
    exprEvaluator->getFunction()->update(
        (uint8_t*)sumState.get(), doubleValueVector.get(), doubleValueVector->state->selectedSize);
    auto otherSumState =
        static_unique_pointer_cast<AggregationState, SumFunction<double_t>::SumState>(
            exprEvaluator->getFunction()->initialize());
    otherSumState->val = 10.0;
    otherSumState->isNull = false;
    exprEvaluator->getFunction()->combine((uint8_t*)sumState.get(), (uint8_t*)otherSumState.get());
    exprEvaluator->getFunction()->finalize((uint8_t*)sumState.get());
    auto sumValue = otherSumState->val;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i * 1.5;
        }
    }
    ASSERT_EQ(sumState->val, sumValue);
}

TEST_F(AggrExpressionEvaluatorTest, UNSTRSumTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(SUM_FUNC, UNSTRUCTURED,
        AggregateExpressionEvaluator::getAggregationFunction(SUM_FUNC, UNSTRUCTURED));
    auto sumState = static_unique_pointer_cast<AggregationState, SumFunction<Value>::SumState>(
        exprEvaluator->getFunction()->initialize());
    exprEvaluator->getFunction()->update(
        (uint8_t*)sumState.get(), unStrValueVector.get(), unStrValueVector->state->selectedSize);
    auto otherSumState = static_unique_pointer_cast<AggregationState, SumFunction<Value>::SumState>(
        exprEvaluator->getFunction()->initialize());
    otherSumState->val = Value((int64_t)10);
    otherSumState->isNull = false;
    exprEvaluator->getFunction()->combine((uint8_t*)sumState.get(), (uint8_t*)otherSumState.get());
    exprEvaluator->getFunction()->finalize((uint8_t*)sumState.get());
    auto sumValue = otherSumState->val.val.int64Val;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i;
        }
    }
    ASSERT_EQ(sumState->val.val.int64Val, sumValue);
}

TEST_F(AggrExpressionEvaluatorTest, INT64AvgTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        AVG_FUNC, DOUBLE, AggregateExpressionEvaluator::getAggregationFunction(AVG_FUNC, INT64));
    auto avgState = static_unique_pointer_cast<AggregationState, AvgFunction<int64_t>::AvgState>(
        exprEvaluator->getFunction()->initialize());
    exprEvaluator->getFunction()->update(
        (uint8_t*)avgState.get(), int64ValueVector.get(), int64ValueVector->state->selectedSize);
    auto otherAvgState =
        static_unique_pointer_cast<AggregationState, AvgFunction<int64_t>::AvgState>(
            exprEvaluator->getFunction()->initialize());
    otherAvgState->val = 10;
    otherAvgState->numValues = 1;
    otherAvgState->isNull = false;
    exprEvaluator->getFunction()->combine((uint8_t*)avgState.get(), (uint8_t*)otherAvgState.get());
    exprEvaluator->getFunction()->finalize((uint8_t*)avgState.get());
    auto sumValue = 10;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i;
        }
    }
    ASSERT_EQ(avgState->val, (double_t)(sumValue) / (double_t)51);
}

TEST_F(AggrExpressionEvaluatorTest, DOUBLEAvgTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        AVG_FUNC, DOUBLE, AggregateExpressionEvaluator::getAggregationFunction(AVG_FUNC, DOUBLE));
    auto avgState = static_unique_pointer_cast<AggregationState, AvgFunction<double_t>::AvgState>(
        exprEvaluator->getFunction()->initialize());
    exprEvaluator->getFunction()->update(
        (uint8_t*)avgState.get(), doubleValueVector.get(), doubleValueVector->state->selectedSize);
    auto otherAvgState =
        static_unique_pointer_cast<AggregationState, AvgFunction<double_t>::AvgState>(
            exprEvaluator->getFunction()->initialize());
    otherAvgState->val = 10.0;
    otherAvgState->numValues = 1;
    otherAvgState->isNull = false;
    exprEvaluator->getFunction()->combine((uint8_t*)avgState.get(), (uint8_t*)otherAvgState.get());
    exprEvaluator->getFunction()->finalize((uint8_t*)avgState.get());
    auto sumValue = 10.0;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i * 1.5;
        }
    }
    ASSERT_EQ(avgState->val, (double_t)(sumValue) / (double_t)51);
}

TEST_F(AggrExpressionEvaluatorTest, INT64MaxTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        MAX_FUNC, INT64, AggregateExpressionEvaluator::getAggregationFunction(MAX_FUNC, INT64));
    auto maxState =
        static_unique_pointer_cast<AggregationState, MinMaxFunction<int64_t>::MinMaxState>(
            exprEvaluator->getFunction()->initialize());
    exprEvaluator->getFunction()->update(
        (uint8_t*)maxState.get(), int64ValueVector.get(), int64ValueVector->state->selectedSize);
    auto otherMaxState =
        static_unique_pointer_cast<AggregationState, MinMaxFunction<int64_t>::MinMaxState>(
            exprEvaluator->getFunction()->initialize());
    otherMaxState->val = 101;
    otherMaxState->isNull = false;
    exprEvaluator->getFunction()->combine((uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    exprEvaluator->getFunction()->finalize((uint8_t*)maxState.get());
    ASSERT_EQ(maxState->val, 101);
}

TEST_F(AggrExpressionEvaluatorTest, DOUBLEMaxTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        MAX_FUNC, DOUBLE, AggregateExpressionEvaluator::getAggregationFunction(MAX_FUNC, DOUBLE));
    auto maxState =
        static_unique_pointer_cast<AggregationState, MinMaxFunction<double_t>::MinMaxState>(
            exprEvaluator->getFunction()->initialize());
    exprEvaluator->getFunction()->update(
        (uint8_t*)maxState.get(), doubleValueVector.get(), doubleValueVector->state->selectedSize);
    auto otherMaxState =
        static_unique_pointer_cast<AggregationState, MinMaxFunction<double_t>::MinMaxState>(
            exprEvaluator->getFunction()->initialize());
    otherMaxState->val = 101.0;
    otherMaxState->isNull = false;
    exprEvaluator->getFunction()->combine((uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    exprEvaluator->getFunction()->finalize((uint8_t*)maxState.get());
    ASSERT_EQ(maxState->val, 148.5);
}

TEST_F(AggrExpressionEvaluatorTest, STRINGMaxTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        MAX_FUNC, STRING, AggregateExpressionEvaluator::getAggregationFunction(MAX_FUNC, STRING));
    auto maxState =
        static_unique_pointer_cast<AggregationState, MinMaxFunction<gf_string_t>::MinMaxState>(
            exprEvaluator->getFunction()->initialize());
    exprEvaluator->getFunction()->update(
        (uint8_t*)maxState.get(), stringValueVector.get(), stringValueVector->state->selectedSize);
    auto otherMaxState =
        static_unique_pointer_cast<AggregationState, MinMaxFunction<gf_string_t>::MinMaxState>(
            exprEvaluator->getFunction()->initialize());
    gf_string_t otherStr;
    otherStr.set("0");
    otherMaxState->val = otherStr;
    otherMaxState->isNull = false;
    exprEvaluator->getFunction()->combine((uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    exprEvaluator->getFunction()->finalize((uint8_t*)maxState.get());
    ASSERT_EQ(maxState->val.getAsString(), "99");
}

TEST_F(AggrExpressionEvaluatorTest, UNSTRMaxTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(MAX_FUNC, UNSTRUCTURED,
        AggregateExpressionEvaluator::getAggregationFunction(MAX_FUNC, UNSTRUCTURED));
    auto maxState =
        static_unique_pointer_cast<AggregationState, MinMaxFunction<Value>::MinMaxState>(
            exprEvaluator->getFunction()->initialize());
    exprEvaluator->getFunction()->update(
        (uint8_t*)maxState.get(), unStrValueVector.get(), unStrValueVector->state->selectedSize);
    auto otherMaxState =
        static_unique_pointer_cast<AggregationState, MinMaxFunction<Value>::MinMaxState>(
            exprEvaluator->getFunction()->initialize());
    otherMaxState->val = Value((int64_t)101);
    otherMaxState->isNull = false;
    exprEvaluator->getFunction()->combine((uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    exprEvaluator->getFunction()->finalize((uint8_t*)maxState.get());
    ASSERT_EQ(maxState->val.val.int64Val, 101);
}

TEST_F(AggrExpressionEvaluatorTest, INT64MinTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        MIN_FUNC, INT64, AggregateExpressionEvaluator::getAggregationFunction(MIN_FUNC, INT64));
    auto minState =
        static_unique_pointer_cast<AggregationState, MinMaxFunction<int64_t>::MinMaxState>(
            exprEvaluator->getFunction()->initialize());
    exprEvaluator->getFunction()->update(
        (uint8_t*)minState.get(), int64ValueVector.get(), int64ValueVector->state->selectedSize);
    auto otherMinState =
        static_unique_pointer_cast<AggregationState, MinMaxFunction<int64_t>::MinMaxState>(
            exprEvaluator->getFunction()->initialize());
    otherMinState->val = -10;
    otherMinState->isNull = false;
    exprEvaluator->getFunction()->combine((uint8_t*)minState.get(), (uint8_t*)otherMinState.get());
    exprEvaluator->getFunction()->finalize((uint8_t*)minState.get());
    ASSERT_EQ(minState->val, -10);
}

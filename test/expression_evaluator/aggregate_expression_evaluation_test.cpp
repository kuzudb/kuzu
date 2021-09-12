#include "gtest/gtest.h"

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
            doubleValues[i] = i * 1.5;
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
    auto countStarAggrEvaluator =
        reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto countStarState = make_unique<CountFunction<true>::CountState>();
    countStarAggrEvaluator->getFunction()->initialize((uint8_t*)countStarState.get());
    countStarAggrEvaluator->getFunction()->update(
        (uint8_t*)countStarState.get(), nullptr, resultSet->getNumTuples());
    auto otherCountStarState = make_unique<CountFunction<true>::CountState>();
    otherCountStarState->val = 10;
    countStarAggrEvaluator->getFunction()->combine(
        (uint8_t*)countStarState.get(), (uint8_t*)otherCountStarState.get());
    auto countStarFinalState = make_unique<CountFunction<true>::CountState>();
    countStarAggrEvaluator->getFunction()->finalize(
        (uint8_t*)countStarState.get(), (uint8_t*)countStarFinalState.get());
    ASSERT_EQ(countStarFinalState->val, 110);
}

TEST_F(AggrExpressionEvaluatorTest, CountTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        COUNT_FUNC, INT64, AggregateExpressionEvaluator::getAggregationFunction(COUNT_FUNC, INT64));
    auto countAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto countState = make_unique<CountFunction<false>::CountState>();
    countAggrEvaluator->getFunction()->initialize((uint8_t*)countState.get());
    countAggrEvaluator->getFunction()->update(
        (uint8_t*)countState.get(), int64ValueVector.get(), int64ValueVector->state->selectedSize);
    auto otherCountState = make_unique<CountFunction<false>::CountState>();
    otherCountState->val = 10;
    countAggrEvaluator->getFunction()->combine(
        (uint8_t*)countState.get(), (uint8_t*)otherCountState.get());
    auto countFinalState = make_unique<CountFunction<false>::CountState>();
    countAggrEvaluator->getFunction()->finalize(
        (uint8_t*)countState.get(), (uint8_t*)countFinalState.get());
    ASSERT_EQ(countFinalState->val, 60);
}

TEST_F(AggrExpressionEvaluatorTest, INT64SumTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        SUM_FUNC, INT64, AggregateExpressionEvaluator::getAggregationFunction(SUM_FUNC, INT64));
    auto sumAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto sumState = make_unique<SumFunction<int64_t>::SumState>();
    sumAggrEvaluator->getFunction()->initialize((uint8_t*)sumState.get());
    sumAggrEvaluator->getFunction()->update(
        (uint8_t*)sumState.get(), int64ValueVector.get(), int64ValueVector->state->selectedSize);
    auto otherSumState = make_unique<SumFunction<int64_t>::SumState>();
    otherSumState->val = 10;
    sumAggrEvaluator->getFunction()->combine(
        (uint8_t*)sumState.get(), (uint8_t*)otherSumState.get());
    auto sumFinalState = make_unique<SumFunction<int64_t>::SumState>();
    sumAggrEvaluator->getFunction()->finalize(
        (uint8_t*)sumState.get(), (uint8_t*)sumFinalState.get());
    auto sumValue = otherSumState->val;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i;
        }
    }
    ASSERT_EQ(sumFinalState->val, sumValue);
}

TEST_F(AggrExpressionEvaluatorTest, DOUBLESumTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        SUM_FUNC, DOUBLE, AggregateExpressionEvaluator::getAggregationFunction(SUM_FUNC, DOUBLE));
    auto sumAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto sumState = make_unique<SumFunction<double_t>::SumState>();
    sumAggrEvaluator->getFunction()->initialize((uint8_t*)sumState.get());
    sumAggrEvaluator->getFunction()->update(
        (uint8_t*)sumState.get(), doubleValueVector.get(), doubleValueVector->state->selectedSize);
    auto otherSumState = make_unique<SumFunction<double_t>::SumState>();
    otherSumState->val = 10.0;
    sumAggrEvaluator->getFunction()->combine(
        (uint8_t*)sumState.get(), (uint8_t*)otherSumState.get());
    auto sumFinalState = make_unique<SumFunction<double_t>::SumState>();
    sumAggrEvaluator->getFunction()->finalize(
        (uint8_t*)sumState.get(), (uint8_t*)sumFinalState.get());
    auto sumValue = otherSumState->val;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i * 1.5;
        }
    }
    ASSERT_EQ(sumFinalState->val, sumValue);
}

TEST_F(AggrExpressionEvaluatorTest, UNSTRSumTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(SUM_FUNC, UNSTRUCTURED,
        AggregateExpressionEvaluator::getAggregationFunction(SUM_FUNC, UNSTRUCTURED));
    auto sumAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto sumState = make_unique<SumFunction<Value>::SumState>();
    sumAggrEvaluator->getFunction()->initialize((uint8_t*)sumState.get());
    sumAggrEvaluator->getFunction()->update(
        (uint8_t*)sumState.get(), unStrValueVector.get(), unStrValueVector->state->selectedSize);
    auto otherSumState = make_unique<SumFunction<Value>::SumState>();
    otherSumState->val = Value((int64_t)10);
    sumAggrEvaluator->getFunction()->combine(
        (uint8_t*)sumState.get(), (uint8_t*)otherSumState.get());
    auto sumFinalState = make_unique<SumFunction<Value>::SumState>();
    sumAggrEvaluator->getFunction()->finalize(
        (uint8_t*)sumState.get(), (uint8_t*)sumFinalState.get());
    auto sumValue = otherSumState->val.val.int64Val;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i;
        }
    }
    ASSERT_EQ(sumFinalState->val.val.int64Val, sumValue);
}

TEST_F(AggrExpressionEvaluatorTest, INT64AvgTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        AVG_FUNC, DOUBLE, AggregateExpressionEvaluator::getAggregationFunction(AVG_FUNC, INT64));
    auto avgAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto avgState = make_unique<AvgFunction<int64_t>::AvgState>();
    avgAggrEvaluator->getFunction()->initialize((uint8_t*)avgState.get());
    avgAggrEvaluator->getFunction()->update(
        (uint8_t*)avgState.get(), int64ValueVector.get(), int64ValueVector->state->selectedSize);
    auto otherAvgState = make_unique<AvgFunction<int64_t>::AvgState>();
    otherAvgState->val = 10;
    otherAvgState->numValues = 1;
    avgAggrEvaluator->getFunction()->combine(
        (uint8_t*)avgState.get(), (uint8_t*)otherAvgState.get());
    auto avgFinalState = make_unique<AvgFunction<double_t>::AvgState>();
    avgAggrEvaluator->getFunction()->finalize(
        (uint8_t*)avgState.get(), (uint8_t*)avgFinalState.get());
    auto sumValue = otherAvgState->val;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i;
        }
    }
    ASSERT_EQ(avgFinalState->val, (double_t)(sumValue) / (double_t)51);
}

TEST_F(AggrExpressionEvaluatorTest, DOUBLEAvgTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        AVG_FUNC, DOUBLE, AggregateExpressionEvaluator::getAggregationFunction(AVG_FUNC, DOUBLE));
    auto avgAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto avgState = make_unique<AvgFunction<double_t>::AvgState>();
    avgAggrEvaluator->getFunction()->initialize((uint8_t*)avgState.get());
    avgAggrEvaluator->getFunction()->update(
        (uint8_t*)avgState.get(), doubleValueVector.get(), doubleValueVector->state->selectedSize);
    auto otherAvgState = make_unique<AvgFunction<double_t>::AvgState>();
    otherAvgState->val = 10.0;
    otherAvgState->numValues = 1;
    avgAggrEvaluator->getFunction()->combine(
        (uint8_t*)avgState.get(), (uint8_t*)otherAvgState.get());
    auto avgFinalState = make_unique<AvgFunction<double_t>::AvgState>();
    avgAggrEvaluator->getFunction()->finalize(
        (uint8_t*)avgState.get(), (uint8_t*)avgFinalState.get());
    auto sumValue = otherAvgState->val;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i * 1.5;
        }
    }
    ASSERT_EQ(avgFinalState->val, (double_t)(sumValue) / (double_t)51);
}

TEST_F(AggrExpressionEvaluatorTest, INT64MaxTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        MAX_FUNC, INT64, AggregateExpressionEvaluator::getAggregationFunction(MAX_FUNC, INT64));
    auto maxAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto maxState = make_unique<MinMaxFunction<int64_t>::MinMaxState>();
    maxAggrEvaluator->getFunction()->initialize((uint8_t*)maxState.get());
    maxAggrEvaluator->getFunction()->update(
        (uint8_t*)maxState.get(), int64ValueVector.get(), int64ValueVector->state->selectedSize);
    auto otherMaxState = make_unique<MinMaxFunction<int64_t>::MinMaxState>();
    otherMaxState->val = 101;
    maxAggrEvaluator->getFunction()->combine(
        (uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    auto maxFinalState = make_unique<MinMaxFunction<int64_t>::MinMaxState>();
    maxAggrEvaluator->getFunction()->finalize(
        (uint8_t*)maxState.get(), (uint8_t*)maxFinalState.get());
    ASSERT_EQ(maxFinalState->val, 101);
}

TEST_F(AggrExpressionEvaluatorTest, DOUBLEMaxTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        MAX_FUNC, DOUBLE, AggregateExpressionEvaluator::getAggregationFunction(MAX_FUNC, DOUBLE));
    auto maxAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto maxState = make_unique<MinMaxFunction<double_t>::MinMaxState>();
    maxAggrEvaluator->getFunction()->initialize((uint8_t*)maxState.get());
    maxAggrEvaluator->getFunction()->update(
        (uint8_t*)maxState.get(), doubleValueVector.get(), doubleValueVector->state->selectedSize);
    auto otherMaxState = make_unique<MinMaxFunction<double_t>::MinMaxState>();
    otherMaxState->val = 101.0;
    maxAggrEvaluator->getFunction()->combine(
        (uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    auto maxFinalState = make_unique<MinMaxFunction<double_t>::MinMaxState>();
    maxAggrEvaluator->getFunction()->finalize(
        (uint8_t*)maxState.get(), (uint8_t*)maxFinalState.get());
    ASSERT_EQ(maxFinalState->val, 148.5);
}

TEST_F(AggrExpressionEvaluatorTest, STRINGMaxTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        MAX_FUNC, STRING, AggregateExpressionEvaluator::getAggregationFunction(MAX_FUNC, STRING));
    auto maxAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto maxState = make_unique<MinMaxFunction<gf_string_t>::MinMaxState>();
    maxAggrEvaluator->getFunction()->initialize((uint8_t*)maxState.get());
    maxAggrEvaluator->getFunction()->update(
        (uint8_t*)maxState.get(), stringValueVector.get(), stringValueVector->state->selectedSize);
    auto otherMaxState = make_unique<MinMaxFunction<gf_string_t>::MinMaxState>();
    gf_string_t otherStr;
    otherStr.set("0");
    otherMaxState->val = otherStr;
    maxAggrEvaluator->getFunction()->combine(
        (uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    auto maxFinalState = make_unique<MinMaxFunction<gf_string_t>::MinMaxState>();
    maxAggrEvaluator->getFunction()->finalize(
        (uint8_t*)maxState.get(), (uint8_t*)maxFinalState.get());
    ASSERT_EQ(maxFinalState->val.getAsString(), "99");
}

TEST_F(AggrExpressionEvaluatorTest, UNSTRMaxTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(MAX_FUNC, UNSTRUCTURED,
        AggregateExpressionEvaluator::getAggregationFunction(MAX_FUNC, UNSTRUCTURED));
    auto maxAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto maxState = make_unique<MinMaxFunction<Value>::MinMaxState>();
    maxAggrEvaluator->getFunction()->initialize((uint8_t*)maxState.get());
    maxAggrEvaluator->getFunction()->update(
        (uint8_t*)maxState.get(), unStrValueVector.get(), unStrValueVector->state->selectedSize);
    auto otherMaxState = make_unique<MinMaxFunction<Value>::MinMaxState>();
    otherMaxState->val = Value((int64_t)101);
    maxAggrEvaluator->getFunction()->combine(
        (uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    auto maxFinalState = make_unique<MinMaxFunction<Value>::MinMaxState>();
    maxAggrEvaluator->getFunction()->finalize(
        (uint8_t*)maxState.get(), (uint8_t*)maxFinalState.get());
    ASSERT_EQ(maxFinalState->val.val.int64Val, 101);
}

TEST_F(AggrExpressionEvaluatorTest, INT64MinTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        MIN_FUNC, INT64, AggregateExpressionEvaluator::getAggregationFunction(MIN_FUNC, INT64));
    auto maxAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto maxState = make_unique<MinMaxFunction<int64_t>::MinMaxState>();
    maxAggrEvaluator->getFunction()->initialize((uint8_t*)maxState.get());
    maxAggrEvaluator->getFunction()->update(
        (uint8_t*)maxState.get(), int64ValueVector.get(), int64ValueVector->state->selectedSize);
    auto otherMaxState = make_unique<MinMaxFunction<int64_t>::MinMaxState>();
    otherMaxState->val = -10;
    maxAggrEvaluator->getFunction()->combine(
        (uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    auto maxFinalState = make_unique<MinMaxFunction<int64_t>::MinMaxState>();
    maxAggrEvaluator->getFunction()->finalize(
        (uint8_t*)maxState.get(), (uint8_t*)maxFinalState.get());
    ASSERT_EQ(maxFinalState->val, -10);
}

#include "gtest/gtest.h"

#include "src/expression_evaluator/include/aggregate_expression_evaluator.h"
#include "src/function/include/aggregation/avg.h"
#include "src/function/include/aggregation/count.h"

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
    auto countStarAggrEvaluator =
        reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto countStarState = countStarAggrEvaluator->getFunction()->initialize();
    countStarAggrEvaluator->getFunction()->update(
        (uint8_t*)countStarState.get(), nullptr, resultSet->getNumTuples());
    auto otherCountStarState = countStarAggrEvaluator->getFunction()->initialize();
    *((uint64_t*)otherCountStarState->val.get()) = 10;
    countStarAggrEvaluator->getFunction()->combine(
        (uint8_t*)countStarState.get(), (uint8_t*)otherCountStarState.get());
    countStarAggrEvaluator->getFunction()->finalize((uint8_t*)countStarState.get());
    ASSERT_EQ(*((uint64_t*)countStarState->val.get()), 110);
}

TEST_F(AggrExpressionEvaluatorTest, CountTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        COUNT_FUNC, INT64, AggregateExpressionEvaluator::getAggregationFunction(COUNT_FUNC, INT64));
    auto countAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto countState = countAggrEvaluator->getFunction()->initialize();
    countAggrEvaluator->getFunction()->update(
        (uint8_t*)countState.get(), int64ValueVector.get(), int64ValueVector->state->selectedSize);
    auto otherCountState = countAggrEvaluator->getFunction()->initialize();
    *((uint64_t*)otherCountState->val.get()) = 10;
    countAggrEvaluator->getFunction()->combine(
        (uint8_t*)countState.get(), (uint8_t*)otherCountState.get());
    countAggrEvaluator->getFunction()->finalize((uint8_t*)countState.get());
    ASSERT_EQ(*((uint64_t*)countState->val.get()), 60);
}

TEST_F(AggrExpressionEvaluatorTest, INT64SumTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        SUM_FUNC, INT64, AggregateExpressionEvaluator::getAggregationFunction(SUM_FUNC, INT64));
    auto sumAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto sumState = sumAggrEvaluator->getFunction()->initialize();
    sumAggrEvaluator->getFunction()->update(
        (uint8_t*)sumState.get(), int64ValueVector.get(), int64ValueVector->state->selectedSize);
    auto otherSumState = sumAggrEvaluator->getFunction()->initialize();
    *((uint64_t*)otherSumState->val.get()) = 10;
    otherSumState->isNull = false;
    sumAggrEvaluator->getFunction()->combine(
        (uint8_t*)sumState.get(), (uint8_t*)otherSumState.get());
    sumAggrEvaluator->getFunction()->finalize((uint8_t*)sumState.get());
    auto sumValue = *(uint64_t*)otherSumState->val.get();
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i;
        }
    }
    ASSERT_EQ(*(uint64_t*)sumState->val.get(), sumValue);
}

TEST_F(AggrExpressionEvaluatorTest, DOUBLESumTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        SUM_FUNC, DOUBLE, AggregateExpressionEvaluator::getAggregationFunction(SUM_FUNC, DOUBLE));
    auto sumAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto sumState = sumAggrEvaluator->getFunction()->initialize();
    sumAggrEvaluator->getFunction()->update(
        (uint8_t*)sumState.get(), doubleValueVector.get(), doubleValueVector->state->selectedSize);
    auto otherSumState = sumAggrEvaluator->getFunction()->initialize();
    *((double_t*)otherSumState->val.get()) = 10.0;
    otherSumState->isNull = false;
    sumAggrEvaluator->getFunction()->combine(
        (uint8_t*)sumState.get(), (uint8_t*)otherSumState.get());
    sumAggrEvaluator->getFunction()->finalize((uint8_t*)sumState.get());
    auto sumValue = *((double_t*)otherSumState->val.get());
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i * 1.5;
        }
    }
    ASSERT_EQ(*((double_t*)sumState->val.get()), sumValue);
}

TEST_F(AggrExpressionEvaluatorTest, UNSTRSumTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(SUM_FUNC, UNSTRUCTURED,
        AggregateExpressionEvaluator::getAggregationFunction(SUM_FUNC, UNSTRUCTURED));
    auto sumAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto sumState = sumAggrEvaluator->getFunction()->initialize();
    sumAggrEvaluator->getFunction()->update(
        (uint8_t*)sumState.get(), unStrValueVector.get(), unStrValueVector->state->selectedSize);
    auto otherSumState = sumAggrEvaluator->getFunction()->initialize();
    *((Value*)otherSumState->val.get()) = Value((int64_t)10);
    otherSumState->isNull = false;
    sumAggrEvaluator->getFunction()->combine(
        (uint8_t*)sumState.get(), (uint8_t*)otherSumState.get());
    sumAggrEvaluator->getFunction()->finalize((uint8_t*)sumState.get());
    auto sumValue = ((Value*)otherSumState->val.get())->val.int64Val;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i;
        }
    }
    ASSERT_EQ(((Value*)sumState->val.get())->val.int64Val, sumValue);
}

TEST_F(AggrExpressionEvaluatorTest, INT64AvgTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        AVG_FUNC, DOUBLE, AggregateExpressionEvaluator::getAggregationFunction(AVG_FUNC, INT64));
    auto avgAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto avgState = avgAggrEvaluator->getFunction()->initialize();
    avgAggrEvaluator->getFunction()->update(
        (uint8_t*)avgState.get(), int64ValueVector.get(), int64ValueVector->state->selectedSize);
    auto otherAvgState = avgAggrEvaluator->getFunction()->initialize();
    *((double_t*)otherAvgState->val.get()) = 10;
    ((AvgFunction<int64_t>::AvgState&)otherAvgState).numValues = 1;
    otherAvgState->isNull = false;
    avgAggrEvaluator->getFunction()->combine(
        (uint8_t*)avgState.get(), (uint8_t*)otherAvgState.get());
    avgAggrEvaluator->getFunction()->finalize((uint8_t*)avgState.get());
    auto sumValue = 10;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i;
        }
    }
    ASSERT_EQ(*((double_t*)avgState->val.get()), (double_t)(sumValue) / (double_t)50);
}

TEST_F(AggrExpressionEvaluatorTest, DOUBLEAvgTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        AVG_FUNC, DOUBLE, AggregateExpressionEvaluator::getAggregationFunction(AVG_FUNC, DOUBLE));
    auto avgAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto avgState = avgAggrEvaluator->getFunction()->initialize();
    avgAggrEvaluator->getFunction()->update(
        (uint8_t*)avgState.get(), doubleValueVector.get(), doubleValueVector->state->selectedSize);
    auto otherAvgState = avgAggrEvaluator->getFunction()->initialize();
    *((double_t*)otherAvgState->val.get()) = 10.0;
    ((AvgFunction<double_t>::AvgState&)otherAvgState).numValues = 1;
    otherAvgState->isNull = false;
    avgAggrEvaluator->getFunction()->combine(
        (uint8_t*)avgState.get(), (uint8_t*)otherAvgState.get());
    avgAggrEvaluator->getFunction()->finalize((uint8_t*)avgState.get());
    auto sumValue = 10.0;
    for (auto i = 0u; i < 100; i++) {
        if (i % 2 != 0) {
            sumValue += i * 1.5;
        }
    }
    ASSERT_EQ(*((double_t*)avgState->val.get()), (double_t)(sumValue) / (double_t)50);
}

TEST_F(AggrExpressionEvaluatorTest, INT64MaxTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        MAX_FUNC, INT64, AggregateExpressionEvaluator::getAggregationFunction(MAX_FUNC, INT64));
    auto maxAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto maxState = maxAggrEvaluator->getFunction()->initialize();
    maxAggrEvaluator->getFunction()->update(
        (uint8_t*)maxState.get(), int64ValueVector.get(), int64ValueVector->state->selectedSize);
    auto otherMaxState = maxAggrEvaluator->getFunction()->initialize();
    *((int64_t*)otherMaxState->val.get()) = 101;
    otherMaxState->isNull = false;
    maxAggrEvaluator->getFunction()->combine(
        (uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    maxAggrEvaluator->getFunction()->finalize((uint8_t*)maxState.get());
    ASSERT_EQ(*((int64_t*)maxState->val.get()), 101);
}

TEST_F(AggrExpressionEvaluatorTest, DOUBLEMaxTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        MAX_FUNC, DOUBLE, AggregateExpressionEvaluator::getAggregationFunction(MAX_FUNC, DOUBLE));
    auto maxAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto maxState = maxAggrEvaluator->getFunction()->initialize();
    maxAggrEvaluator->getFunction()->update(
        (uint8_t*)maxState.get(), doubleValueVector.get(), doubleValueVector->state->selectedSize);
    auto otherMaxState = maxAggrEvaluator->getFunction()->initialize();
    *((double_t*)otherMaxState->val.get()) = 101.0;
    otherMaxState->isNull = false;
    maxAggrEvaluator->getFunction()->combine(
        (uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    maxAggrEvaluator->getFunction()->finalize((uint8_t*)maxState.get());
    ASSERT_EQ(*((double_t*)maxState->val.get()), 148.5);
}

TEST_F(AggrExpressionEvaluatorTest, STRINGMaxTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        MAX_FUNC, STRING, AggregateExpressionEvaluator::getAggregationFunction(MAX_FUNC, STRING));
    auto maxAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto maxState = maxAggrEvaluator->getFunction()->initialize();
    maxAggrEvaluator->getFunction()->update(
        (uint8_t*)maxState.get(), stringValueVector.get(), stringValueVector->state->selectedSize);
    auto otherMaxState = maxAggrEvaluator->getFunction()->initialize();
    gf_string_t otherStr;
    otherStr.set("0");
    *((gf_string_t*)otherMaxState->val.get()) = otherStr;
    otherMaxState->isNull = false;
    maxAggrEvaluator->getFunction()->combine(
        (uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    maxAggrEvaluator->getFunction()->finalize((uint8_t*)maxState.get());
    ASSERT_EQ(((gf_string_t*)maxState->val.get())->getAsString(), "99");
}

TEST_F(AggrExpressionEvaluatorTest, UNSTRMaxTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(MAX_FUNC, UNSTRUCTURED,
        AggregateExpressionEvaluator::getAggregationFunction(MAX_FUNC, UNSTRUCTURED));
    auto maxAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto maxState = maxAggrEvaluator->getFunction()->initialize();
    maxAggrEvaluator->getFunction()->update(
        (uint8_t*)maxState.get(), unStrValueVector.get(), unStrValueVector->state->selectedSize);
    auto otherMaxState = maxAggrEvaluator->getFunction()->initialize();
    *((Value*)otherMaxState->val.get()) = Value((int64_t)101);
    otherMaxState->isNull = false;
    maxAggrEvaluator->getFunction()->combine(
        (uint8_t*)maxState.get(), (uint8_t*)otherMaxState.get());
    maxAggrEvaluator->getFunction()->finalize((uint8_t*)maxState.get());
    ASSERT_EQ(((Value*)maxState->val.get())->val.int64Val, 101);
}

TEST_F(AggrExpressionEvaluatorTest, INT64MinTest) {
    auto exprEvaluator = make_unique<AggregateExpressionEvaluator>(
        MIN_FUNC, INT64, AggregateExpressionEvaluator::getAggregationFunction(MIN_FUNC, INT64));
    auto minAggrEvaluator = reinterpret_cast<AggregateExpressionEvaluator*>(exprEvaluator.get());
    auto minState = minAggrEvaluator->getFunction()->initialize();
    minAggrEvaluator->getFunction()->update(
        (uint8_t*)minState.get(), int64ValueVector.get(), int64ValueVector->state->selectedSize);
    auto otherMinState = minAggrEvaluator->getFunction()->initialize();
    *((int64_t*)otherMinState->val.get()) = -10;
    otherMinState->isNull = false;
    minAggrEvaluator->getFunction()->combine(
        (uint8_t*)minState.get(), (uint8_t*)otherMinState.get());
    minAggrEvaluator->getFunction()->finalize((uint8_t*)minState.get());
    ASSERT_EQ(*((int64_t*)minState->val.get()), -10);
}

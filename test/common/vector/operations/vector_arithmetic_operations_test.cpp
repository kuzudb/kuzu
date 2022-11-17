#include "gtest/gtest.h"
#include "test/common/include/vector/operations/vector_operations_test_helper.h"

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/type_utils.h"
#include "src/common/types/include/value.h"
#include "src/function/arithmetic/operations/include/arithmetic_operations.h"
#include "src/function/include/binary_operation_executor.h"
#include "src/function/include/unary_operation_executor.h"

using namespace kuzu::common;
using namespace kuzu::function;
using namespace kuzu::testing;
using namespace std;

// Creates two vectors: vector1: [0, 1, ..., 100] and vector2: [110, 109, ...., 8, 9].
class Int64ArithmeticOperandsInSameDataChunkTest : public OperandsInSameDataChunk, public Test {

public:
    DataTypeID getDataTypeOfOperands() override { return INT64; }
    DataTypeID getDataTypeOfResultVector() override { return INT64; }

    void SetUp() override {
        initDataChunk();
        for (int i = 0; i < NUM_TUPLES; i++) {
            vector1->setValue(i, (int64_t)i);
            vector2->setValue(i, (int64_t)(110 - i));
        }
    }
};

// Creates two vectors: vector1: [0, 1, ..., 100] and vector2: [110, 109, ...., 8, 9].
class Int64ArithmeticOperandsInDifferentDataChunksTest : public OperandsInDifferentDataChunks,
                                                         public Test {

public:
    DataTypeID getDataTypeOfOperands() override { return INT64; }
    DataTypeID getDataTypeOfResultVector() override { return INT64; }

    void SetUp() override {
        initDataChunk();
        for (int i = 0; i < NUM_TUPLES; i++) {
            vector1->setValue(i, (int64_t)i);
            vector2->setValue(i, (int64_t)(110 - i));
        }
    }
};

class UnstructuredArithmeticOperandsInSameDataChunkTest : public OperandsInSameDataChunk,
                                                          public Test {

public:
    DataTypeID getDataTypeOfOperands() override { return UNSTRUCTURED; }
    DataTypeID getDataTypeOfResultVector() override { return UNSTRUCTURED; }

    void SetUp() override { initDataChunk(); }
};

TEST_F(Int64ArithmeticOperandsInSameDataChunkTest, Int64UnaryAndBinaryAllUnflatNoNulls) {
    auto lVector = vector1;
    auto rVector = vector2;

    UnaryOperationExecutor::execute<int64_t, int64_t, operation::Negate>(*lVector, *result);

    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<int64_t>(i), -i);
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());

    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, int64_t, operation::Add,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<int64_t>(i), 110);
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());

    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, int64_t, operation::Subtract,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<int64_t>(i), 2 * i - 110);
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());

    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, int64_t, operation::Multiply,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<int64_t>(i), i * (110 - i));
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());

    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, int64_t, operation::Divide,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (auto i = 0u; i < dataChunk->state->selVector->selectedSize; i++) {
        ASSERT_EQ(result->getValue<int64_t>(i), i / (110 - i));
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());

    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, int64_t, operation::Modulo,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (auto i = 0u; i < dataChunk->state->selVector->selectedSize; i++) {
        ASSERT_EQ(result->getValue<int64_t>(i), i % (110 - i));
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());

    result = make_shared<ValueVector>(DOUBLE, memoryManager.get());
    dataChunk->insert(0, result);
    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, double_t, operation::Power,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<double_t>(i), pow(i, 110 - i));
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());
}

// We use Negate and Addition as an example comparison operator.
TEST_F(Int64ArithmeticOperandsInSameDataChunkTest, Int64UnaryAndBinaryAllUnflatWithNulls) {
    auto lVector = vector1;
    auto rVector = vector2;
    // We set every odd value in vector 2 to NULL.
    for (int i = 0; i < NUM_TUPLES; ++i) {
        rVector->setNull(i, (i % 2) == 1);
    }

    UnaryOperationExecutor::execute<int64_t, int64_t, operation::Negate>(*rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        if (i % 2 == 0) {
            ASSERT_EQ(result->getValue<int64_t>(i), -(110 - i));
            ASSERT_FALSE(result->isNull(i));
        } else {
            ASSERT_TRUE(result->isNull(i));
        }
    }
    ASSERT_FALSE(result->hasNoNullsGuarantee());

    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, int64_t, operation::Add,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        if (i % 2 == 0) {
            ASSERT_EQ(result->getValue<int64_t>(i), 110);
            ASSERT_FALSE(result->isNull(i));
        } else {
            ASSERT_TRUE(result->isNull(i));
        }
    }
    ASSERT_FALSE(result->hasNoNullsGuarantee());
}

// We use Addition as an example comparison operator.
TEST_F(Int64ArithmeticOperandsInDifferentDataChunksTest, Int64BinaryOneFlatOneUnflatNoNulls) {
    // Flatten dataChunkWithVector1, which holds vector1
    dataChunkWithVector1->state->currIdx = 80;

    // Test 1: Left flat and right is unflat.
    // The addition is 80 + [110, 109, ...., 8, 9]. The results are: [190, 189, ...., 88, 89]
    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, int64_t, operation::Add,
        BinaryOperationWrapper>(*vector1, *vector2, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<uint64_t>(i), 190 - i);
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());

    // Test 2: Left unflat and right is flat. The result is the same as above.
    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, int64_t, operation::Add,
        BinaryOperationWrapper>(*vector2, *vector1, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<uint64_t>(i), 190 - i);
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());
}

// We use Addition as an example comparison operator.
TEST_F(Int64ArithmeticOperandsInDifferentDataChunksTest, Int64BinaryOneFlatOneUnflatWithNulls) {
    auto lVector = vector1;
    auto rVector = vector2;
    // We set every odd value in vector 2 to NULL.
    for (int i = 0; i < NUM_TUPLES; ++i) {
        rVector->setNull(i, (i % 2) == 1);
    }
    // Flatten dataChunkWithVector1, which holds vector1
    dataChunkWithVector1->state->currIdx = 80;

    // Test 1: Left flat and right is unflat.
    // The addition is 80 + [110, 109, ...., 8, 9]. The results are: [190, NULL, ...., 88, NULL]
    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, int64_t, operation::Add,
        BinaryOperationWrapper>(*vector1, *vector2, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        if (i % 2 == 0) {
            ASSERT_EQ(result->getValue<int64_t>(i), 190 - i);
            ASSERT_FALSE(result->isNull(i));
        } else {
            ASSERT_TRUE(result->isNull(i));
        }
    }
    ASSERT_FALSE(result->hasNoNullsGuarantee());

    // Test 2: Left unflat and right is flat. The result is the same as above.
    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, int64_t, operation::Add,
        BinaryOperationWrapper>(*vector2, *vector1, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        if (i % 2 == 0) {
            ASSERT_EQ(result->getValue<int64_t>(i), 190 - i);
            ASSERT_FALSE(result->isNull(i));
        } else {
            ASSERT_TRUE(result->isNull(i));
        }
    }
    ASSERT_FALSE(result->hasNoNullsGuarantee());
}

TEST_F(UnstructuredArithmeticOperandsInSameDataChunkTest, UnstructuredInt64Test) {
    auto lVector = vector1;
    auto rVector = vector2;

    // Fill values before the comparison.
    for (int i = 0; i < NUM_TUPLES; i++) {
        lVector->setValue(i, Value((int64_t)i));
        rVector->setValue(i, Value((int64_t)110 - i));
    }

    UnaryOperationExecutor::execute<Value, Value, operation::Negate>(*lVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<Value>(i).val.int64Val, -i);
    }

    BinaryOperationExecutor::executeSwitch<Value, Value, Value, operation::Add,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<Value>(i).val.int64Val, 110);
    }

    BinaryOperationExecutor::executeSwitch<Value, Value, Value, operation::Subtract,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<Value>(i).val.int64Val, 2 * i - 110);
    }

    BinaryOperationExecutor::executeSwitch<Value, Value, Value, operation::Multiply,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<Value>(i).val.int64Val, i * (110 - i));
    }

    BinaryOperationExecutor::executeSwitch<Value, Value, Value, operation::Divide,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<Value>(i).val.int64Val, i / (110 - i));
    }

    BinaryOperationExecutor::executeSwitch<Value, Value, Value, operation::Modulo,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<Value>(i).val.int64Val, i % (110 - i));
    }
}

TEST_F(UnstructuredArithmeticOperandsInSameDataChunkTest, UnstructuredInt32AndDoubleTest) {
    auto lVector = vector1;
    auto rVector = vector2;

    // Fill values before the comparison.
    for (int i = 0; i < NUM_TUPLES; i++) {
        lVector->setValue(i, Value((double)i));
        rVector->setValue(i, Value((int64_t)(110 - i)));
    }

    UnaryOperationExecutor::execute<Value, Value, operation::Negate>(*lVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<Value>(i).val.doubleVal, (double)-i);
    }

    BinaryOperationExecutor::executeSwitch<Value, Value, Value, operation::Add,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<Value>(i).val.doubleVal, (double)110);
    }

    BinaryOperationExecutor::executeSwitch<Value, Value, Value, operation::Subtract,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<Value>(i).val.doubleVal, (double)(2 * i - 110));
    }

    BinaryOperationExecutor::executeSwitch<Value, Value, Value, operation::Multiply,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<Value>(i).val.doubleVal, (double)(i * (110 - i)));
    }

    BinaryOperationExecutor::executeSwitch<Value, Value, Value, operation::Divide,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<Value>(i).val.doubleVal, (double)i / (110 - i));
    }
}

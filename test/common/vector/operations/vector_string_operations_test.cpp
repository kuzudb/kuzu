#include "function/binary_operation_executor.h"
#include "function/string/operations/concat_operation.h"
#include "gtest/gtest.h"
#include "vector/operations/vector_operations_test_helper.h"

using namespace kuzu::function;
using namespace kuzu::testing;

class StringArithmeticOperandsInSameDataChunkTest : public OperandsInSameDataChunk, public Test {

public:
    DataTypeID getDataTypeOfOperands() override { return STRING; }
    DataTypeID getDataTypeOfResultVector() override { return STRING; }

    void SetUp() override { initDataChunk(); }
};

TEST_F(StringArithmeticOperandsInSameDataChunkTest, StringTest) {
    auto lVector = vector1;
    auto rVector = vector2;
    // Fill values before the comparison.
    for (int i = 0; i < NUM_TUPLES; i++) {
        lVector->setValue(i, to_string(i));
        rVector->setValue(i, to_string(110 - i));
    }

    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, ku_string_t, operation::Concat,
        BinaryStringAndListOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(
            result->getValue<ku_string_t>(i).getAsString(), to_string(i) + to_string(110 - i));
    }
}

TEST_F(StringArithmeticOperandsInSameDataChunkTest, BigStringTest) {
    auto lVector = vector1;
    auto rVector = vector2;
    // Fill values before the comparison.
    for (int i = 0; i < NUM_TUPLES; i++) {
        lVector->setValue(i, to_string(i) + "abcdefabcdefqwert");
        rVector->setValue(i, to_string(110 - i) + "abcdefabcdefqwert");
    }

    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, ku_string_t, operation::Concat,
        BinaryStringAndListOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<ku_string_t>(i).getAsString(),
            to_string(i) + "abcdefabcdefqwert" + to_string(110 - i) + "abcdefabcdefqwert");
    }
}

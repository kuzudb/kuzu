#include "gtest/gtest.h"

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/vector/operations/vector_arithmetic_operations.h"

using namespace graphflow::common;
using namespace std;

TEST(VectorArithTests, test) {
    auto dataChunk = make_shared<DataChunk>();
    dataChunk->state->size = 100;
    dataChunk->state->size = 100;

    auto lVector = make_shared<ValueVector>(INT32);
    dataChunk->append(lVector);
    auto lData = (int32_t*)lVector->values;

    auto rVector = make_shared<ValueVector>(INT32);
    dataChunk->append(rVector);
    auto rData = (int32_t*)rVector->values;

    auto result = make_shared<ValueVector>(INT32);
    dataChunk->append(result);
    auto resultData = (int32_t*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < 100; i++) {
        lData[i] = i;
        rData[i] = 110 - i;
    }

    VectorArithmeticOperations::Negate(*lVector, *result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], -i);
    }

    VectorArithmeticOperations::Add(*lVector, *rVector, *result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], 110);
    }

    VectorArithmeticOperations::Subtract(*lVector, *rVector, *result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], 2 * i - 110);
    }

    VectorArithmeticOperations::Multiply(*lVector, *rVector, *result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i * (110 - i));
    }

    VectorArithmeticOperations::Divide(*lVector, *rVector, *result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i / (110 - i));
    }

    /* note rVector and lVector are flipped */
    VectorArithmeticOperations::Modulo(*rVector, *lVector, *result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i % (110 - i));
    }

    result = make_shared<ValueVector>(DOUBLE);
    dataChunk->append(result);
    auto resultDataAsDoubleArr = (double_t*)result->values;
    VectorArithmeticOperations::Power(*lVector, *rVector, *result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultDataAsDoubleArr[i], pow(i, 110 - i));
    }
}

#include "gtest/gtest.h"

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/vector/operations/vector_arithmetic_operations.h"
#include "src/common/include/vector/value_vector.h"

using namespace graphflow::common;
using namespace std;

TEST(VectorArithTests, test) {
    auto dataChunk = make_shared<DataChunk>();
    dataChunk->state->size = 100;
    dataChunk->state->numSelectedValues = 100;

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

    auto negateOp = ValueVector::getUnaryOperation(ExpressionType::NEGATE);
    negateOp(*lVector, *result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], -i);
    }

    auto addOp = ValueVector::getBinaryOperation(ExpressionType::ADD);
    addOp(*lVector, *rVector, *result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], 110);
    }

    auto subtractOp = ValueVector::getBinaryOperation(ExpressionType::SUBTRACT);
    subtractOp(*lVector, *rVector, *result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], 2 * i - 110);
    }

    auto multiplyOp = ValueVector::getBinaryOperation(ExpressionType::MULTIPLY);
    multiplyOp(*lVector, *rVector, *result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i * (110 - i));
    }

    auto divideOp = ValueVector::getBinaryOperation(ExpressionType::DIVIDE);
    divideOp(*lVector, *rVector, *result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i / (110 - i));
    }

    /* note rVector and lVector are flipped */
    auto moduloOp = ValueVector::getBinaryOperation(ExpressionType::MODULO);
    moduloOp(*rVector, *lVector, *result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i % (110 - i));
    }

    result = make_shared<ValueVector>(DOUBLE);
    dataChunk->append(result);
    auto resultDataAsDoubleArr = (double_t*)result->values;
    auto powerOp = ValueVector::getBinaryOperation(ExpressionType::POWER);
    powerOp(*lVector, *rVector, *result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultDataAsDoubleArr[i], pow(i, 110 - i));
    }
}

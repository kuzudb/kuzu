#include "gtest/gtest.h"

#include "src/common/include/vector/operations/vector_arithmetic_operations.h"
#include "src/common/include/vector/value_vector.h"

using namespace graphflow::common;
using namespace std;

TEST(VectorArithTests, test) {
    auto dataChunk = make_shared<DataChunk>();
    dataChunk->size = 100;
    dataChunk->numSelectedValues = 100;

    auto lVector = ValueVector(INT32);
    lVector.setDataChunkOwner(dataChunk);
    auto lData = (int32_t*)lVector.getValues();

    auto rVector = ValueVector(INT32);
    rVector.setDataChunkOwner(dataChunk);
    auto rData = (int32_t*)rVector.getValues();

    auto result = ValueVector(INT32);
    result.setDataChunkOwner(dataChunk);
    auto resultData = (int32_t*)result.getValues();

    // Fill values before the comparison.
    for (int32_t i = 0; i < 100; i++) {
        lData[i] = i;
        rData[i] = 110 - i;
    }

    auto negateOp = ValueVector::getUnaryOperation(ExpressionType::NEGATE);
    negateOp(lVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], -i);
    }

    auto addOp = ValueVector::getBinaryOperation(ExpressionType::ADD);
    addOp(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], 110);
    }

    auto subtractOp = ValueVector::getBinaryOperation(ExpressionType::SUBTRACT);
    subtractOp(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], 2 * i - 110);
    }

    auto multiplyOp = ValueVector::getBinaryOperation(ExpressionType::MULTIPLY);
    multiplyOp(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i * (110 - i));
    }

    auto divideOp = ValueVector::getBinaryOperation(ExpressionType::DIVIDE);
    divideOp(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i / (110 - i));
    }

    /* note rVector and lVector are flipped */
    auto moduloOp = ValueVector::getBinaryOperation(ExpressionType::MODULO);
    moduloOp(rVector, lVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i % (110 - i));
    }

    result = ValueVector(DOUBLE);
    result.setDataChunkOwner(dataChunk);
    auto resultDataAsDoubleArr = (double_t*)result.getValues();
    auto powerOp = ValueVector::getBinaryOperation(ExpressionType::POWER);
    powerOp(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultDataAsDoubleArr[i], pow(i, 110 - i));
    }
}

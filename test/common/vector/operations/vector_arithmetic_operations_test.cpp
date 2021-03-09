#include "gtest/gtest.h"

#include "src/common/include/vector/operations/vector_operations.h"

using namespace graphflow::common;
using namespace std;

TEST(VectorArithTests, test) {
    auto dataChunk = make_shared<DataChunk>();
    dataChunk->size = 100;

    auto lVector = ValueVector(INT);
    lVector.setDataChunkOwner(dataChunk);
    auto lData = (int32_t*)lVector.getValues();

    auto rVector = ValueVector(INT);
    rVector.setDataChunkOwner(dataChunk);
    auto rData = (int32_t*)rVector.getValues();

    auto result = ValueVector(INT);
    auto resultData = (int32_t*)result.getValues();

    // Fill values before the comparison.
    for (int32_t i = 0; i < 100; i++) {
        lData[i] = i;
        rData[i] = 110 - i;
    }

    auto negateOp = VectorOperations::getOperation(UNARY_OPERATION::NEGATE);
    negateOp(lVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], -i);
    }

    auto addOp = VectorOperations::getOperation(BINARY_OPERATION::ADD);
    addOp(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], 110);
    }

    auto subtractOp = VectorOperations::getOperation(BINARY_OPERATION::SUBTRACT);
    subtractOp(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], 2 * i - 110);
    }

    auto multiplyOp = VectorOperations::getOperation(BINARY_OPERATION::MULTIPLY);
    multiplyOp(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i * (110 - i));
    }

    auto divideOp = VectorOperations::getOperation(BINARY_OPERATION::DIVIDE);
    divideOp(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i / (110 - i));
    }

    /* note rVector and lVector are flipped */
    auto moduloOp = VectorOperations::getOperation(BINARY_OPERATION::MODULO);
    moduloOp(rVector, lVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i % (110 - i));
    }

    result = ValueVector(DOUBLE);
    auto resultDataAsDoubleArr = (double_t*)result.getValues();
    auto powerOp = VectorOperations::getOperation(BINARY_OPERATION::POWER);
    powerOp(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultDataAsDoubleArr[i], pow(i, 110 - i));
    }
}

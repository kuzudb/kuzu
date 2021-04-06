#include "gtest/gtest.h"

#include "src/common/include/vector/value_vector.h"

using namespace graphflow::common;
using namespace std;

TEST(VectorBoolTests, test) {
    auto VECTOR_SIZE = 4;
    auto dataChunk = make_shared<DataChunk>();
    dataChunk->size = VECTOR_SIZE;
    dataChunk->numSelectedValues = VECTOR_SIZE;

    auto lVector = ValueVector(BOOL);
    lVector.setDataChunkOwner(dataChunk);
    auto lData = (uint8_t*)lVector.getValues();

    auto rVector = ValueVector(BOOL);
    rVector.setDataChunkOwner(dataChunk);
    auto rData = (uint8_t*)rVector.getValues();

    auto result = ValueVector(BOOL);
    result.setDataChunkOwner(dataChunk);
    auto resultData = (uint8_t*)result.getValues();

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        if (i < 2)
            lData[i] = FALSE;
        else if (i < 4)
            lData[i] = TRUE;

        if (i % 2 == 0)
            rData[i] = FALSE;
        else if (i % 2 == 1)
            rData[i] = TRUE;
    }

    uint8_t andExpectedResult[] = {FALSE, FALSE, FALSE, TRUE};
    auto andOp = ValueVector::getBinaryOperation(ExpressionType::AND);
    andOp(lVector, rVector, result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i], andExpectedResult[i]);
    }

    uint8_t orExpectedResult[] = {FALSE, TRUE, TRUE, TRUE};
    auto orOp = ValueVector::getBinaryOperation(ExpressionType::OR);
    orOp(lVector, rVector, result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i], orExpectedResult[i]);
    }

    uint8_t xorExpectedResult[] = {FALSE, TRUE, TRUE, FALSE};
    auto xorOp = ValueVector::getBinaryOperation(ExpressionType::XOR);
    xorOp(lVector, rVector, result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i], xorExpectedResult[i]);
    }

    uint8_t notExpectedResult[] = {TRUE, TRUE, FALSE, FALSE};
    auto notOp = ValueVector::getUnaryOperation(ExpressionType::NOT);
    notOp(lVector, result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i], notExpectedResult[i]);
    }
}

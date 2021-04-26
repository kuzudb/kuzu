#include "gtest/gtest.h"

#include "src/common/include/data_chunk/data_chunk.h"

using namespace graphflow::common;
using namespace std;

TEST(VectorBoolTests, test) {
    auto VECTOR_SIZE = 4;
    auto dataChunk = make_shared<DataChunk>();
    dataChunk->state->size = VECTOR_SIZE;
    dataChunk->state->numSelectedValues = VECTOR_SIZE;

    auto lVector = make_shared<ValueVector>(BOOL);
    dataChunk->append(lVector);
    auto lData = (uint8_t*)lVector->values;

    auto rVector = make_shared<ValueVector>(BOOL);
    dataChunk->append(rVector);
    auto rData = (uint8_t*)rVector->values;

    auto result = make_shared<ValueVector>(BOOL);
    dataChunk->append(result);
    auto resultData = (uint8_t*)result->values;

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
    andOp(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i], andExpectedResult[i]);
    }

    uint8_t orExpectedResult[] = {FALSE, TRUE, TRUE, TRUE};
    auto orOp = ValueVector::getBinaryOperation(ExpressionType::OR);
    orOp(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i], orExpectedResult[i]);
    }

    uint8_t xorExpectedResult[] = {FALSE, TRUE, TRUE, FALSE};
    auto xorOp = ValueVector::getBinaryOperation(ExpressionType::XOR);
    xorOp(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i], xorExpectedResult[i]);
    }

    uint8_t notExpectedResult[] = {TRUE, TRUE, FALSE, FALSE};
    auto notOp = ValueVector::getUnaryOperation(ExpressionType::NOT);
    notOp(*lVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i], notExpectedResult[i]);
    }
}

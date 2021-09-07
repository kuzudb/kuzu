#include "gtest/gtest.h"

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/vector/operations/vector_boolean_operations.h"

using namespace graphflow::common;
using namespace std;

TEST(VectorBooleanTests, test) {
    auto VECTOR_SIZE = 4;
    auto dataChunk = make_shared<DataChunk>(3);
    dataChunk->state->selectedSize = VECTOR_SIZE;
    auto memoryManager = make_unique<MemoryManager>();

    auto lVector = make_shared<ValueVector>(memoryManager.get(), BOOL);
    dataChunk->insert(0, lVector);
    auto lData = (uint8_t*)lVector->values;

    auto rVector = make_shared<ValueVector>(memoryManager.get(), BOOL);
    dataChunk->insert(1, rVector);
    auto rData = (uint8_t*)rVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), BOOL);
    dataChunk->insert(2, result);
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
    VectorBooleanOperations::And(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i], andExpectedResult[i]);
    }

    uint8_t orExpectedResult[] = {FALSE, TRUE, TRUE, TRUE};
    VectorBooleanOperations::Or(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i], orExpectedResult[i]);
    }

    uint8_t xorExpectedResult[] = {FALSE, TRUE, TRUE, FALSE};
    VectorBooleanOperations::Xor(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i], xorExpectedResult[i]);
    }

    uint8_t notExpectedResult[] = {TRUE, TRUE, FALSE, FALSE};
    VectorBooleanOperations::Not(*lVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i], notExpectedResult[i]);
    }
}

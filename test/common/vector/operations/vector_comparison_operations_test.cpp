#include "gtest/gtest.h"
#include "test/common/include/vector/operations/vector_operations_test_helper.h"

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/vector/operations/vector_comparison_operations.h"
#include "src/common/types/include/gf_string.h"

using namespace graphflow::common;
using namespace graphflow::testing;
using namespace std;

// Creates two vectors: vector1: [0, 1, ..., 100] and vector2: [90, 89, ...., -8, -9].
class Int64ComparisonOperandsInSameDataChunkTest : public OperandsInSameDataChunk, public Test {

public:
    DataType getDataTypeOfOperands() override { return INT64; }
    DataType getDataTypeOfResultVector() override { return BOOL; }

    void SetUp() override {
        initDataChunk();
        auto vector1Data = (int64_t*)vector1->values;
        auto vector2Data = (int64_t*)vector2->values;

        for (int i = 0; i < NUM_TUPLES; i++) {
            vector1Data[i] = i;
            vector2Data[i] = 90 - i;
        }
    }
};

// Creates two vectors: vector1: [0, 1, ..., 100] and vector2: [90, 89, ...., -8, -9].
class Int64ComparisonOperandsInDifferentDataChunksTest : public OperandsInDifferentDataChunks,
                                                         public Test {

public:
    DataType getDataTypeOfOperands() override { return INT64; }
    DataType getDataTypeOfResultVector() override { return BOOL; }

    void SetUp() override {
        initDataChunk();
        auto vector1Data = (int64_t*)vector1->values;
        auto vector2Data = (int64_t*)vector2->values;

        for (int i = 0; i < NUM_TUPLES; i++) {
            vector1Data[i] = i;
            vector2Data[i] = 90 - i;
        }
    }
};

TEST_F(Int64ComparisonOperandsInSameDataChunkTest, Int64TwoUnflatNoNulls) {
    auto lVector = vector1;
    auto rVector = vector2;
    auto resultData = result->values;

    VectorComparisonOperations::Equals(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(resultData[i], i == 45 ? true : false);
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());

    VectorComparisonOperations::NotEquals(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(resultData[i], i == 45 ? false : true);
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());

    VectorComparisonOperations::LessThan(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(resultData[i], i < 45 ? true : false);
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());

    VectorComparisonOperations::LessThanEquals(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(resultData[i], i < 46 ? true : false);
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());

    VectorComparisonOperations::GreaterThan(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(resultData[i], i < 46 ? false : true);
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());

    VectorComparisonOperations::GreaterThanEquals(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(resultData[i], i < 45 ? false : true);
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());
}

// We use LessThan as an example comparison operator.
TEST_F(Int64ComparisonOperandsInSameDataChunkTest, Int64TwoUnflatWithNulls) {
    auto lVector = vector1;
    auto rVector = vector2;
    auto resultData = result->values;
    // We set every odd value in vector 2 to NULL.
    for (int i = 0; i < NUM_TUPLES; ++i) {
        vector2->setNull(i, (i % 2) == 1);
    }

    VectorComparisonOperations::LessThan(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        if (i % 2 == 0) {
            ASSERT_EQ(resultData[i], i < 45 ? true : false);
            ASSERT_FALSE(result->isNull(i));
        } else {
            ASSERT_TRUE(result->isNull(i));
        }
    }
    ASSERT_FALSE(result->hasNoNullsGuarantee());
}

// We use LessThan as an example comparison operator.
TEST_F(Int64ComparisonOperandsInDifferentDataChunksTest, Int64OneFlatOneUnflatNoNulls) {
    // Flatten dataChunkWithVector1, which holds vector1
    dataChunkWithVector1->state->currIdx = 80;
    // Recall vector2 and result are in the same data chunk
    auto resultData = result->values;

    // Test 1: Left flat and right is unflat.
    // The comparison ins 80 < [90, 89, ...., -9]. The first 10 (90, ..., 81) the result is true,
    // the rest should be false.
    VectorComparisonOperations::LessThan(*vector1, *vector2, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(resultData[i], i < 10 ? true : false);
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());

    // Test 2: Left unflat and right is flat.
    // Now the comparison is [90, 89, ...., -9] < 80. So now the first 11 (90, ..., 81) the result
    // is false and the rest is true.
    VectorComparisonOperations::LessThan(*vector2, *vector1, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(resultData[i], i < 11 ? false : true);
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());
}

// We use LessThan as an example comparison operator.
TEST_F(Int64ComparisonOperandsInDifferentDataChunksTest, Int64OneFlatOneUnflatWithNulls) {
    // Flatten dataChunkWithVector1, which holds vector1
    dataChunkWithVector1->state->currIdx = 80;
    // We set every odd value in vector 2 to NULL.
    for (int i = 0; i < NUM_TUPLES; ++i) {
        vector2->setNull(i, (i % 2) == 1);
    }
    // Recall vector2 and result are in the same data chunk
    auto resultData = result->values;

    // Test 1: Left flat and right is unflat.
    // The comparison ins 80 < [90, NULL, 88, NULL ...., -8, NULL]. The first 10 (90, ..., 81) the
    // result is true for even and NULL for odd indices. For the rest, the result is false for even
    // and NULL for odd indices.
    VectorComparisonOperations::LessThan(*vector1, *vector2, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        if ((i % 2) == 0) {
            ASSERT_EQ(resultData[i], i < 10 ? true : false);
            ASSERT_FALSE(result->isNull(i));
        } else {
            ASSERT_TRUE(result->isNull(i));
        }
    }
    ASSERT_FALSE(result->hasNoNullsGuarantee());

    // We first need to reset the result value's nullmask to be all non-NULL.
    for (int i = 0; i < NUM_TUPLES; ++i) {
        result->setNull(i, false);
    }
    // Test 2: Left unflat and right is flat.
    // Now the comparison is [90, 89, ...., -9] < 80. So now for the first 11 (90, ..., 81) the
    // result is false for even and NULL for odd indices. For the rest, the result is true for even
    // and NULL for odd indices.
    VectorComparisonOperations::LessThan(*vector2, *vector1, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        if ((i % 2) == 0) {
            ASSERT_EQ(resultData[i], i < 11 ? false : true);
            ASSERT_FALSE(result->isNull(i));
        } else {
            ASSERT_TRUE(result->isNull(i));
        }
    }
    ASSERT_FALSE(result->hasNoNullsGuarantee());
}

TEST(VectorCmpTests, cmpTwoShortStrings) {
    auto numTuples = 1;

    auto dataChunk = make_shared<DataChunk>(3);
    dataChunk->state->selectedSize = numTuples;
    dataChunk->state->currIdx = 0;
    auto bufferManager = make_unique<BufferManager>();
    auto memoryManager = make_unique<MemoryManager>(bufferManager.get());

    auto lVector = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->insert(0, lVector);
    auto lData = ((gf_string_t*)lVector->values);

    auto rVector = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->insert(1, rVector);
    auto rData = ((gf_string_t*)rVector->values);

    auto result = make_shared<ValueVector>(memoryManager.get(), BOOL);
    dataChunk->insert(2, result);
    auto resultData = result->values;

    string value = "abcdefgh";
    lData[0].len = 8;
    rData[0].len = 8;
    memcpy(lData[0].prefix, value.data(), gf_string_t::PREFIX_LENGTH);
    memcpy(rData[0].prefix, value.data(), gf_string_t::PREFIX_LENGTH);
    memcpy(
        lData[0].data, value.data() + gf_string_t::PREFIX_LENGTH, 8 - gf_string_t::PREFIX_LENGTH);
    memcpy(
        rData[0].data, value.data() + gf_string_t::PREFIX_LENGTH, 8 - gf_string_t::PREFIX_LENGTH);

    VectorComparisonOperations::Equals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], true);

    rData[0].data[3] = 'i';
    VectorComparisonOperations::Equals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], false);

    VectorComparisonOperations::NotEquals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], true);

    VectorComparisonOperations::LessThan(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], true);

    VectorComparisonOperations::LessThanEquals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], true);

    VectorComparisonOperations::GreaterThan(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], false);

    VectorComparisonOperations::GreaterThanEquals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], false);

    rData[0].data[3] = 'a';
    VectorComparisonOperations::LessThan(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], false);

    VectorComparisonOperations::LessThanEquals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], false);

    VectorComparisonOperations::GreaterThan(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], true);

    VectorComparisonOperations::GreaterThanEquals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], true);
}

TEST(VectorCmpTests, cmpTwoLongStrings) {
    auto VECTOR_SIZE = 1;

    auto dataChunk = make_shared<DataChunk>(3);
    dataChunk->state->selectedSize = VECTOR_SIZE;
    dataChunk->state->currIdx = 0;
    auto bufferManager = make_unique<BufferManager>();
    auto memoryManager = make_unique<MemoryManager>(bufferManager.get());

    auto lVector = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->insert(0, lVector);
    auto lData = ((gf_string_t*)lVector->values);

    auto rVector = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->insert(1, rVector);
    auto rData = ((gf_string_t*)rVector->values);

    auto result = make_shared<ValueVector>(memoryManager.get(), BOOL);
    dataChunk->insert(2, result);
    auto resultData = result->values;

    string value = "abcdefghijklmnopqrstuvwxy"; // 25.
    lData[0].len = 25;
    rData[0].len = 25;
    memcpy(lData[0].prefix, value.data(), gf_string_t::PREFIX_LENGTH);
    memcpy(rData[0].prefix, value.data(), gf_string_t::PREFIX_LENGTH);
    auto overflowLen = 25;
    char lOverflow[overflowLen];
    memcpy(lOverflow, value.data(), overflowLen);
    lData->overflowPtr = reinterpret_cast<uintptr_t>(lOverflow);
    char rOverflow[overflowLen];
    memcpy(rOverflow, value.data(), overflowLen);
    rData->overflowPtr = reinterpret_cast<uintptr_t>(rOverflow);

    VectorComparisonOperations::Equals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], true);

    rOverflow[overflowLen - 1] = 'z';
    VectorComparisonOperations::Equals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], false);

    VectorComparisonOperations::NotEquals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], true);

    VectorComparisonOperations::LessThan(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], true);

    VectorComparisonOperations::LessThanEquals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], true);

    VectorComparisonOperations::GreaterThan(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], false);

    VectorComparisonOperations::GreaterThanEquals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], false);

    rOverflow[overflowLen - 1] = 'a';
    VectorComparisonOperations::LessThan(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], false);

    VectorComparisonOperations::LessThanEquals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], false);

    VectorComparisonOperations::GreaterThan(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], true);

    VectorComparisonOperations::GreaterThanEquals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], true);
}

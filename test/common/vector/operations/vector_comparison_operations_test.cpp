#include "function/comparison/vector_comparison_operations.h"
#include "gtest/gtest.h"
#include "vector/operations/vector_operations_test_helper.h"

using namespace kuzu::function;
using namespace kuzu::common;
using namespace kuzu::testing;
using namespace std;

// Creates two vectors: vector1: [0, 1, ..., 100] and vector2: [90, 89, ...., -8, -9].
class Int64ComparisonOperandsInSameDataChunkTest : public OperandsInSameDataChunk, public Test {

public:
    DataTypeID getDataTypeOfOperands() override { return INT64; }
    DataTypeID getDataTypeOfResultVector() override { return BOOL; }

    void SetUp() override {
        initDataChunk();

        for (int i = 0; i < NUM_TUPLES; i++) {
            vector1->setValue(i, (int64_t)i);
            vector2->setValue(i, (int64_t)(90 - i));
        }
    }
};

// Creates two vectors: vector1: [0, 1, ..., 100] and vector2: [90, 89, ...., -8, -9].
class Int64ComparisonOperandsInDifferentDataChunksTest : public OperandsInDifferentDataChunks,
                                                         public Test {

public:
    DataTypeID getDataTypeOfOperands() override { return INT64; }
    DataTypeID getDataTypeOfResultVector() override { return BOOL; }

    void SetUp() override {
        initDataChunk();

        for (int i = 0; i < NUM_TUPLES; i++) {
            vector1->setValue(i, (int64_t)i);
            vector2->setValue(i, (int64_t)(90 - i));
        }
    }
};

TEST_F(Int64ComparisonOperandsInSameDataChunkTest, Int64TwoUnflatNoNulls) {
    auto lVector = vector1;
    auto rVector = vector2;

    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, uint8_t, operation::Equals,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<bool>(i), i == 45 ? true : false);
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());

    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, uint8_t, operation::NotEquals,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<bool>(i), i == 45 ? false : true);
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());

    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, uint8_t, operation::LessThan,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<bool>(i), i < 45 ? true : false);
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());

    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, uint8_t, operation::LessThanEquals,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<bool>(i), i < 46 ? true : false);
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());

    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, uint8_t, operation::GreaterThan,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<bool>(i), i < 46 ? false : true);
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());

    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, uint8_t, operation::GreaterThanEquals,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<bool>(i), i < 45 ? false : true);
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());
}

// We use LessThan as an example comparison operator.
TEST_F(Int64ComparisonOperandsInSameDataChunkTest, Int64TwoUnflatWithNulls) {
    auto lVector = vector1;
    auto rVector = vector2;
    // We set every odd value in vector 2 to NULL.
    for (int i = 0; i < NUM_TUPLES; ++i) {
        vector2->setNull(i, (i % 2) == 1);
    }

    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, uint8_t, operation::LessThan,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        if (i % 2 == 0) {
            ASSERT_EQ(result->getValue<bool>(i), i < 45 ? true : false);
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

    // Test 1: Left flat and right is unflat.
    // The comparison ins 80 < [90, 89, ...., -9]. The first 10 (90, ..., 81) the result is true,
    // the rest should be false.
    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, uint8_t, operation::LessThan,
        BinaryOperationWrapper>(*vector1, *vector2, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<bool>(i), i < 10 ? true : false);
        ASSERT_FALSE(result->isNull(i));
    }
    ASSERT_TRUE(result->hasNoNullsGuarantee());

    // Test 2: Left unflat and right is flat.
    // Now the comparison is [90, 89, ...., -9] < 80. So now the first 11 (90, ..., 81) the result
    // is false and the rest is true.
    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, uint8_t, operation::LessThan,
        BinaryOperationWrapper>(*vector2, *vector1, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        ASSERT_EQ(result->getValue<bool>(i), i < 11 ? false : true);
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

    // Test 1: Left flat and right is unflat.
    // The comparison ins 80 < [90, NULL, 88, NULL ...., -8, NULL]. The first 10 (90, ..., 81) the
    // result is true for even and NULL for odd indices. For the rest, the result is false for even
    // and NULL for odd indices.
    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, uint8_t, operation::LessThan,
        BinaryOperationWrapper>(*vector1, *vector2, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        if ((i % 2) == 0) {
            ASSERT_EQ(result->getValue<bool>(i), i < 10 ? true : false);
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
    BinaryOperationExecutor::executeSwitch<int64_t, int64_t, uint8_t, operation::LessThan,
        BinaryOperationWrapper>(*vector2, *vector1, *result);
    for (int i = 0; i < NUM_TUPLES; i++) {
        if ((i % 2) == 0) {
            ASSERT_EQ(result->getValue<bool>(i), i < 11 ? false : true);
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
    dataChunk->state->selVector->selectedSize = numTuples;
    dataChunk->state->currIdx = 0;
    auto bufferManager =
        make_unique<BufferManager>(StorageConfig::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING);
    auto memoryManager = make_unique<MemoryManager>(bufferManager.get());

    auto lVector = make_shared<ValueVector>(STRING, memoryManager.get());
    dataChunk->insert(0, lVector);
    auto lData = ((ku_string_t*)lVector->getData());

    auto rVector = make_shared<ValueVector>(STRING, memoryManager.get());
    dataChunk->insert(1, rVector);
    auto rData = ((ku_string_t*)rVector->getData());

    auto result = make_shared<ValueVector>(BOOL, memoryManager.get());
    dataChunk->insert(2, result);

    string value = "abcdefgh";
    lData[0].len = 8;
    rData[0].len = 8;
    memcpy(lData[0].prefix, value.data(), ku_string_t::PREFIX_LENGTH);
    memcpy(rData[0].prefix, value.data(), ku_string_t::PREFIX_LENGTH);
    memcpy(
        lData[0].data, value.data() + ku_string_t::PREFIX_LENGTH, 8 - ku_string_t::PREFIX_LENGTH);
    memcpy(
        rData[0].data, value.data() + ku_string_t::PREFIX_LENGTH, 8 - ku_string_t::PREFIX_LENGTH);

    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t, operation::Equals,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), true);

    rData[0].data[3] = 'i';
    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t, operation::Equals,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), false);

    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t, operation::NotEquals,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), true);

    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t, operation::LessThan,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), true);

    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t,
        operation::LessThanEquals, BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), true);

    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t,
        operation::GreaterThan, BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), false);

    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t,
        operation::GreaterThanEquals, BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), false);

    rData[0].data[3] = 'a';
    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t, operation::LessThan,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), false);

    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t,
        operation::LessThanEquals, BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), false);

    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t,
        operation::GreaterThan, BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), true);

    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t,
        operation::GreaterThanEquals, BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), true);
}

TEST(VectorCmpTests, cmpTwoLongStrings) {
    auto VECTOR_SIZE = 1;

    auto dataChunk = make_shared<DataChunk>(3);
    dataChunk->state->selVector->selectedSize = VECTOR_SIZE;
    dataChunk->state->currIdx = 0;
    auto bufferManager =
        make_unique<BufferManager>(StorageConfig::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING);
    auto memoryManager = make_unique<MemoryManager>(bufferManager.get());

    auto lVector = make_shared<ValueVector>(STRING, memoryManager.get());
    dataChunk->insert(0, lVector);
    auto lData = ((ku_string_t*)lVector->getData());

    auto rVector = make_shared<ValueVector>(STRING, memoryManager.get());
    dataChunk->insert(1, rVector);
    auto rData = ((ku_string_t*)rVector->getData());

    auto result = make_shared<ValueVector>(BOOL, memoryManager.get());
    dataChunk->insert(2, result);

    string value = "abcdefghijklmnopqrstuvwxy"; // 25.
    lData[0].len = 25;
    rData[0].len = 25;
    memcpy(lData[0].prefix, value.data(), ku_string_t::PREFIX_LENGTH);
    memcpy(rData[0].prefix, value.data(), ku_string_t::PREFIX_LENGTH);
    auto overflowLen = 25;
    char lOverflow[overflowLen];
    memcpy(lOverflow, value.data(), overflowLen);
    lData->overflowPtr = reinterpret_cast<uintptr_t>(lOverflow);
    char rOverflow[overflowLen];
    memcpy(rOverflow, value.data(), overflowLen);
    rData->overflowPtr = reinterpret_cast<uintptr_t>(rOverflow);

    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t, operation::Equals,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), true);

    rOverflow[overflowLen - 1] = 'z';
    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t, operation::Equals,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), false);

    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t, operation::NotEquals,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), true);

    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t, operation::LessThan,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), true);

    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t,
        operation::LessThanEquals, BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), true);

    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t,
        operation::GreaterThan, BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), false);

    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t,
        operation::GreaterThanEquals, BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), false);

    rOverflow[overflowLen - 1] = 'a';
    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t, operation::LessThan,
        BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), false);

    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t,
        operation::LessThanEquals, BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), false);

    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t,
        operation::GreaterThan, BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), true);

    BinaryOperationExecutor::executeSwitch<ku_string_t, ku_string_t, uint8_t,
        operation::GreaterThanEquals, BinaryOperationWrapper>(*lVector, *rVector, *result);
    ASSERT_EQ(result->getValue<bool>(0), true);
}

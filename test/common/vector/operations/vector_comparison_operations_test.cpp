#include "gtest/gtest.h"

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/gf_string.h"
#include "src/common/include/vector/operations/vector_comparison_operations.h"

using namespace graphflow::common;
using namespace std;

TEST(VectorCmpTests, cmpInt) {
    auto numTuples = 100;

    auto dataChunk = make_shared<DataChunk>();
    dataChunk->state->size = numTuples;

    auto lVector = make_shared<ValueVector>(INT32);
    dataChunk->append(lVector);
    auto lData = (int32_t*)lVector->values;

    auto rVector = make_shared<ValueVector>(INT32);
    dataChunk->append(rVector);
    auto rData = (int32_t*)rVector->values;

    auto result = make_shared<ValueVector>(BOOL);
    dataChunk->append(result);
    auto resultData = result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < numTuples; i++) {
        lData[i] = i;
        rData[i] = 90 - i;
    }

    VectorComparisonOperations::Equals(*lVector, *rVector, *result);
    for (int32_t i = 0; i < numTuples; i++) {
        ASSERT_EQ(resultData[i], i == 45 ? TRUE : FALSE);
    }

    VectorComparisonOperations::NotEquals(*lVector, *rVector, *result);
    for (int32_t i = 0; i < numTuples; i++) {
        ASSERT_EQ(resultData[i], i == 45 ? FALSE : TRUE);
    }

    VectorComparisonOperations::LessThan(*lVector, *rVector, *result);
    for (int32_t i = 0; i < numTuples; i++) {
        ASSERT_EQ(resultData[i], i < 45 ? TRUE : FALSE);
    }

    VectorComparisonOperations::LessThanEquals(*lVector, *rVector, *result);
    for (int32_t i = 0; i < numTuples; i++) {
        ASSERT_EQ(resultData[i], i < 46 ? TRUE : FALSE);
    }

    VectorComparisonOperations::GreaterThan(*lVector, *rVector, *result);
    for (int32_t i = 0; i < numTuples; i++) {
        ASSERT_EQ(resultData[i], i < 46 ? FALSE : TRUE);
    }

    VectorComparisonOperations::GreaterThanEquals(*lVector, *rVector, *result);
    for (int32_t i = 0; i < numTuples; i++) {
        ASSERT_EQ(resultData[i], i < 45 ? FALSE : TRUE);
    }
}

TEST(VectorCmpTests, cmpTwoShortStrings) {
    auto numTuples = 1;

    auto dataChunk = make_shared<DataChunk>();
    dataChunk->state->size = numTuples;
    dataChunk->state->currPos = 0;

    auto lVector = make_shared<ValueVector>(STRING);
    dataChunk->append(lVector);
    auto lData = ((gf_string_t*)lVector->values);

    auto rVector = make_shared<ValueVector>(STRING);
    dataChunk->append(rVector);
    auto rData = ((gf_string_t*)rVector->values);

    auto result = make_shared<ValueVector>(BOOL);
    dataChunk->append(result);
    auto resultData = result->values;

    char* value = "abcdefgh";
    lData[0].len = 8;
    rData[0].len = 8;
    memcpy(lData[0].prefix, value, gf_string_t::PREFIX_LENGTH);
    memcpy(rData[0].prefix, value, gf_string_t::PREFIX_LENGTH);
    memcpy(lData[0].data, value + gf_string_t::PREFIX_LENGTH, 8 - gf_string_t::PREFIX_LENGTH);
    memcpy(rData[0].data, value + gf_string_t::PREFIX_LENGTH, 8 - gf_string_t::PREFIX_LENGTH);

    VectorComparisonOperations::Equals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);

    rData[0].data[3] = 'i';
    VectorComparisonOperations::Equals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], FALSE);

    VectorComparisonOperations::NotEquals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);

    VectorComparisonOperations::LessThan(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);

    VectorComparisonOperations::LessThanEquals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);

    VectorComparisonOperations::GreaterThan(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], FALSE);

    VectorComparisonOperations::GreaterThanEquals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], FALSE);

    rData[0].data[3] = 'a';
    VectorComparisonOperations::LessThan(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], FALSE);

    VectorComparisonOperations::LessThanEquals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], FALSE);

    VectorComparisonOperations::GreaterThan(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);

    VectorComparisonOperations::GreaterThanEquals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);
}

TEST(VectorCmpTests, cmpTwoLongStrings) {
    auto VECTOR_SIZE = 1;

    auto dataChunk = make_shared<DataChunk>();
    dataChunk->state->size = VECTOR_SIZE;
    dataChunk->state->currPos = 0;

    auto lVector = make_shared<ValueVector>(STRING);
    dataChunk->append(lVector);
    auto lData = ((gf_string_t*)lVector->values);

    auto rVector = make_shared<ValueVector>(STRING);
    dataChunk->append(rVector);
    auto rData = ((gf_string_t*)rVector->values);

    auto result = make_shared<ValueVector>(BOOL);
    dataChunk->append(result);
    auto resultData = result->values;

    char* value = "abcdefghijklmnopqrstuvwxy"; // 25.
    lData[0].len = 25;
    rData[0].len = 25;
    memcpy(lData[0].prefix, value, gf_string_t::PREFIX_LENGTH);
    memcpy(rData[0].prefix, value, gf_string_t::PREFIX_LENGTH);
    auto overflowLen = 25;
    char lOverflow[overflowLen];
    memcpy(lOverflow, value, overflowLen);
    lData->overflowPtr = reinterpret_cast<uintptr_t>(lOverflow);
    char rOverflow[overflowLen];
    memcpy(rOverflow, value, overflowLen);
    rData->overflowPtr = reinterpret_cast<uintptr_t>(rOverflow);

    VectorComparisonOperations::Equals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);

    rOverflow[overflowLen - 1] = 'z';
    VectorComparisonOperations::Equals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], FALSE);

    VectorComparisonOperations::NotEquals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);

    VectorComparisonOperations::LessThan(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);

    VectorComparisonOperations::LessThanEquals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);

    VectorComparisonOperations::GreaterThan(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], FALSE);

    VectorComparisonOperations::GreaterThanEquals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], FALSE);

    rOverflow[overflowLen - 1] = 'a';
    VectorComparisonOperations::LessThan(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], FALSE);

    VectorComparisonOperations::LessThanEquals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], FALSE);

    VectorComparisonOperations::GreaterThan(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);

    VectorComparisonOperations::GreaterThanEquals(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);
}

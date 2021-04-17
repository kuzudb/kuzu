#include "gtest/gtest.h"

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/vector/operations/vector_comparison_operations.h"
#include "src/common/include/vector/value_vector.h"

using namespace graphflow::common;
using namespace std;

TEST(VectorCmpTests, cmpInt) {
    auto numTuples = 100;

    auto dataChunk = make_shared<DataChunk>();
    dataChunk->state->size = numTuples;
    dataChunk->state->numSelectedValues = numTuples;

    auto lVector = make_shared<ValueVector>(INT32, numTuples);
    dataChunk->append(lVector);
    auto lData = (int32_t*)lVector->values;

    auto rVector = make_shared<ValueVector>(INT32, numTuples);
    dataChunk->append(rVector);
    auto rData = (int32_t*)rVector->values;

    auto result = make_shared<ValueVector>(BOOL, numTuples);
    dataChunk->append(result);
    auto resultData = result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < numTuples; i++) {
        lData[i] = i;
        rData[i] = 90 - i;
    }

    auto equalsOp = ValueVector::getBinaryOperation(ExpressionType::EQUALS);
    equalsOp(*lVector, *rVector, *result);
    for (int32_t i = 0; i < numTuples; i++) {
        ASSERT_EQ(resultData[i], i == 45 ? TRUE : FALSE);
    }

    auto notEqualsOp = ValueVector::getBinaryOperation(ExpressionType::NOT_EQUALS);
    notEqualsOp(*lVector, *rVector, *result);
    for (int32_t i = 0; i < numTuples; i++) {
        ASSERT_EQ(resultData[i], i == 45 ? FALSE : TRUE);
    }

    auto lessThanOp = ValueVector::getBinaryOperation(ExpressionType::LESS_THAN);
    lessThanOp(*lVector, *rVector, *result);
    for (int32_t i = 0; i < numTuples; i++) {
        ASSERT_EQ(resultData[i], i < 45 ? TRUE : FALSE);
    }

    auto lessThanEqualsOp = ValueVector::getBinaryOperation(ExpressionType::LESS_THAN_EQUALS);
    lessThanEqualsOp(*lVector, *rVector, *result);
    for (int32_t i = 0; i < numTuples; i++) {
        ASSERT_EQ(resultData[i], i < 46 ? TRUE : FALSE);
    }

    auto greaterThanOp = ValueVector::getBinaryOperation(ExpressionType::GREATER_THAN);
    greaterThanOp(*lVector, *rVector, *result);
    for (int32_t i = 0; i < numTuples; i++) {
        ASSERT_EQ(resultData[i], i < 46 ? FALSE : TRUE);
    }

    auto greaterThanEqualsOp = ValueVector::getBinaryOperation(ExpressionType::GREATER_THAN_EQUALS);
    greaterThanEqualsOp(*lVector, *rVector, *result);
    for (int32_t i = 0; i < numTuples; i++) {
        ASSERT_EQ(resultData[i], i < 45 ? FALSE : TRUE);
    }
}

TEST(VectorCmpTests, cmpTwoShortStrings) {
    auto numTuples = 1;

    auto dataChunk = make_shared<DataChunk>();
    dataChunk->state->size = numTuples;
    dataChunk->state->currPos = 0;

    auto lVector = make_shared<ValueVector>(STRING, numTuples);
    dataChunk->append(lVector);
    auto lData = ((gf_string_t*)lVector->values);

    auto rVector = make_shared<ValueVector>(STRING, numTuples);
    dataChunk->append(rVector);
    auto rData = ((gf_string_t*)rVector->values);

    auto result = make_shared<ValueVector>(BOOL, numTuples);
    dataChunk->append(result);
    auto resultData = result->values;

    char* value = "abcdefgh";
    lData[0].len = 8;
    rData[0].len = 8;
    memcpy(lData[0].prefix, value, gf_string_t::PREFIX_LENGTH);
    memcpy(rData[0].prefix, value, gf_string_t::PREFIX_LENGTH);
    memcpy(lData[0].data, value + gf_string_t::PREFIX_LENGTH, 8 - gf_string_t::PREFIX_LENGTH);
    memcpy(rData[0].data, value + gf_string_t::PREFIX_LENGTH, 8 - gf_string_t::PREFIX_LENGTH);

    auto equalsOp = ValueVector::getBinaryOperation(ExpressionType::EQUALS);
    equalsOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);

    rData[0].data[3] = 'i';
    equalsOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], FALSE);

    auto notEqualsOp = ValueVector::getBinaryOperation(ExpressionType::NOT_EQUALS);
    notEqualsOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);

    auto lessThanOp = ValueVector::getBinaryOperation(ExpressionType::LESS_THAN);
    lessThanOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);

    auto lessThanEqualsOp = ValueVector::getBinaryOperation(ExpressionType::LESS_THAN_EQUALS);
    lessThanEqualsOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);

    auto greaterThanOp = ValueVector::getBinaryOperation(ExpressionType::GREATER_THAN);
    greaterThanOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], FALSE);

    auto greaterThanEqualsOp = ValueVector::getBinaryOperation(ExpressionType::GREATER_THAN_EQUALS);
    greaterThanEqualsOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], FALSE);

    rData[0].data[3] = 'a';
    lessThanOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], FALSE);

    lessThanEqualsOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], FALSE);

    greaterThanOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);

    greaterThanEqualsOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);
}

TEST(VectorCmpTests, cmpTwoLongStrings) {
    auto VECTOR_SIZE = 1;

    auto dataChunk = make_shared<DataChunk>();
    dataChunk->state->size = VECTOR_SIZE;
    dataChunk->state->numSelectedValues = VECTOR_SIZE;
    dataChunk->state->currPos = 0;

    auto lVector = make_shared<ValueVector>(STRING, VECTOR_SIZE);
    dataChunk->append(lVector);
    auto lData = ((gf_string_t*)lVector->values);

    auto rVector = make_shared<ValueVector>(STRING, VECTOR_SIZE);
    dataChunk->append(rVector);
    auto rData = ((gf_string_t*)rVector->values);

    auto result = make_shared<ValueVector>(BOOL, VECTOR_SIZE);
    dataChunk->append(result);
    auto resultData = result->values;

    char* value = "abcdefghijklmnopqrstuvwxy"; // 25.
    lData[0].len = 25;
    rData[0].len = 25;
    memcpy(lData[0].prefix, value, gf_string_t::PREFIX_LENGTH);
    memcpy(rData[0].prefix, value, gf_string_t::PREFIX_LENGTH);
    auto overflowLen = 25 - gf_string_t::PREFIX_LENGTH;
    char lOverflow[overflowLen];
    memcpy(lOverflow, value + gf_string_t::PREFIX_LENGTH, overflowLen);
    lData->overflowPtr = reinterpret_cast<uintptr_t>(lOverflow);
    char rOverflow[overflowLen];
    memcpy(rOverflow, value + gf_string_t::PREFIX_LENGTH, overflowLen);
    rData->overflowPtr = reinterpret_cast<uintptr_t>(rOverflow);

    auto equalsOp = ValueVector::getBinaryOperation(ExpressionType::EQUALS);
    equalsOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);

    rOverflow[overflowLen - 1] = 'z';
    equalsOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], FALSE);

    auto notEqualsOp = ValueVector::getBinaryOperation(ExpressionType::NOT_EQUALS);
    notEqualsOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);

    auto lessThanOp = ValueVector::getBinaryOperation(ExpressionType::LESS_THAN);
    lessThanOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);

    auto lessThanEqualsOp = ValueVector::getBinaryOperation(ExpressionType::LESS_THAN_EQUALS);
    lessThanEqualsOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);

    auto greaterThanOp = ValueVector::getBinaryOperation(ExpressionType::GREATER_THAN);
    greaterThanOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], FALSE);

    auto greaterThanEqualsOp = ValueVector::getBinaryOperation(ExpressionType::GREATER_THAN_EQUALS);
    greaterThanEqualsOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], FALSE);

    rOverflow[overflowLen - 1] = 'a';
    lessThanOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], FALSE);

    lessThanEqualsOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], FALSE);

    greaterThanOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);

    greaterThanEqualsOp(*lVector, *rVector, *result);
    ASSERT_EQ(resultData[0], TRUE);
}

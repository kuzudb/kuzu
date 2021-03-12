#include "gtest/gtest.h"

#include "src/common/include/vector/value_vector.h"

using namespace graphflow::common;
using namespace std;

TEST(VectorCmpTests, test) {
    auto dataChunk = make_shared<DataChunk>();
    dataChunk->size = 100;

    auto lVector = ValueVector(INT);
    lVector.setDataChunkOwner(dataChunk);
    auto lData = (int32_t*)lVector.getValues();

    auto rVector = ValueVector(INT);
    rVector.setDataChunkOwner(dataChunk);
    auto rData = (int32_t*)rVector.getValues();

    auto result = ValueVector(BOOL);
    auto resultData = result.getValues();
    // Fill values before the comparison.
    for (int32_t i = 0; i < 100; i++) {
        lData[i] = i;
        rData[i] = 90 - i;
    }

    auto equalsOp = ValueVector::getBinaryOperation(ExpressionType::EQUALS);
    equalsOp(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i == 45 ? TRUE : FALSE);
    }

    auto notEqualsOp = ValueVector::getBinaryOperation(ExpressionType::NOT_EQUALS);
    notEqualsOp(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i == 45 ? FALSE : TRUE);
    }

    auto lessThanOp = ValueVector::getBinaryOperation(ExpressionType::LESS_THAN);
    lessThanOp(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i < 45 ? TRUE : FALSE);
    }

    auto lessThanEqualsOp = ValueVector::getBinaryOperation(ExpressionType::LESS_THAN_EQUALS);
    lessThanEqualsOp(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i < 46 ? TRUE : FALSE);
    }

    auto greaterThanOp = ValueVector::getBinaryOperation(ExpressionType::GREATER_THAN);
    greaterThanOp(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i < 46 ? FALSE : TRUE);
    }

    auto greaterThanEqualsOp =
        ValueVector::getBinaryOperation(ExpressionType::GREATER_THAN_EQUALS);
    greaterThanEqualsOp(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i < 45 ? FALSE : TRUE);
    }
}

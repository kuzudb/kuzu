#include <memory>

#include "gtest/gtest.h"

#include "src/common/include/vector/operations/vector_operations.h"

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

    auto equalsOp = VectorOperations::getOperation(BINARY_OPERATION::EQUALS);
    equalsOp(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i == 45 ? TRUE : FALSE);
    }

    auto notEqualsOp = VectorOperations::getOperation(BINARY_OPERATION::NOT_EQUALS);
    notEqualsOp(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i == 45 ? FALSE : TRUE);
    }

    auto lessThanOp = VectorOperations::getOperation(BINARY_OPERATION::LESS_THAN);
    lessThanOp(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i < 45 ? TRUE : FALSE);
    }

    auto lessThanEqualsOp = VectorOperations::getOperation(BINARY_OPERATION::LESS_THAN_EQUALS);
    lessThanEqualsOp(lVector, rVector, result);
    VectorOperations::LessThanEquals(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i < 46 ? TRUE : FALSE);
    }

    auto greaterThanOp = VectorOperations::getOperation(BINARY_OPERATION::GREATER_THAN);
    greaterThanOp(lVector, rVector, result);
    VectorOperations::GreaterThan(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i < 46 ? FALSE : TRUE);
    }

    auto greaterThanEqualsOp = VectorOperations::getOperation(BINARY_OPERATION::GREATER_THAN_EQUALS);
    greaterThanEqualsOp(lVector, rVector, result);
    VectorOperations::GreaterThanEquals(lVector, rVector, result);
    for (int32_t i = 0; i < 100; i++) {
        ASSERT_EQ(resultData[i], i < 45 ? FALSE : TRUE);
    }
}

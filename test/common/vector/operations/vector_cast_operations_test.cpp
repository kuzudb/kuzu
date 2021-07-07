#include "gtest/gtest.h"

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/date.h"
#include "src/common/include/value.h"
#include "src/common/include/vector/operations/vector_cast_operations.h"

using namespace graphflow::common;
using namespace std;

class VectorCastOperationsTest : public testing::Test {

public:
    const int32_t VECTOR_SIZE = 100;

    void SetUp() override {
        dataChunk = make_shared<DataChunk>();
        dataChunk->state->size = VECTOR_SIZE;
        memoryManager = make_unique<MemoryManager>();
    }

public:
    shared_ptr<DataChunk> dataChunk;
    unique_ptr<MemoryManager> memoryManager;
};

TEST_F(VectorCastOperationsTest, CastStructuredBoolToUnstructuredValueTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), BOOL);
    dataChunk->append(lVector);
    auto lData = (bool*)lVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->append(result);
    auto resultData = (Value*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i] = (i % 2) == 0;
    }
    VectorCastOperations::castStructuredToUnstructuredValue(*lVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.booleanVal, (i % 2) == 0);
    }
}

TEST_F(VectorCastOperationsTest, CastStructuredInt64ToUnstructuredValueTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), INT64);
    dataChunk->append(lVector);
    auto lData = (int64_t*)lVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->append(result);
    auto resultData = (Value*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i] = i * 2;
    }
    VectorCastOperations::castStructuredToUnstructuredValue(*lVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.int64Val, i * 2);
    }
}

TEST_F(VectorCastOperationsTest, CastStructuredDoubleToUnstructuredValueTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), DOUBLE);
    dataChunk->append(lVector);
    auto lData = (double*)lVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->append(result);
    auto resultData = (Value*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i] = (double)(i * 2);
    }
    VectorCastOperations::castStructuredToUnstructuredValue(*lVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.doubleVal, (double)(i * 2));
    }
}

TEST_F(VectorCastOperationsTest, CastStructuredDateToUnstructuredValueTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), DATE);
    dataChunk->append(lVector);
    auto lData = (date_t*)lVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->append(result);
    auto resultData = (Value*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i] = date_t(i * 2);
    }
    VectorCastOperations::castStructuredToUnstructuredValue(*lVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.dateVal, date_t(i * 2));
    }
}

TEST_F(VectorCastOperationsTest, CastStructuredStringToUnstructuredValueTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->append(lVector);
    auto lData = (gf_string_t*)lVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->append(result);
    auto resultData = (Value*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        string lStr = to_string(i * 2);
        lVector->allocateStringOverflowSpace(lData[i], lStr.length());
        lData[i].set(lStr);
    }
    VectorCastOperations::castStructuredToUnstructuredValue(*lVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.strVal.getAsString(), to_string(i * 2));
    }
}

TEST_F(VectorCastOperationsTest, CastStructuredBooleanToStringTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), BOOL);
    dataChunk->append(lVector);
    auto lData = (bool*)lVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->append(result);
    auto resultData = (gf_string_t*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i] = i % 2 == 0;
    }
    VectorCastOperations::castStructuredToStringValue(*lVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].getAsString(), (i % 2 == 0 ? "True" : "False"));
    }
}

TEST_F(VectorCastOperationsTest, CastStructuredInt32ToStringTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), INT64);
    dataChunk->append(lVector);
    auto lData = (int64_t*)lVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->append(result);
    auto resultData = (gf_string_t*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i] = i * 2;
    }
    VectorCastOperations::castStructuredToStringValue(*lVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].getAsString(), to_string(i * 2));
    }
}

TEST_F(VectorCastOperationsTest, CastStructuredDoubleToStringTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), DOUBLE);
    dataChunk->append(lVector);
    auto lData = (double*)lVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->append(result);
    auto resultData = (gf_string_t*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i] = (double)(i * 2);
    }
    VectorCastOperations::castStructuredToStringValue(*lVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].getAsString(), to_string((double)(i * 2)));
    }
}

TEST_F(VectorCastOperationsTest, CastStructuredDateToStringTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), DATE);
    dataChunk->append(lVector);
    auto lData = (date_t*)lVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->append(result);
    auto resultData = (gf_string_t*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i] = date_t(1000 + i);
    }
    VectorCastOperations::castStructuredToStringValue(*lVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].getAsString(), Date::toString(date_t(1000 + i)));
    }
}

TEST_F(VectorCastOperationsTest, CastUnStructuredBooleanToBooleanTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->append(lVector);
    auto lData = (Value*)lVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), BOOL);
    dataChunk->append(result);
    auto resultData = (bool*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i].val.booleanVal = (i % 2 == 0);
        lData[i].dataType = BOOL;
    }
    VectorCastOperations::castUnstructuredToBoolValue(*lVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i], (i % 2 == 0));
    }
}

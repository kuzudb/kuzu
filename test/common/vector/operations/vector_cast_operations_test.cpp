#include "gtest/gtest.h"

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/function/cast/include/vector_cast_operations.h"
#include "src/function/include/unary_operation_executor.h"

using namespace graphflow::function;
using namespace graphflow::common;
using namespace std;

class VectorCastOperationsTest : public testing::Test {

public:
    const int32_t VECTOR_SIZE = 100;

    void SetUp() override {
        dataChunk = make_shared<DataChunk>(2);
        dataChunk->state->selectedSize = VECTOR_SIZE;
        bufferManager = make_unique<BufferManager>();
        memoryManager = make_unique<MemoryManager>(bufferManager.get());
    }

public:
    unique_ptr<BufferManager> bufferManager;
    unique_ptr<MemoryManager> memoryManager;
    shared_ptr<DataChunk> dataChunk;
};

TEST_F(VectorCastOperationsTest, CastStructuredBoolToUnstructuredValueTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), BOOL);
    dataChunk->insert(0, lVector);
    auto lData = (bool*)lVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->insert(1, result);
    auto resultData = (Value*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i] = (i % 2) == 0;
    }
    VectorOperations::UnaryExecFunction<uint8_t, Value, operation::CastToUnstructured>(
        vector<shared_ptr<ValueVector>>{lVector}, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.booleanVal, (i % 2) == 0);
    }
}

TEST_F(VectorCastOperationsTest, CastStructuredInt64ToUnstructuredValueTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), INT64);
    dataChunk->insert(0, lVector);
    auto lData = (int64_t*)lVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->insert(1, result);
    auto resultData = (Value*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i] = i * 2;
    }
    VectorOperations::UnaryExecFunction<int64_t, Value, operation::CastToUnstructured>(
        vector<shared_ptr<ValueVector>>{lVector}, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.int64Val, i * 2);
    }
}

TEST_F(VectorCastOperationsTest, CastStructuredDoubleToUnstructuredValueTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), DOUBLE);
    dataChunk->insert(0, lVector);
    auto lData = (double*)lVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->insert(1, result);
    auto resultData = (Value*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i] = (double)(i * 2);
    }
    VectorOperations::UnaryExecFunction<double_t, Value, operation::CastToUnstructured>(
        vector<shared_ptr<ValueVector>>{lVector}, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.doubleVal, (double)(i * 2));
    }
}

TEST_F(VectorCastOperationsTest, CastStructuredDateToUnstructuredValueTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), DATE);
    dataChunk->insert(0, lVector);
    auto lData = (date_t*)lVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->insert(1, result);
    auto resultData = (Value*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i] = date_t(i * 2);
    }
    VectorOperations::UnaryExecFunction<date_t, Value, operation::CastToUnstructured>(
        vector<shared_ptr<ValueVector>>{lVector}, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.dateVal, date_t(i * 2));
    }
}

TEST_F(VectorCastOperationsTest, CastStructuredStringToUnstructuredValueTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->insert(0, lVector);
    auto lData = (gf_string_t*)lVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->insert(1, result);
    auto resultData = (Value*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        string lStr = to_string(i * 2);
        TypeUtils::copyString(lStr, lData[i], lVector->getOverflowBuffer());
    }
    VectorCastOperations::castStringToUnstructured(
        vector<shared_ptr<ValueVector>>{lVector}, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.strVal.getAsString(), to_string(i * 2));
    }
}

TEST_F(VectorCastOperationsTest, CastStructuredBooleanToStringTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), BOOL);
    dataChunk->insert(0, lVector);
    auto lData = (bool*)lVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->insert(1, result);
    auto resultData = (gf_string_t*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i] = i % 2 == 0;
    }
    VectorCastOperations::castToString<bool>(vector<shared_ptr<ValueVector>>{lVector}, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].getAsString(), (i % 2 == 0 ? "True" : "False"));
    }
}

TEST_F(VectorCastOperationsTest, CastStructuredInt32ToStringTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), INT64);
    dataChunk->insert(0, lVector);
    auto lData = (int64_t*)lVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->insert(1, result);
    auto resultData = (gf_string_t*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i] = i * 2;
    }
    VectorCastOperations::castToString<int64_t>(vector<shared_ptr<ValueVector>>{lVector}, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].getAsString(), to_string(i * 2));
    }
}

TEST_F(VectorCastOperationsTest, CastStructuredDoubleToStringTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), DOUBLE);
    dataChunk->insert(0, lVector);
    auto lData = (double*)lVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->insert(1, result);
    auto resultData = (gf_string_t*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i] = (double)(i * 2);
    }
    VectorCastOperations::castToString<double_t>(vector<shared_ptr<ValueVector>>{lVector}, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].getAsString(), to_string((double)(i * 2)));
    }
}

TEST_F(VectorCastOperationsTest, CastStructuredDateToStringTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), DATE);
    dataChunk->insert(0, lVector);
    auto lData = (date_t*)lVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->insert(1, result);
    auto resultData = (gf_string_t*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i] = date_t(1000 + i);
    }
    VectorCastOperations::castToString<date_t>(vector<shared_ptr<ValueVector>>{lVector}, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].getAsString(), Date::toString(date_t(1000 + i)));
    }
}

TEST_F(VectorCastOperationsTest, CastUnStructuredBooleanToBooleanTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->insert(0, lVector);
    auto lData = (Value*)lVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), BOOL);
    dataChunk->insert(1, result);
    auto resultData = (bool*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i].val.booleanVal = (i % 2 == 0);
        lData[i].dataType.typeID = BOOL;
    }

    UnaryOperationExecutor::execute<Value, uint8_t, operation::CastUnstructuredToBool>(
        *lVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i], (i % 2 == 0));
    }
}

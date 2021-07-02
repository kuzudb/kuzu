#include "gtest/gtest.h"

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/value.h"
#include "src/common/include/vector/operations/vector_arithmetic_operations.h"

using namespace graphflow::common;
using namespace std;

class VectorArithmeticOperationsTest : public testing::Test {
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

TEST_F(VectorArithmeticOperationsTest, Int32Test) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), INT32);
    dataChunk->append(lVector);
    auto lData = (int32_t*)lVector->values;

    auto rVector = make_shared<ValueVector>(memoryManager.get(), INT32);
    dataChunk->append(rVector);
    auto rData = (int32_t*)rVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), INT32);
    dataChunk->append(result);
    auto resultData = (int32_t*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i] = i;
        rData[i] = 110 - i;
    }

    VectorArithmeticOperations::Negate(*lVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i], -i);
    }

    VectorArithmeticOperations::Add(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i], 110);
    }

    VectorArithmeticOperations::Subtract(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i], 2 * i - 110);
    }

    VectorArithmeticOperations::Multiply(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i], i * (110 - i));
    }

    VectorArithmeticOperations::Divide(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i], i / (110 - i));
    }

    /* note rVector and lVector are flipped */
    VectorArithmeticOperations::Modulo(*rVector, *lVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i], i % (110 - i));
    }

    result = make_shared<ValueVector>(memoryManager.get(), DOUBLE);
    dataChunk->append(result);
    auto resultDataAsDoubleArr = (double_t*)result->values;
    VectorArithmeticOperations::Power(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultDataAsDoubleArr[i], pow(i, 110 - i));
    }
}

TEST_F(VectorArithmeticOperationsTest, StringTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->append(lVector);

    auto rVector = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->append(rVector);

    auto result = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->append(result);
    auto resultData = (gf_string_t*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lVector->addString(i, to_string(i));
        rVector->addString(i, to_string(110 - i));
    }

    VectorArithmeticOperations::Add(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].getAsString(), to_string(i) + to_string(110 - i));
    }
}

TEST_F(VectorArithmeticOperationsTest, BigStringTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->append(lVector);

    auto rVector = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->append(rVector);

    auto result = make_shared<ValueVector>(memoryManager.get(), STRING);
    dataChunk->append(result);
    auto resultData = (gf_string_t*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lVector->addString(i, to_string(i) + "abcdefabcdefqwert");
        rVector->addString(i, to_string(110 - i) + "abcdefabcdefqwert");
    }

    VectorArithmeticOperations::Add(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].getAsString(),
            to_string(i) + "abcdefabcdefqwert" + to_string(110 - i) + "abcdefabcdefqwert");
    }
}

TEST_F(VectorArithmeticOperationsTest, UnstructuredInt32Test) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->append(lVector);
    auto lData = (Value*)lVector->values;

    auto rVector = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->append(rVector);
    auto rData = (Value*)rVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->append(result);
    auto resultData = (Value*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i] = Value(i);
        rData[i] = Value(110 - i);
    }

    VectorArithmeticOperations::Negate(*lVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.int32Val, -i);
    }

    VectorArithmeticOperations::Add(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.int32Val, 110);
    }

    VectorArithmeticOperations::Subtract(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.int32Val, 2 * i - 110);
    }

    VectorArithmeticOperations::Multiply(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.int32Val, i * (110 - i));
    }

    VectorArithmeticOperations::Divide(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.int32Val, i / (110 - i));
    }

    /* note rVector and lVector are flipped */
    VectorArithmeticOperations::Modulo(*rVector, *lVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.int32Val, i % (110 - i));
    }
}

TEST_F(VectorArithmeticOperationsTest, UnstructuredInt32AndDoubleTest) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->append(lVector);
    auto lData = (Value*)lVector->values;

    auto rVector = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->append(rVector);
    auto rData = (Value*)rVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->append(result);
    auto resultData = (Value*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        lData[i] = Value((double)i);
        rData[i] = Value(110 - i);
    }

    VectorArithmeticOperations::Negate(*lVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.doubleVal, (double)-i);
    }

    VectorArithmeticOperations::Add(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.doubleVal, (double)110);
    }

    VectorArithmeticOperations::Subtract(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.doubleVal, (double)(2 * i - 110));
    }

    VectorArithmeticOperations::Multiply(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.doubleVal, (double)(i * (110 - i)));
    }

    VectorArithmeticOperations::Divide(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.doubleVal, (double)i / (110 - i));
    }
}

TEST_F(VectorArithmeticOperationsTest, UnstructuredStringAndInt32Test) {
    auto lVector = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->append(lVector);
    auto lData = (Value*)lVector->values;

    auto rVector = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->append(rVector);
    auto rData = (Value*)rVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), UNSTRUCTURED);
    dataChunk->append(result);
    auto resultData = (Value*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        string lStr = to_string(i);
        lVector->allocateStringOverflowSpace(lData[i].val.strVal, lStr.length());
        lData[i].val.strVal.set(lStr);
        lData[i].dataType = STRING;
        rData[i] = Value(110 - i);
    }

    VectorArithmeticOperations::Add(*lVector, *rVector, *result);
    for (int32_t i = 0; i < VECTOR_SIZE; i++) {
        ASSERT_EQ(resultData[i].val.strVal.getAsString(), to_string(i) + to_string(110 - i));
    }
}

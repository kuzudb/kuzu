#include "function/binary_operation_executor.h"
#include "function/null/null_operation_executor.h"
#include "function/null/null_operations.h"
#include "function/string/operations/left_operation.h"
#include "function/string/operations/substr_operation.h"
#include "function/ternary_operation_executor.h"
#include "gtest/gtest.h"
#include "processor/result/result_set.h"

using ::testing::Test;

using namespace kuzu::processor;
using namespace kuzu::function;

class OperationExecutorTest : public Test {

public:
    void SetUp() override {
        bufferManager =
            make_unique<BufferManager>(StorageConfig::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING);
        memoryManager = make_unique<MemoryManager>(bufferManager.get());
        flatDataChunk = make_shared<DataChunk>(5);
        unFlatDataChunk = make_shared<DataChunk>(5);

        flatDataChunk->state->currIdx = 0;
        flatDataChunk->state->selVector->selectedSize = 3;
        flatDataChunk->state->originalSize = 3;
        unFlatDataChunk->state->currIdx = -1;
        unFlatDataChunk->state->selVector->selectedSize = 3;
        unFlatDataChunk->state->originalSize = 3;

        flatDataChunk->insert(0, getStringValueVector());
        unFlatDataChunk->insert(0, getStringValueVector());
        flatDataChunk->insert(1, getInt64ValueVector(true /* isStartPosVector */));
        unFlatDataChunk->insert(1, getInt64ValueVector(true /* isStartPosVector */));
        flatDataChunk->insert(2, getInt64ValueVector(false /* isStartPosVector */));
        unFlatDataChunk->insert(2, getInt64ValueVector(false /* isStartPosVector */));
        flatDataChunk->insert(3, getStringValueVector());
        unFlatDataChunk->insert(3, getStringValueVector());
        flatDataChunk->insert(4, make_shared<ValueVector>(BOOL, memoryManager.get()));
        unFlatDataChunk->insert(4, make_shared<ValueVector>(BOOL, memoryManager.get()));

        unFlatValueVectorA = unFlatDataChunk->valueVectors[0];
        unFlatValueVectorB = unFlatDataChunk->valueVectors[1];
        unFlatValueVectorC = unFlatDataChunk->valueVectors[2];
        unFlatResultValueVector = unFlatDataChunk->valueVectors[3];
        unFlatBoolValueVector = unFlatDataChunk->valueVectors[4];
        flatValueVectorA = flatDataChunk->valueVectors[0];
        flatValueVectorB = flatDataChunk->valueVectors[1];
        flatValueVectorC = flatDataChunk->valueVectors[2];
        flatResultValueVector = flatDataChunk->valueVectors[3];
        flatBoolValueVector = flatDataChunk->valueVectors[4];
    }

    void validateStringResultValueVector(
        shared_ptr<ValueVector>& valueVector, const vector<string>& expectedValues) {
        if (valueVector->state->isFlat()) {
            assert(expectedValues.size() == 1);
            ASSERT_EQ(valueVector->getValue<ku_string_t>(valueVector->state->getPositionOfCurrIdx())
                          .getAsString(),
                expectedValues[0]);
        } else {
            for (auto i = 0u; i < expectedValues.size(); i++) {
                if (expectedValues[i] == "") {
                    ASSERT_EQ(valueVector->isNull(i), true);
                } else {
                    ASSERT_EQ(valueVector
                                  ->getValue<ku_string_t>(
                                      valueVector->state->selVector->selectedPositions[i])
                                  .getAsString(),
                        expectedValues[i]);
                }
            }
        }
    }

    void validateBoolResultValueVector(
        shared_ptr<ValueVector>& valueVector, const vector<bool>& expectedValues) {
        if (valueVector->state->isFlat()) {
            assert(expectedValues.size() == 1);
            ASSERT_EQ(valueVector->getValue<bool>(valueVector->state->getPositionOfCurrIdx()),
                expectedValues[0]);
        } else {
            for (auto i = 0u; i < expectedValues.size(); i++) {
                ASSERT_EQ(valueVector->getValue<bool>(
                              valueVector->state->selVector->selectedPositions[i]),
                    expectedValues[i]);
            }
        }
    }

    void setUnFlatDataChunkFiltered() {
        unFlatDataChunk->state->selVector->selectedSize = 2;
        unFlatDataChunk->state->selVector->resetSelectorToValuePosBuffer();
        unFlatDataChunk->state->selVector->selectedPositions[0] = (sel_t)0;
        unFlatDataChunk->state->selVector->selectedPositions[1] = (sel_t)2;
    }

    // Since nullmask is a private field of valueVector, we do this trick to let
    // hasNoNullGuarantee() return false.
    void setHasNoNullGuaranteeToFalse(shared_ptr<ValueVector>& valueVector) {
        valueVector->setNull(0, true);
        valueVector->setNull(0, false);
    }

    void runTernaryOperationExecutorTest(vector<string>& expectedStr,
        shared_ptr<ValueVector>& valueVectorA, shared_ptr<ValueVector>& valueVectorB,
        shared_ptr<ValueVector>& valueVectorC, shared_ptr<ValueVector>& resultValueVector,
        shared_ptr<ValueVector>& valueVectorToSetHasNoNullGuarantee) {
        TernaryOperationExecutor::executeStringAndList<ku_string_t, int64_t, int64_t, ku_string_t,
            operation::SubStr>(*valueVectorA, *valueVectorB, *valueVectorC, *resultValueVector);
        validateStringResultValueVector(resultValueVector, expectedStr);

        setHasNoNullGuaranteeToFalse(valueVectorToSetHasNoNullGuarantee);
        TernaryOperationExecutor::executeStringAndList<ku_string_t, int64_t, int64_t, ku_string_t,
            operation::SubStr>(*valueVectorA, *valueVectorB, *valueVectorC, *resultValueVector);
        validateStringResultValueVector(resultValueVector, expectedStr);
    }

    void runBinaryOperationExecutorTest(vector<string>& expectedStr,
        shared_ptr<ValueVector>& valueVectorA, shared_ptr<ValueVector>& valueVectorB,
        shared_ptr<ValueVector>& resultValueVector,
        shared_ptr<ValueVector>& valueVectorToSetHasNoNullGuarantee) {
        BinaryOperationExecutor::executeStringAndList<ku_string_t, int64_t, ku_string_t,
            operation::Left>(*valueVectorA, *valueVectorB, *resultValueVector);
        validateStringResultValueVector(resultValueVector, expectedStr);

        setHasNoNullGuaranteeToFalse(valueVectorToSetHasNoNullGuarantee);
        BinaryOperationExecutor::executeStringAndList<ku_string_t, int64_t, ku_string_t,
            operation::Left>(*valueVectorA, *valueVectorB, *resultValueVector);
        validateStringResultValueVector(resultValueVector, expectedStr);
    }

private:
    shared_ptr<ValueVector> getStringValueVector() {
        auto valueVector = make_shared<ValueVector>(STRING, memoryManager.get());
        valueVector->setValue<string>(0, "This is a long string1");
        valueVector->setValue<string>(1, "This is another long string");
        valueVector->setValue<string>(2, "Substring test");
        return valueVector;
    }
    shared_ptr<ValueVector> getInt64ValueVector(bool isStartPosVector) {
        auto valueVector = make_shared<ValueVector>(INT64, memoryManager.get());
        vector<int64_t> data =
            isStartPosVector ? vector<int64_t>{2, 1, 3} : vector<int64_t>{7, 10, 12};
        valueVector->setValue(0, data[0]);
        valueVector->setValue(1, data[1]);
        valueVector->setValue(2, data[2]);
        return valueVector;
    }

public:
    unique_ptr<BufferManager> bufferManager;
    unique_ptr<MemoryManager> memoryManager;
    shared_ptr<ValueVector> unFlatValueVectorA;
    shared_ptr<ValueVector> unFlatValueVectorB;
    shared_ptr<ValueVector> unFlatValueVectorC;
    shared_ptr<ValueVector> unFlatResultValueVector;
    shared_ptr<ValueVector> flatValueVectorA;
    shared_ptr<ValueVector> flatValueVectorB;
    shared_ptr<ValueVector> flatValueVectorC;
    shared_ptr<ValueVector> flatResultValueVector;
    shared_ptr<DataChunk> flatDataChunk;
    shared_ptr<DataChunk> unFlatDataChunk;
    shared_ptr<ValueVector> flatBoolValueVector;
    shared_ptr<ValueVector> unFlatBoolValueVector;
};

TEST_F(OperationExecutorTest, BinaryOperationExecutorAllFlat) {
    vector<string> expectedStr = {"Th"};
    BinaryOperationExecutor::executeStringAndList<ku_string_t, int64_t, ku_string_t,
        operation::Left>(*flatValueVectorA, *flatValueVectorB, *flatResultValueVector);
    validateStringResultValueVector(flatResultValueVector, expectedStr);
}

TEST_F(OperationExecutorTest, BinaryOperationExecutorAllUnFlatAndUnfiltered) {
    vector<string> expectedStr = {"Th", "T", "Sub"};
    runBinaryOperationExecutorTest(expectedStr, unFlatValueVectorA, unFlatValueVectorB,
        unFlatResultValueVector, unFlatValueVectorA);
}

TEST_F(OperationExecutorTest, BinaryOperationExecutorAllUnFlatAndFiltered) {
    vector<string> expectedStr = {"Th", "Sub"};
    setUnFlatDataChunkFiltered();
    runBinaryOperationExecutorTest(expectedStr, unFlatValueVectorA, unFlatValueVectorB,
        unFlatResultValueVector, unFlatValueVectorA);
}

TEST_F(OperationExecutorTest, BinaryOperationExecutorFlatUnFlatAndUnfiltered) {
    vector<string> expectedStr = {"Th", "T", "Thi"};
    runBinaryOperationExecutorTest(expectedStr, flatValueVectorA, unFlatValueVectorB,
        unFlatResultValueVector, unFlatValueVectorB);
}

TEST_F(OperationExecutorTest, BinaryOperationExecutorFlatUnFlatAndFiltered) {
    vector<string> expectedStr = {"Th", "Thi"};
    setUnFlatDataChunkFiltered();
    runBinaryOperationExecutorTest(expectedStr, flatValueVectorA, unFlatValueVectorB,
        unFlatResultValueVector, unFlatValueVectorB);
}

TEST_F(OperationExecutorTest, BinaryOperationExecutorFlatUnFlatWithNull) {
    vector<string> expectedStr = {"", ""};
    flatValueVectorA->setNull(0, true);
    runBinaryOperationExecutorTest(expectedStr, flatValueVectorA, unFlatValueVectorB,
        unFlatResultValueVector, unFlatValueVectorB);
}

TEST_F(OperationExecutorTest, BinaryOperationExecutorUnFlatFlatAndUnfiltered) {
    vector<string> expectedStr = {"Th", "Th", "Su"};
    runBinaryOperationExecutorTest(expectedStr, unFlatValueVectorA, flatValueVectorB,
        unFlatResultValueVector, unFlatValueVectorA);
}

TEST_F(OperationExecutorTest, BinaryOperationExecutorUnFlatFlatAndFiltered) {
    vector<string> expectedStr = {"Th", "Su"};
    setUnFlatDataChunkFiltered();
    runBinaryOperationExecutorTest(expectedStr, unFlatValueVectorA, flatValueVectorB,
        unFlatResultValueVector, unFlatValueVectorA);
}

TEST_F(OperationExecutorTest, BinaryOperationExecutorUnFlatFlatWithNull) {
    vector<string> expectedStr = {"", ""};
    flatValueVectorB->setNull(0, true);
    runBinaryOperationExecutorTest(expectedStr, unFlatValueVectorA, flatValueVectorB,
        unFlatResultValueVector, unFlatValueVectorA);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorAllFlat) {
    vector<string> expectedStr = {"his is "};
    TernaryOperationExecutor::executeStringAndList<ku_string_t, int64_t, int64_t, ku_string_t,
        operation::SubStr>(
        *flatValueVectorA, *flatValueVectorB, *flatValueVectorC, *flatResultValueVector);
    validateStringResultValueVector(flatResultValueVector, expectedStr);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorAllUnFlatAndUnfiltered) {
    vector<string> expectedStr = {"his is ", "This is an", "bstring test"};
    runTernaryOperationExecutorTest(expectedStr, unFlatValueVectorA, unFlatValueVectorB,
        unFlatValueVectorC, unFlatResultValueVector, unFlatValueVectorA);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorAllUnFlatAndFiltered) {
    vector<string> expectedStr = {"his is ", "bstring test"};
    setUnFlatDataChunkFiltered();
    runTernaryOperationExecutorTest(expectedStr, unFlatValueVectorA, unFlatValueVectorB,
        unFlatValueVectorC, unFlatResultValueVector, unFlatValueVectorA);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorFlatFlatUnFlatAndUnfiltered) {
    vector<string> expectedStr = {"his is ", "his is a l", "his is a lon"};
    runTernaryOperationExecutorTest(expectedStr, flatValueVectorA, flatValueVectorB,
        unFlatValueVectorC, unFlatResultValueVector, unFlatValueVectorC);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorFlatFlatUnFlatAndFiltered) {
    vector<string> expectedStr = {"his is ", "his is a lon"};
    setUnFlatDataChunkFiltered();
    runTernaryOperationExecutorTest(expectedStr, flatValueVectorA, flatValueVectorB,
        unFlatValueVectorC, unFlatResultValueVector, unFlatValueVectorC);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorFlatFlatUnFlatWithNull) {
    vector<string> expectedStr = {"", "", ""};
    flatValueVectorA->setNull(0, true);
    runTernaryOperationExecutorTest(expectedStr, flatValueVectorA, flatValueVectorB,
        unFlatValueVectorC, unFlatResultValueVector, unFlatValueVectorC);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorFlatUnFlatUnFlatAndUnfiltered) {
    vector<string> expectedStr = {"his is ", "This is a ", "is is a long"};
    runTernaryOperationExecutorTest(expectedStr, flatValueVectorA, unFlatValueVectorB,
        unFlatValueVectorC, unFlatResultValueVector, unFlatValueVectorC);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorFlatUnFlatUnFlatAndFiltered) {
    vector<string> expectedStr = {"his is ", "is is a long"};
    setUnFlatDataChunkFiltered();
    runTernaryOperationExecutorTest(expectedStr, flatValueVectorA, unFlatValueVectorB,
        unFlatValueVectorC, unFlatResultValueVector, unFlatValueVectorC);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorFlatUnFlatUnFlatWithNull) {
    vector<string> expectedStr = {"", "", ""};
    flatValueVectorA->setNull(0, true);
    runTernaryOperationExecutorTest(expectedStr, flatValueVectorA, unFlatValueVectorB,
        unFlatValueVectorC, unFlatResultValueVector, unFlatValueVectorC);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorFlatUnFlatFlatAndUnfiltered) {
    vector<string> expectedStr = {"his is ", "This is", "is is a"};
    runTernaryOperationExecutorTest(expectedStr, flatValueVectorA, unFlatValueVectorB,
        flatValueVectorC, unFlatResultValueVector, unFlatValueVectorB);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorFlatUnFlatFlatAndFiltered) {
    vector<string> expectedStr = {"his is ", "is is a"};
    setUnFlatDataChunkFiltered();
    runTernaryOperationExecutorTest(expectedStr, flatValueVectorA, unFlatValueVectorB,
        flatValueVectorC, unFlatResultValueVector, unFlatValueVectorB);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorFlatUnFlatFlatWithNull) {
    vector<string> expectedStr = {"", "", ""};
    flatValueVectorA->setNull(0, true);
    runTernaryOperationExecutorTest(expectedStr, flatValueVectorA, unFlatValueVectorB,
        flatValueVectorC, unFlatResultValueVector, unFlatValueVectorB);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorUnFlatFlatFlatAndUnfiltered) {
    vector<string> expectedStr = {"his is ", "his is ", "ubstrin"};
    runTernaryOperationExecutorTest(expectedStr, unFlatValueVectorA, flatValueVectorB,
        flatValueVectorC, unFlatResultValueVector, unFlatValueVectorA);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorUnFlatFlatFlatAndFiltered) {
    vector<string> expectedStr = {"his is ", "ubstrin"};
    setUnFlatDataChunkFiltered();
    runTernaryOperationExecutorTest(expectedStr, unFlatValueVectorA, flatValueVectorB,
        flatValueVectorC, unFlatResultValueVector, unFlatValueVectorA);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorUnFlatFlatFlatWithNull) {
    vector<string> expectedStr = {"", "", ""};
    flatValueVectorB->setNull(0, true);
    runTernaryOperationExecutorTest(expectedStr, unFlatValueVectorA, flatValueVectorB,
        flatValueVectorC, unFlatResultValueVector, unFlatValueVectorA);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorUnFlatFlatUnFlatAndUnfiltered) {
    vector<string> expectedStr = {"his is ", "his is ano", "ubstring tes"};
    runTernaryOperationExecutorTest(expectedStr, unFlatValueVectorA, flatValueVectorB,
        unFlatValueVectorC, unFlatResultValueVector, unFlatValueVectorA);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorUnFlatFlatUnFlatAndFiltered) {
    vector<string> expectedStr = {"his is ", "ubstring tes"};
    setUnFlatDataChunkFiltered();
    runTernaryOperationExecutorTest(expectedStr, unFlatValueVectorA, flatValueVectorB,
        unFlatValueVectorC, unFlatResultValueVector, unFlatValueVectorA);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorUnFlatFlatUnFlatWithNull) {
    vector<string> expectedStr = {"", "", ""};
    flatValueVectorB->setNull(0, true);
    runTernaryOperationExecutorTest(expectedStr, unFlatValueVectorA, flatValueVectorB,
        unFlatValueVectorC, unFlatResultValueVector, unFlatValueVectorA);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorUnFlatUnFlatFlatAndUnfiltered) {
    vector<string> expectedStr = {"his is ", "This is", "bstring"};
    runTernaryOperationExecutorTest(expectedStr, unFlatValueVectorA, unFlatValueVectorB,
        flatValueVectorC, unFlatResultValueVector, unFlatValueVectorA);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorUnFlatUnFlatFlatAndFiltered) {
    vector<string> expectedStr = {"his is ", "bstring"};
    setUnFlatDataChunkFiltered();
    runTernaryOperationExecutorTest(expectedStr, unFlatValueVectorA, unFlatValueVectorB,
        flatValueVectorC, unFlatResultValueVector, unFlatValueVectorA);
}

TEST_F(OperationExecutorTest, TernaryOperationExecutorUnFlatUnFlatFlatWithNull) {
    vector<string> expectedStr = {"", "", ""};
    flatValueVectorC->setNull(0, true);
    runTernaryOperationExecutorTest(expectedStr, unFlatValueVectorA, unFlatValueVectorB,
        flatValueVectorC, unFlatResultValueVector, unFlatValueVectorA);
}

TEST_F(OperationExecutorTest, NullOperationExecutorFlat) {
    vector<bool> expectedResult = {true};
    flatValueVectorA->setNull(0, true);
    NullOperationExecutor::execute<operation::IsNull>(*flatValueVectorA, *flatBoolValueVector);
    validateBoolResultValueVector(flatBoolValueVector, expectedResult);
}

TEST_F(OperationExecutorTest, NullOperationExecutorUnFlatUnfiltered) {
    vector<bool> expectedResult = {true, false, true};
    unFlatValueVectorA->setNull(0, true);
    unFlatValueVectorA->setNull(2, true);
    NullOperationExecutor::execute<operation::IsNull>(*unFlatValueVectorA, *unFlatBoolValueVector);
    validateBoolResultValueVector(unFlatBoolValueVector, expectedResult);
}

TEST_F(OperationExecutorTest, NullOperationExecutorUnFlatFiltered) {
    vector<bool> expectedResult = {true, false};
    setUnFlatDataChunkFiltered();
    unFlatValueVectorA->setNull(0, true);
    NullOperationExecutor::execute<operation::IsNull>(*unFlatValueVectorA, *unFlatBoolValueVector);
    validateBoolResultValueVector(unFlatBoolValueVector, expectedResult);
}

TEST_F(OperationExecutorTest, NullOperationSelect) {
    vector<bool> expectedResult = {true};
    flatValueVectorA->setNull(0, true);
    SelectionVector selVector(DEFAULT_VECTOR_CAPACITY);
    ASSERT_EQ(NullOperationExecutor::select<operation::IsNull>(*flatValueVectorA, selVector), true);
}

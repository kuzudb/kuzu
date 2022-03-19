#include "gtest/gtest.h"

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/function/hash/operations/include/hash_operations.h"
#include "src/function/include/unary_operation_executor.h"

using namespace graphflow::common;
using namespace graphflow::function;
using namespace std;

TEST(VectorHashNodeIDTests, nonSequenceNodeIDTest) {
    auto dataChunk = make_shared<DataChunk>(2);
    dataChunk->state->selectedSize = 1000;
    auto bufferManager = make_unique<BufferManager>();
    auto memoryManager = make_unique<MemoryManager>(bufferManager.get());

    auto nodeVector = make_shared<ValueVector>(memoryManager.get(), NODE);
    dataChunk->insert(0, nodeVector);
    auto nodeData = (nodeID_t*)nodeVector->values;

    auto result = make_shared<ValueVector>(memoryManager.get(), INT64);
    dataChunk->insert(1, result);
    auto resultData = (uint64_t*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < 1000; i++) {
        nodeData[i].label = 100;
        nodeData[i].offset = i * 10 + 1;
    }

    UnaryOperationExecutor::execute<nodeID_t, hash_t, operation::Hash>(*nodeVector, *result);
    for (int32_t i = 0; i < 1000; i++) {
        auto expected = operation::murmurhash64(i * 10 + 1) ^ operation::murmurhash64(100);
        ASSERT_EQ(resultData[i], expected);
    }

    // Set dataChunk to flat
    dataChunk->state->currIdx = 8;
    UnaryOperationExecutor::execute<nodeID_t, hash_t, operation::Hash>(*nodeVector, *result);
    auto expected = operation::murmurhash64(8 * 10 + 1) ^ operation::murmurhash64(100);
    auto pos = result->state->getPositionOfCurrIdx();
    ASSERT_EQ(resultData[pos], expected);
}

TEST(VectorHashNodeIDTests, sequenceNodeIDTest) {
    auto dataChunk = make_shared<DataChunk>(2);
    dataChunk->state->selectedSize = 1000;
    auto bufferManager = make_unique<BufferManager>();
    auto memoryManager = make_unique<MemoryManager>(bufferManager.get());

    auto nodeVector = make_shared<ValueVector>(memoryManager.get(), NODE);
    for (auto i = 0u; i < 1000; i++) {
        ((nodeID_t*)nodeVector->values)[i].label = 100;
        ((nodeID_t*)nodeVector->values)[i].offset = 10 + i;
    }
    dataChunk->insert(0, nodeVector);

    auto result = make_shared<ValueVector>(memoryManager.get(), INT64);
    dataChunk->insert(1, result);
    auto resultData = (uint64_t*)result->values;

    UnaryOperationExecutor::execute<nodeID_t, hash_t, operation::Hash>(*nodeVector, *result);
    for (int32_t i = 0; i < 1000; i++) {
        auto expected = operation::murmurhash64(10 + i) ^ operation::murmurhash64(100);
        ASSERT_EQ(resultData[i], expected);
    }

    // Set dataChunk to flat
    dataChunk->state->currIdx = 8;
    UnaryOperationExecutor::execute<nodeID_t, hash_t, operation::Hash>(*nodeVector, *result);
    auto expected = operation::murmurhash64(10 + 8) ^ operation::murmurhash64(100);
    auto pos = result->state->getPositionOfCurrIdx();
    ASSERT_EQ(resultData[pos], expected);
}

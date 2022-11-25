#include "common/data_chunk/data_chunk.h"
#include "function/hash/hash_operations.h"
#include "function/unary_operation_executor.h"
#include "gtest/gtest.h"

using namespace kuzu::common;
using namespace kuzu::function;
using namespace std;

TEST(VectorHashNodeIDTests, nonSequenceNodeIDTest) {
    auto dataChunk = make_shared<DataChunk>(2);
    dataChunk->state->selVector->selectedSize = 1000;
    auto bufferManager =
        make_unique<BufferManager>(StorageConfig::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING);
    auto memoryManager = make_unique<MemoryManager>(bufferManager.get());

    auto nodeVector = make_shared<ValueVector>(NODE_ID, memoryManager.get());
    dataChunk->insert(0, nodeVector);

    auto result = make_shared<ValueVector>(INT64, memoryManager.get());
    dataChunk->insert(1, result);

    // Fill values before the comparison.
    for (int32_t i = 0; i < 1000; i++) {
        nodeID_t nodeID{(node_offset_t)(i * 10 + 1), 100};
        nodeVector->setValue(i, nodeID);
    }

    UnaryOperationExecutor::execute<nodeID_t, hash_t, operation::Hash>(*nodeVector, *result);
    for (int32_t i = 0; i < 1000; i++) {
        auto expected = operation::murmurhash64(i * 10 + 1) ^ operation::murmurhash64(100);
        ASSERT_EQ(result->getValue<uint64_t>(i), expected);
    }

    // Set dataChunk to flat
    dataChunk->state->currIdx = 8;
    UnaryOperationExecutor::execute<nodeID_t, hash_t, operation::Hash>(*nodeVector, *result);
    auto expected = operation::murmurhash64(8 * 10 + 1) ^ operation::murmurhash64(100);
    auto pos = result->state->getPositionOfCurrIdx();
    ASSERT_EQ(result->getValue<uint64_t>(pos), expected);
}

TEST(VectorHashNodeIDTests, sequenceNodeIDTest) {
    auto dataChunk = make_shared<DataChunk>(2);
    dataChunk->state->selVector->selectedSize = 1000;
    auto bufferManager =
        make_unique<BufferManager>(StorageConfig::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING);
    auto memoryManager = make_unique<MemoryManager>(bufferManager.get());

    auto nodeVector = make_shared<ValueVector>(NODE_ID, memoryManager.get());
    for (auto i = 0u; i < 1000; i++) {
        nodeID_t nodeID{(node_offset_t)(10 + i), 100};
        nodeVector->setValue(i, nodeID);
    }
    dataChunk->insert(0, nodeVector);

    auto result = make_shared<ValueVector>(INT64, memoryManager.get());
    dataChunk->insert(1, result);

    UnaryOperationExecutor::execute<nodeID_t, hash_t, operation::Hash>(*nodeVector, *result);
    for (int32_t i = 0; i < 1000; i++) {
        auto expected = operation::murmurhash64(10 + i) ^ operation::murmurhash64(100);
        ASSERT_EQ(result->getValue<uint64_t>(i), expected);
    }

    // Set dataChunk to flat
    dataChunk->state->currIdx = 8;
    UnaryOperationExecutor::execute<nodeID_t, hash_t, operation::Hash>(*nodeVector, *result);
    auto expected = operation::murmurhash64(10 + 8) ^ operation::murmurhash64(100);
    auto pos = result->state->getPositionOfCurrIdx();
    ASSERT_EQ(result->getValue<uint64_t>(pos), expected);
}

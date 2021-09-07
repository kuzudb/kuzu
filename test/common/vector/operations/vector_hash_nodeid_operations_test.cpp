#include "gtest/gtest.h"

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/operations/hash_operations.h"
#include "src/common/include/vector/operations/vector_hash_operations.h"

using namespace graphflow::common;
using namespace std;

TEST(VectorHashNodeIDTests, nonSequenceNodeIDTest) {
    auto dataChunk = make_shared<DataChunk>(2);
    dataChunk->state->selectedSize = 1000;
    auto memoryManager = make_unique<MemoryManager>();

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

    VectorHashOperations::Hash(*nodeVector, *result);
    for (int32_t i = 0; i < 1000; i++) {
        auto expected = operation::murmurhash64(i * 10 + 1) ^ operation::murmurhash64(100);
        ASSERT_EQ(resultData[i], expected);
    }

    // Set dataChunk to flat
    dataChunk->state->currIdx = 8;
    VectorHashOperations::Hash(*nodeVector, *result);
    auto expected = operation::murmurhash64(8 * 10 + 1) ^ operation::murmurhash64(100);
    auto pos = result->state->getPositionOfCurrIdx();
    ASSERT_EQ(resultData[pos], expected);
}

TEST(VectorHashNodeIDTests, sequenceNodeIDTest) {
    auto dataChunk = make_shared<DataChunk>(2);
    dataChunk->state->selectedSize = 1000;
    auto memoryManager = make_unique<MemoryManager>();

    auto nodeVector = make_shared<ValueVector>(memoryManager.get(), NODE);
    for (auto i = 0u; i < 1000; i++) {
        ((nodeID_t*)nodeVector->values)[i].label = 100;
        ((nodeID_t*)nodeVector->values)[i].offset = 10 + i;
    }
    dataChunk->insert(0, nodeVector);

    auto result = make_shared<ValueVector>(memoryManager.get(), INT64);
    dataChunk->insert(1, result);
    auto resultData = (uint64_t*)result->values;

    VectorHashOperations::Hash(*nodeVector, *result);
    for (int32_t i = 0; i < 1000; i++) {
        auto expected = operation::murmurhash64(10 + i) ^ operation::murmurhash64(100);
        ASSERT_EQ(resultData[i], expected);
    }

    // Set dataChunk to flat
    dataChunk->state->currIdx = 8;
    VectorHashOperations::Hash(*nodeVector, *result);
    auto expected = operation::murmurhash64(10 + 8) ^ operation::murmurhash64(100);
    auto pos = result->state->getPositionOfCurrIdx();
    ASSERT_EQ(resultData[pos], expected);
}

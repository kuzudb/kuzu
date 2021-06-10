#include "gtest/gtest.h"

#include "src/common/include/data_chunk/data_chunk.h"
#include "src/common/include/operations/hash_operations.h"
#include "src/common/include/vector/node_vector.h"
#include "src/common/include/vector/operations/vector_node_id_operations.h"

using namespace graphflow::common;
using namespace std;

TEST(VectorHashNodeIDTests, nonSequenceNodeIDTest) {
    auto dataChunk = make_shared<DataChunk>();
    dataChunk->state->size = 1000;
    dataChunk->state->size = 1000;

    NodeIDCompressionScheme compressionScheme;
    auto nodeVector = make_shared<NodeIDVector>(100, compressionScheme, false);
    dataChunk->append(nodeVector);
    auto nodeData = (uint64_t*)nodeVector->values;

    auto result = make_shared<ValueVector>(INT64);
    dataChunk->append(result);
    auto resultData = (uint64_t*)result->values;

    // Fill values before the comparison.
    for (int32_t i = 0; i < 1000; i++) {
        nodeData[i] = i * 10 + 1;
    }

    VectorNodeIDOperations::Hash(*nodeVector, *result);
    for (int32_t i = 0; i < 1000; i++) {
        auto expected = operation::murmurhash64(i * 10 + 1) ^ operation::murmurhash64(100);
        ASSERT_EQ(resultData[i], expected);
    }

    // Set dataChunk to flat
    dataChunk->state->currPos = 8;
    VectorNodeIDOperations::Hash(*nodeVector, *result);
    auto expected = operation::murmurhash64(8 * 10 + 1) ^ operation::murmurhash64(100);
    auto pos = result->state->getCurrSelectedValuesPos();
    ASSERT_EQ(resultData[pos], expected);
}

TEST(VectorHashNodeIDTests, sequenceNodeIDTest) {
    auto dataChunk = make_shared<DataChunk>();
    dataChunk->state->size = 1000;
    dataChunk->state->size = 1000;

    label_t commonLabel = 100;
    NodeIDCompressionScheme compressionScheme;
    auto nodeVector = make_shared<NodeIDVector>(commonLabel, compressionScheme, true);
    nodeVector->setStartOffset(10);
    dataChunk->append(nodeVector);

    auto result = make_shared<ValueVector>(INT64);
    dataChunk->append(result);
    auto resultData = (uint64_t*)result->values;

    VectorNodeIDOperations::Hash(*nodeVector, *result);
    for (int32_t i = 0; i < 1000; i++) {
        auto expected = operation::murmurhash64(10 + i) ^ operation::murmurhash64(100);
        ASSERT_EQ(resultData[i], expected);
    }

    // Set dataChunk to flat
    dataChunk->state->currPos = 8;
    VectorNodeIDOperations::Hash(*nodeVector, *result);
    auto expected = operation::murmurhash64(10 + 8) ^ operation::murmurhash64(100);
    auto pos = result->state->getCurrSelectedValuesPos();
    ASSERT_EQ(resultData[pos], expected);
}

#include "gtest/gtest.h"

#include "src/common/include/operations/hash_operations.h"
#include "src/common/include/vector/node_vector.h"

using namespace graphflow::common;
using namespace std;

TEST(VectorHashNodeIDTests, nonSequenceNodeIDTest) {
    auto dataChunk = make_shared<DataChunk>();
    dataChunk->size = 1000;

    NodeIDCompressionScheme compressionScheme;
    auto nodeVector = NodeIDVector(100, compressionScheme);
    nodeVector.setDataChunkOwner(dataChunk);
    auto nodeData = (uint64_t*)nodeVector.getValues();

    auto result = ValueVector(INT64);
    auto resultData = (uint64_t*)result.getValues();

    // Fill values before the comparison.
    for (int32_t i = 0; i < 1000; i++) {
        nodeData[i] = i * 10 + 1;
    }

    auto hashNodeIDOp = ValueVector::getUnaryOperation(HASH_NODE_ID);
    hashNodeIDOp(nodeVector, result);
    for (int32_t i = 0; i < 1000; i++) {
        auto expected = operation::murmurhash64(i * 10 + 1) ^ operation::murmurhash64(100);
        ASSERT_EQ(resultData[i], expected);
    }

    // Set dataChunk to flat
    dataChunk->currPos = 8;
    hashNodeIDOp(nodeVector, result);
    auto expected = operation::murmurhash64(8 * 10 + 1) ^ operation::murmurhash64(100);
    ASSERT_EQ(resultData[0], expected);
}

TEST(VectorHashNodeIDTests, sequenceNodeIDTest) {
    auto dataChunk = make_shared<DataChunk>();
    dataChunk->size = 1000;

    label_t commonLable = 100;
    auto nodeVector = NodeIDSequenceVector(commonLable);
    nodeVector.setStartOffset(10);
    nodeVector.setDataChunkOwner(dataChunk);

    auto result = ValueVector(INT64);
    auto resultData = (uint64_t*)result.getValues();

    auto hashNodeIDOp = ValueVector::getUnaryOperation(HASH_NODE_ID);
    hashNodeIDOp(nodeVector, result);
    for (int32_t i = 0; i < 1000; i++) {
        auto expected = operation::murmurhash64(10 + i) ^ operation::murmurhash64(100);
        ASSERT_EQ(resultData[i], expected);
    }

    // Set dataChunk to flat
    dataChunk->currPos = 8;
    hashNodeIDOp(nodeVector, result);
    auto expected = operation::murmurhash64(10 + 8) ^ operation::murmurhash64(100);
    ASSERT_EQ(resultData[0], expected);
}

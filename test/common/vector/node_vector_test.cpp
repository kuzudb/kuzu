#include "gtest/gtest.h"

#include "src/common/include/vector/node_vector.h"

TEST(ListTests, CreateNodeVectorTest) {
    auto vector = NodeVector(CompressionScheme::LABEL_0_NODEOFFSET_8_BYTES);
    for (node_offset_t i = 0; i < VECTOR_SIZE; i++) {
        vector.set(i, i * 2 + 3);
    }
    node_t node;
    for (uint64_t i = 0; i < VECTOR_SIZE; i++) {
        vector.get(i, node);
        ASSERT_EQ(node.nodeOffset, i * 2 + 3);
    }
}

TEST(ListTests, CreateSequenceNodeVectorTest) {
    auto vector = NodeSequenceVector();
    vector.set(100);
    node_t node;
    for (uint64_t i = 0; i < VECTOR_SIZE; i++) {
        vector.get(i, node);
        ASSERT_EQ(node.nodeOffset, 100 + i);
    }
}

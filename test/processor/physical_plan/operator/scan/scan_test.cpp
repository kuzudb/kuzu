#include "gtest/gtest.h"

#include "src/processor/include/physical_plan/operator/scan_node_id/scan_node_id.h"

using namespace graphflow::processor;

TEST(ScanTests, ScanTest) {
    auto morsel = make_shared<MorselsDesc>(0, 1025013 /*numNodes*/);
    auto scan = make_unique<ScanNodeID<true>>(morsel);
    auto resultSet = scan->getResultSet();
    auto dataChunk = resultSet->dataChunks[0];
    auto nodeVector = static_pointer_cast<NodeIDVector>(dataChunk->getValueVector(0));
    node_offset_t currNodeOffset = 0;
    auto size = NODE_SEQUENCE_VECTOR_SIZE;
    while (morsel->currNodeOffset < 1025013) {
        scan->getNextTuples();
        if (morsel->currNodeOffset >= 1025013) {
            size = 1025013 % NODE_SEQUENCE_VECTOR_SIZE;
        }
        ASSERT_EQ(dataChunk->state->size, size);
        nodeID_t node;
        for (uint64_t i = 0; i < dataChunk->state->size; i++) {
            nodeVector->readNodeOffset(i, node);
            ASSERT_EQ(node.offset, currNodeOffset + i);
        }
        currNodeOffset += NODE_SEQUENCE_VECTOR_SIZE;
    }
    ASSERT_EQ(morsel->currNodeOffset, 1025013);
}

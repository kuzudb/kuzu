#include "gtest/gtest.h"

#include "src/processor/include/physical_plan/operator/scan/physical_scan.h"

using namespace graphflow::processor;

void testScan(unique_ptr<PhysicalScan>& scan, shared_ptr<MorselDesc>& morsel,
    node_offset_t max_offset, label_t label);

TEST(ScanTests, ScanTest) {
    auto morsel = make_shared<MorselDesc>(1025013 /*numNodes*/);
    auto scan = make_unique<PhysicalScan>(morsel);
    auto dataChunks = scan->getDataChunks();
    shared_ptr<DataChunk> dataChunk = dataChunks->getDataChunk(0);
    auto nodeVector =
        static_pointer_cast<NodeIDVector>(dataChunks->getDataChunk(0)->getValueVector(0));
    node_offset_t currNodeOffset = 0;
    uint64_t size = ValueVector::NODE_SEQUENCE_VECTOR_SIZE;
    while (morsel->getCurrNodeOffset() < 1025013) {
        ASSERT_EQ(scan->hasNextMorsel(), true);
        scan->getNextTuples();
        if (morsel->getCurrNodeOffset() >= 1025013) {
            size = 1025013 % ValueVector::NODE_SEQUENCE_VECTOR_SIZE;
        }
        ASSERT_EQ(dataChunk->size, size);
        nodeID_t node;
        for (uint64_t i = 0; i < dataChunk->size; i++) {
            nodeVector->readNodeOffset(i, node);
            ASSERT_EQ(node.offset, currNodeOffset + i);
        }
        currNodeOffset += ValueVector::NODE_SEQUENCE_VECTOR_SIZE;
    }
    ASSERT_EQ(morsel->getCurrNodeOffset(), 1025013);
    ASSERT_EQ(scan->hasNextMorsel(), false);
}

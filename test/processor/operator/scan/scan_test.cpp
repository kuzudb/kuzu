#include "gtest/gtest.h"

#include "src/processor/include/operator/scan/scan.h"

using namespace graphflow::processor;

TEST(ScanTests, ScanSingleLabelTest) {
    auto morsel = make_shared<MorselDescSingleLabelNodeIDs>(1, 1025012);
    auto scan = make_unique<ScanSingleLabel>("a", morsel);
    scan->initialize(NULL /* graph */);

    auto dataChunks = scan->getOutDataChunks();
    shared_ptr<DataChunk> dataChunk;
    auto nodeVector = static_pointer_cast<NodeIDVector>(
        dataChunks->getValueVectorAndSetDataChunk("a", dataChunk));
    node_offset_t currNodeOffset = 0;
    uint64_t size = ValueVector::NODE_SEQUENCE_VECTOR_SIZE;
    while (morsel->currNodeOffset < 1025012) {
        ASSERT_EQ(scan->hasNextMorsel(), true);
        scan->getNextTuples();
        if (morsel->currNodeOffset == 1025012) {
            size = 1025012 % ValueVector::NODE_SEQUENCE_VECTOR_SIZE + 1;
        }
        ASSERT_EQ(dataChunk->size, size);
        ASSERT_EQ(nodeVector->getLabel(), 1);
        nodeID_t node;
        for (uint64_t i = 0; i < dataChunk->size; i++) {
            nodeVector->readValue(i, node);
            ASSERT_EQ(node.offset, currNodeOffset + i);
        }
        currNodeOffset += ValueVector::NODE_SEQUENCE_VECTOR_SIZE;
    }

    // morsel->currNodeOffset == 1025012.
    ASSERT_EQ(morsel->currNodeOffset, 1025012);
    ASSERT_EQ(scan->hasNextMorsel(), false);
}

void testScanMultiLabel(unique_ptr<ScanMultiLabel>& scan,
    shared_ptr<MorselDescMultiLabelNodeIDs>& morsel, node_offset_t max_offset, label_t label);

TEST(ScanTests, ScanMultiLabelTest) {
    auto morsel = make_shared<MorselDescMultiLabelNodeIDs>(3 /*num_labels*/);
    morsel->nodeLabel[0] = 1;
    morsel->nodeLabel[1] = 5;
    morsel->nodeLabel[2] = 17;
    morsel->maxNodeOffset[0] = 123456789;
    morsel->maxNodeOffset[1] = 666;
    morsel->maxNodeOffset[2] = 90909090;

    auto scan = make_unique<ScanMultiLabel>("a", morsel);
    scan->initialize(NULL /* graph */);
    testScanMultiLabel(scan, morsel, 123456789, 1);
    testScanMultiLabel(scan, morsel, 666, 5);
    testScanMultiLabel(scan, morsel, 90909090, 17);
}

void testScanMultiLabel(unique_ptr<ScanMultiLabel>& scan,
    shared_ptr<MorselDescMultiLabelNodeIDs>& morsel, node_offset_t max_offset, label_t label) {
    auto dataChunks = scan->getOutDataChunks();
    shared_ptr<DataChunk> dataChunk;
    auto nodeVector = static_pointer_cast<NodeIDVector>(
        dataChunks->getValueVectorAndSetDataChunk("a", dataChunk));
    node_offset_t currNodeOffset = 0;
    uint64_t size = ValueVector::NODE_SEQUENCE_VECTOR_SIZE;
    nodeID_t node;
    while (morsel->currNodeOffset < max_offset) {
        scan->hasNextMorsel();
        scan->getNextTuples();
        if (morsel->currNodeOffset == max_offset) {
            size = max_offset % ValueVector::NODE_SEQUENCE_VECTOR_SIZE + 1;
        }
        ASSERT_EQ(dataChunk->size, size);
        ASSERT_EQ(nodeVector->getLabel(), label);
        nodeVector->readValue(0, node);
        ASSERT_EQ(node.offset, currNodeOffset);
        currNodeOffset += ValueVector::NODE_SEQUENCE_VECTOR_SIZE;
    }
}

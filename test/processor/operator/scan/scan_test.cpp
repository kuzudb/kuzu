#include <memory>

#include "gtest/gtest.h"

#include "src/processor/include/operator/scan/scan.h"

using namespace graphflow::processor;

TEST(ScanTests, ScanSingleLabelTest) {
    auto morsel = new MorselSequenceDesc(1, 1025012);
    auto scan = new ScanSingleLabel();
    scan->initialize(NULL /* graph */, morsel);

    auto dataChunk = scan->getDataChunks()[0];
    auto nodeVector = scan->getNodeVector();
    node_offset_t currNodeOffset = 0;
    uint64_t size = VECTOR_SIZE;
    while (morsel->currNodeOffset < 1025012) {
        scan->getNextMorsel();
        scan->getNextTuples();
        if (morsel->currNodeOffset == 1025012) {
            size = 1025012 % VECTOR_SIZE + 1;
        }
        ASSERT_EQ(dataChunk->size, size);
        ASSERT_EQ(nodeVector->getLabel(), 1);
        node_t node;
        for (uint64_t i = 0; i < dataChunk->size; i++) {
            nodeVector->get(i, node);
            ASSERT_EQ(node.nodeOffset, currNodeOffset + i);
        }
        currNodeOffset += VECTOR_SIZE;
    }
    scan->getNextMorsel();
    ASSERT_EQ(dataChunk->size, 0);

    delete morsel;
}

void testScanMultiLabel(ScanMultiLabel* scan, MorselMultiLabelSequenceDesc* morsel,
    node_offset_t max_offset, label_t label);

TEST(ScanTests, ScanMultiLabelTest) {
    auto morsel = new MorselMultiLabelSequenceDesc(3 /*num_labels*/);
    morsel->nodeLabel[0] = 1;
    morsel->nodeLabel[1] = 5;
    morsel->nodeLabel[2] = 17;
    morsel->maxNodeOffset[0] = 123456789;
    morsel->maxNodeOffset[1] = 666;
    morsel->maxNodeOffset[2] = 90909090;

    auto scan = new ScanMultiLabel();
    scan->initialize(NULL /* graph */, morsel);
    testScanMultiLabel(scan, morsel, 123456789, 1);
    testScanMultiLabel(scan, morsel, 666, 5);
    testScanMultiLabel(scan, morsel, 90909090, 17);

    delete morsel;
}

void testScanMultiLabel(ScanMultiLabel* scan, MorselMultiLabelSequenceDesc* morsel,
    node_offset_t max_offset, label_t label) {
    auto dataChunk = scan->getDataChunks()[0];
    auto nodeVector = scan->getNodeVector();
    node_offset_t currNodeOffset = 0;
    uint64_t size = VECTOR_SIZE;
    node_t node;
    while (morsel->currNodeOffset < max_offset) {
        scan->getNextMorsel();
        scan->getNextTuples();
        if (morsel->currNodeOffset == max_offset) {
            size = max_offset % VECTOR_SIZE + 1;
        }
        ASSERT_EQ(dataChunk->size, size);
        ASSERT_EQ(nodeVector->getLabel(), label);
        nodeVector->get(0, node);
        ASSERT_EQ(node.nodeOffset, currNodeOffset);
        currNodeOffset += VECTOR_SIZE;
    }
}

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
    while (morsel->getCurrNodeOffset() < 1025012) {
        ASSERT_EQ(scan->hasNextMorsel(), true);
        scan->getNextTuples();
        if (morsel->getCurrNodeOffset() == 1025012) {
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
    ASSERT_EQ(morsel->getCurrNodeOffset(), 1025012);
    ASSERT_EQ(scan->hasNextMorsel(), false);
}

void testScanMultiLabel(unique_ptr<ScanMultiLabel>& scan,
    shared_ptr<MorselDescMultiLabelNodeIDs>& morsel, node_offset_t max_offset, label_t label);

TEST(ScanTests, ScanMultiLabelTest) {
    cout << "beg" << endl;
    auto morsel = make_shared<MorselDescMultiLabelNodeIDs>();
    cout << "obj c" << endl;
    morsel->addLabel(1, 123456789);
    cout << "a1" << endl;
    morsel->addLabel(5, 666);
    cout << "a2" << endl;
    morsel->addLabel(17, 90909090);
    cout << "a3" << endl;

    cout << "before test - 2" << endl;
    auto scan = make_unique<ScanMultiLabel>("a", morsel);
    cout << "before test - 1" << endl;
    scan->initialize(NULL /* graph */);
    cout << "before test" << endl;
    testScanMultiLabel(scan, morsel, 123456789, 1);
    cout << "1d" << endl;
    testScanMultiLabel(scan, morsel, 666, 5);
    cout << "2d" << endl;
    testScanMultiLabel(scan, morsel, 90909090, 17);
    cout << "3d" << endl;
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
    while (morsel->getCurrNodeOffset() < max_offset) {
        scan->hasNextMorsel();
        scan->getNextTuples();
        if (morsel->getCurrNodeOffset() == max_offset) {
            size = max_offset % ValueVector::NODE_SEQUENCE_VECTOR_SIZE + 1;
        }
        ASSERT_EQ(dataChunk->size, size);
        ASSERT_EQ(nodeVector->getLabel(), label);
        nodeVector->readValue(0, node);
        ASSERT_EQ(node.offset, currNodeOffset);
        currNodeOffset += ValueVector::NODE_SEQUENCE_VECTOR_SIZE;
    }
}

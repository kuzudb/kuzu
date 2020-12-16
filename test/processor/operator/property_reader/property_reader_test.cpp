#include <fstream>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include "src/processor/include/operator/property_reader/property_reader.h"
#include "src/processor/include/operator/scan/scan.h"

using namespace graphflow::processor;

#define NUM_PAGES 3

class PropertyReaderIntegerTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto f = ofstream("ColEvenIntegersFile", ios_base::out | ios_base::binary);
        int32_t val = 0;
        for (auto pageId = 0; pageId < NUM_PAGES; pageId++) {
            for (auto i = 0u; i < PAGE_SIZE / sizeof(int32_t); i++) {
                f.write((char*)&val, sizeof(int32_t));
                val += 2;
            }
        }
        f.close();
    }

    void TearDown() override {
        auto fname = "ColEvenIntegersFile";
        remove(fname);
    }
};

class GraphStub : public Graph {
public:
    GraphStub(string columnName, int numPages) : columnName(columnName) {
        bufferManager = make_unique<BufferManager>(PAGE_SIZE * numPages);
    }

    BaseColumn* getColumn(label_t label, uint64_t propertyIdx) {
        auto numElements = 30 * (PAGE_SIZE / sizeof(int32_t));
        return new PropertyColumnInt(columnName, numElements, *bufferManager.get());
    }

protected:
    string columnName;
    unique_ptr<BufferManager> bufferManager;
};

class ScanStub : public ScanSingleLabel {
public:
    ScanStub(node_offset_t startNodeOffset) { this->startNodeOffset = startNodeOffset; }

    bool getNextMorsel() { return true; }

    void getNextTuples() {
        nodeIDVector->setStartOffset(startNodeOffset);
        outDataChunk->size = NODE_SEQUENCE_VECTOR_SIZE;
    }

private:
    node_offset_t startNodeOffset;
};

void testPropertyReaderNodeSameLabel(int32_t startExpectedValue);

TEST_F(PropertyReaderIntegerTest, PropertyReaderSameLabelSetPointerTest) {
    //testPropertyReaderNodeSameLabel(0);
}

TEST_F(PropertyReaderIntegerTest, PropertyReaderSameLabelCopyTest) {
    testPropertyReaderNodeSameLabel(200);
}

void testPropertyReaderNodeSameLabel(int32_t startExpectedValue) {
    auto reader = make_unique<StructuredNodePropertyReader<int32_t>>(
        0 /*label*/, 0 /*propertyIdx*/, 0 /*nodeVectordx*/, 0 /*dataChunkIdx*/);
    reader->setPrevOperator(new ScanStub(startExpectedValue / 2));
    auto graph = make_unique<GraphStub>("ColEvenIntegersFile", NUM_PAGES);
    auto morsel = make_shared<MorselDescSingleLabelNodeIDs>(0, 1024);
    auto baseMorsel = static_pointer_cast<MorselDesc>(morsel);
    reader->initialize(graph.get(), baseMorsel);
    ASSERT_EQ(reader->getNextMorsel(), true);
    reader->getNextTuples();
    auto propertyVector = reader->getPropertyVector();
    int32_t actualValue;
    for (uint64_t i = 0; i < 1024; i++) {
        propertyVector->get(i, actualValue);
        ASSERT_EQ(actualValue, startExpectedValue);
        startExpectedValue += 2;
    }
    reader->cleanup();
}

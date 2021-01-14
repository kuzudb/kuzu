#include <fstream>

#include "gtest/gtest.h"
#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

#include "src/processor/include/operator/physical/column_reader/node_property_column_reader.h"
#include "src/processor/include/operator/physical/scan/physical_scan.h"
#include "src/storage/include/structures/column.h"

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
        spdlog::shutdown();
    }
};

class GraphStub : public Graph {

public:
    GraphStub(string columnName, int numPages) : columnName(columnName) {
        bufferManager = make_unique<BufferManager>(PAGE_SIZE * numPages);
        auto numElements = 30 * (PAGE_SIZE / sizeof(int32_t));
        column = make_unique<PropertyColumnInt>(columnName, numElements, *bufferManager.get());
    }

    BaseColumn* getNodePropertyColumn(
        const label_t& nodeLabel, const string& propertyName) const override {
        return column.get();
    }

protected:
    unique_ptr<BufferManager> bufferManager;
    string columnName;
    unique_ptr<BaseColumn> column;
};

class ScanStub : public PhysicalScan {

public:
    ScanStub(node_offset_t startNodeOffset, shared_ptr<MorselDesc>& morsel) : PhysicalScan(morsel) {
        this->startNodeOffset = startNodeOffset;
    }

    bool hasNextMorsel() { return true; }

    void getNextTuples() {
        nodeIDVector->setStartOffset(startNodeOffset);
        outDataChunk->size = ValueVector::NODE_SEQUENCE_VECTOR_SIZE;
    }

private:
    node_offset_t startNodeOffset;
};

void testPropertyReaderNodeSameLabel(int32_t startExpectedValue);

TEST_F(PropertyReaderIntegerTest, PropertyReaderSameLabelSetPointerTest) {
    testPropertyReaderNodeSameLabel(0);
}

TEST_F(PropertyReaderIntegerTest, PropertyReaderSameLabelCopyTest) {
    testPropertyReaderNodeSameLabel(200);
}

void testPropertyReaderNodeSameLabel(int32_t startExpectedValue) {
    auto morsel = make_shared<MorselDesc>(1024);
    unique_ptr<Operator> scanStub = make_unique<ScanStub>(startExpectedValue / 2, morsel);
    auto graph = make_unique<GraphStub>("ColEvenIntegersFile", NUM_PAGES);
    auto column = graph->getNodePropertyColumn(0 /*label*/, "property");
    auto reader = make_unique<NodePropertyColumnReader>(
        0 /*inDataChunkIdx*/, 0 /*inValueVectorIdx*/, column, move(scanStub));
    ASSERT_EQ(reader->hasNextMorsel(), true);
    reader->getNextTuples();
    auto values = reader->getDataChunks()
                      ->getValueVector(0 /*inDataChunkIdx*/, 1 /*inValueVectorIdx*/)
                      ->getValues();
    int32_t actualValue;
    for (uint64_t i = 0; i < 1024; i++) {
        memcpy(&actualValue, (void*)(values + i * sizeof(int32_t)), sizeof(int32_t));
        ASSERT_EQ(actualValue, startExpectedValue);
        startExpectedValue += 2;
    }
    reader->cleanup();
}

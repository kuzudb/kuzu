#include "gtest/gtest.h"

#include "src/processor/include/operator/scan/scan.h"
#include "src/processor/include/processor.h"

using namespace graphflow::processor;

class ProcessorTest : public ::testing::Test {

protected:
    void SetUp() override {}

    void TearDown() override { spdlog::shutdown(); }
};

class GraphStub : public Graph {

public:
    GraphStub() {}
};

TEST(ProcessorTests, MultiThreadedScanTest) {
    unique_ptr<Graph> graph = make_unique<GraphStub>();
    auto morsel = make_shared<MorselDescSingleLabelNodeIDs>(1, 1025012);
    auto plan = make_unique<QueryPlan>(new Sink(new ScanSingleLabel("a", morsel)));
    auto processor = make_unique<QueryProcessor>(*graph, 10);
    auto result = processor->execute(plan, 3);
    ASSERT_EQ(result->first.getNumOutputTuples(), 1025012 /* max_offset */ + 1);
}

#include "gtest/gtest.h"

#include "src/processor/include/physical_plan/operator/scan_node_id/scan_node_id.h"
#include "src/processor/include/physical_plan/operator/sink/result_collector.h"
#include "src/processor/include/processor.h"

using namespace graphflow::processor;

class ProcessorTest : public ::testing::Test {

protected:
    void SetUp() override {}

    void TearDown() override { spdlog::shutdown(); }
};

class GraphStub : public Graph {

public:
    GraphStub() : Graph() {}
};

TEST(ProcessorTests, MultiThreadedScanTest) {
    unique_ptr<Graph> graph = make_unique<GraphStub>();
    auto morsel = make_shared<MorselsDesc>(0, 1025013 /*numNodes*/);
    auto plan = make_unique<PhysicalPlan>(
        make_unique<ResultCollector>(make_unique<ScanNodeID<true>>(morsel)));
    auto processor = make_unique<QueryProcessor>(10);
    auto executionContext = ExecutionContext(move(plan), 1);
    auto result = processor->execute(executionContext);
    ASSERT_EQ(result->numTuples, 1025013);
}

#include "gtest/gtest.h"

#include "src/processor/include/physical_plan/operator/scan_node_id/scan_node_id.h"
#include "src/processor/include/physical_plan/operator/sink/result_collector.h"
#include "src/processor/include/processor.h"
#include "src/storage/include/graph.h"

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
    auto profiler = make_unique<Profiler>();
    auto executionContext = ExecutionContext(*profiler, nullptr);
    auto plan = make_unique<PhysicalPlan>(
        make_unique<ResultCollector>(make_unique<ScanNodeID<true>>(morsel, executionContext, 0),
            RESULT_COLLECTOR, executionContext, 1));
    auto processor = make_unique<QueryProcessor>(10);
    auto result = processor->execute(plan.get(), 1);
    ASSERT_EQ(result->numTuples, 1025013);
}

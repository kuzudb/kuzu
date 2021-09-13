#include "gtest/gtest.h"

#include "src/processor/include/physical_plan/operator/scan_node_id/scan_node_id.h"
#include "src/processor/include/physical_plan/operator/sink/result_collector.h"
#include "src/processor/include/processor.h"
#include "src/storage/include/graph.h"

using namespace graphflow::processor;

class ProcessorTest : public ::testing::Test {

protected:
    void SetUp() override {}

    void TearDown() override {}
};

class GraphStub : public Graph {

public:
    GraphStub() : Graph() {}
};

TEST(ProcessorTests, MultiThreadedScanTest) {
    unique_ptr<Graph> graph = make_unique<GraphStub>();
    auto morsel = make_shared<MorselsDesc>(0, 1025013 /*numNodes*/);
    auto profiler = make_unique<Profiler>();
    auto memoryManager = make_unique<MemoryManager>();
    auto executionContext = ExecutionContext(*profiler, nullptr, memoryManager.get());
    auto resultSet = make_shared<ResultSet>(1);
    resultSet->dataChunks[0] = make_shared<DataChunk>(1);
    auto plan = make_unique<PhysicalPlan>(make_unique<ResultCollector>(move(resultSet),
        make_unique<ScanNodeID>(DataPos{0, 0}, morsel, executionContext, 0), RESULT_COLLECTOR,
        executionContext, 1, false));
    auto processor = make_unique<QueryProcessor>(10);
    auto result = processor->execute(plan.get(), 1);
    ASSERT_EQ(result->numTuples, 1025013);
}

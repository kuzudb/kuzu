#include "gtest/gtest.h"

#include "src/common/include/configs.h"
#include "src/processor/include/physical_plan/operator/result_collector.h"
#include "src/processor/include/physical_plan/operator/scan_node_id.h"
#include "src/processor/include/processor.h"
#include "src/storage/include/storage_manager.h"

using namespace graphflow::processor;

class ProcessorTest : public ::testing::Test {

protected:
    void SetUp() override {}

    void TearDown() override {}
};

TEST(ProcessorTests, MultiThreadedScanTest) {
    auto sharedState = make_shared<ScanNodeIDSharedState>(1025013 /*numNodes*/);
    auto profiler = make_unique<Profiler>();
    auto bufferManager =
        make_unique<BufferManager>(512 * DEFAULT_PAGE_SIZE /* maxSizeForDefaultPagePool */,
            1ull << 25 /* maxSizeForLargePagePool */);
    auto memoryManager = make_unique<MemoryManager>(bufferManager.get());
    auto executionContext = make_unique<ExecutionContext>(
        1 /* numThreads */, profiler.get(), memoryManager.get(), bufferManager.get());
    auto schema = Schema();
    auto group1Pos = schema.createGroup();
    schema.insertToGroupAndScope(
        make_shared<Expression>(VARIABLE, DataType(INT64), "dummy"), group1Pos);
    auto aIDPos = DataPos{0, 0};
    auto vectorsToCollectInfo = vector<pair<DataPos, bool>>{make_pair(aIDPos, false)};
    auto plan = make_unique<PhysicalPlan>(
        make_unique<ResultCollector>(vectorsToCollectInfo, make_shared<FTableSharedState>(),
            make_unique<ScanNodeID>(make_unique<ResultSetDescriptor>(schema), 0, aIDPos,
                sharedState, 0, "" /* paramsString */),
            1, "" /* paramsString */),
        true /* readOnly */);
    auto processor = make_unique<QueryProcessor>(10);
    auto result = processor->execute(plan.get(), executionContext.get());
    ASSERT_EQ(result->getTotalNumFlatTuples(), 1025013);
}

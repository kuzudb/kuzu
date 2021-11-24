#include "gtest/gtest.h"

#include "src/processor/include/physical_plan/operator/scan_node_id.h"

using namespace graphflow::processor;

TEST(ScanTests, ScanTest) {
    auto morsel = make_shared<MorselsDesc>(1025013 /*numNodes*/);
    auto profiler = make_unique<Profiler>();
    auto memoryManager = make_unique<MemoryManager>();
    auto executionContext = ExecutionContext(*profiler, memoryManager.get());
    auto scan = make_unique<ScanNodeID>(0, DataPos{0, 0}, morsel, executionContext, 0);
    auto resultSet = make_shared<ResultSet>(1);
    resultSet->dataChunks[0] = make_shared<DataChunk>(1);
    scan->initResultSet(resultSet);
    auto dataChunk = resultSet->dataChunks[0];
    auto nodeVector = dataChunk->getValueVector(0);
    node_offset_t currNodeOffset = 0;
    auto size = DEFAULT_VECTOR_CAPACITY;
    while (morsel->currentOffset < 1025013) {
        scan->getNextTuples();
        if (morsel->currentOffset >= 1025013) {
            size = 1025013 % DEFAULT_VECTOR_CAPACITY;
        }
        ASSERT_EQ(dataChunk->state->selectedSize, size);
        auto startNodeOffset = ((nodeID_t*)nodeVector->values)[0].offset;
        for (uint64_t i = 0; i < dataChunk->state->selectedSize; i++) {
            ASSERT_EQ(startNodeOffset + i, currNodeOffset + i);
        }
        currNodeOffset += DEFAULT_VECTOR_CAPACITY;
    }
    ASSERT_EQ(morsel->currentOffset, 1025013);
}

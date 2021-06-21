#include "gtest/gtest.h"

#include "src/processor/include/physical_plan/operator/scan_node_id/scan_node_id.h"

using namespace graphflow::processor;

TEST(ScanTests, ScanTest) {
    auto morsel = make_shared<MorselsDesc>(0, 1025013 /*numNodes*/);
    auto profiler = make_unique<Profiler>();
    auto memoryManager = make_unique<MemoryManager>();
    auto executionContext = ExecutionContext(*profiler, nullptr, memoryManager.get());
    auto scan = make_unique<ScanNodeID<true>>(morsel, executionContext, 0);
    auto resultSet = scan->getResultSet();
    auto dataChunk = resultSet->dataChunks[0];
    auto nodeVector = static_pointer_cast<NodeIDVector>(dataChunk->getValueVector(0));
    node_offset_t currNodeOffset = 0;
    auto size = NODE_SEQUENCE_VECTOR_CAPACITY;
    while (morsel->currNodeOffset < 1025013) {
        scan->getNextTuples();
        if (morsel->currNodeOffset >= 1025013) {
            size = 1025013 % NODE_SEQUENCE_VECTOR_CAPACITY;
        }
        ASSERT_EQ(dataChunk->state->size, size);
        for (uint64_t i = 0; i < dataChunk->state->size; i++) {
            auto nodeOffset = nodeVector->readNodeOffset(i);
            ASSERT_EQ(nodeOffset, currNodeOffset + i);
        }
        currNodeOffset += NODE_SEQUENCE_VECTOR_CAPACITY;
    }
    ASSERT_EQ(morsel->currNodeOffset, 1025013);
}

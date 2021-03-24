#include "data_chunks_mock_operator.h"
#include "gtest/gtest.h"

#include "src/processor/include/memory_manager.h"
#include "src/processor/include/physical_plan/operator/hash_join/hash_join.h"
#include "src/processor/include/physical_plan/operator/tuple/data_chunks_iterator.h"
#include "src/processor/include/physical_plan/operator/tuple/tuple.h"

/*
TEST(HashJoinTests, HashJoinTest1) {
    MemoryManager memoryManager(1024 * 1024 * 1024);

    auto buildOp = make_unique<ScanMockOp>();
    auto probeOp = make_unique<ProbeScanMockOp>();

    unique_ptr<HashJoin> hashJoin =
        make_unique<HashJoin>(memoryManager, 0, 0, 0, 0, move(buildOp), move(probeOp));
    uint64_t matchedNumTuples = 0;
    while (true) {
        hashJoin->getNextTuples();
        auto result = hashJoin->getDataChunks();
        matchedNumTuples += result->getNumTuples();
        if (result->getNumTuples() == 0) {
            break;
        }
        ASSERT_EQ(result->getNumTuples(), 100);
        vector<DataType> types;
        for (uint64_t i = 0; i < result->getNumDataChunks(); i++) {
            for (auto& vector : result->getDataChunk(i)->valueVectors) {
                types.push_back(vector->getDataType());
            }
        }
        Tuple tuple(types);
        DataChunksIterator dataChunksIterator(*result);
        uint64_t tupleIndex = 0;
        while (dataChunksIterator.hasNextTuple()) {
            dataChunksIterator.getNextTuple(tuple);
            ASSERT_EQ(tuple.getValue(0)->nodeID.label, 18);
            ASSERT_EQ(tuple.getValue(0)->nodeID.offset, 0);
            ASSERT_EQ(tuple.getValue(1)->primitive.integer, 0);
            ASSERT_EQ(tuple.getValue(2)->primitive.integer, 0);
            ASSERT_EQ(tuple.getValue(3)->nodeID.label, 28);
            auto nodeOffset = 10 - (matchedNumTuples > 1000 ? (matchedNumTuples - 1000) / 100 :
                                                              matchedNumTuples / 100);
            ASSERT_EQ(tuple.getValue(3)->nodeID.offset, nodeOffset);
            ASSERT_EQ(tuple.getValue(4)->primitive.double_, (double)(nodeOffset / 2));
            ASSERT_EQ(tuple.getValue(5)->nodeID.label, 28);
            ASSERT_EQ(tuple.getValue(5)->nodeID.offset, (matchedNumTuples > 1000 ? 3 : 2));
            ASSERT_EQ(tuple.getValue(6)->primitive.double_,
                (double)(tuple.getValue(5)->nodeID.offset / 2));
            ASSERT_EQ(tuple.getValue(7)->nodeID.label, 38);
            ASSERT_EQ(tuple.getValue(7)->nodeID.offset, tupleIndex / 10);
            ASSERT_EQ(tuple.getValue(8)->primitive.boolean, ((tupleIndex / 10) / 2) == 1);
            ASSERT_EQ(tuple.getValue(9)->nodeID.label, 38);
            ASSERT_EQ(tuple.getValue(9)->nodeID.offset, (tupleIndex % 10));
            ASSERT_EQ(tuple.getValue(10)->primitive.boolean, ((tupleIndex % 10) / 2) == 1);
            tupleIndex++;
        }
    }
    ASSERT_EQ(matchedNumTuples, 2000);
}

TEST(HashJoinTests, HashJoinTest2) {
    MemoryManager memoryManager(1024 * 1024 * 1024);

    auto buildOp = make_unique<ScanMockOp>();
    auto probeOp = make_unique<ProbeScanMockOp>();
    unique_ptr<HashJoin> hashJoin =
        make_unique<HashJoin>(memoryManager, 2, 0, 2, 0, move(buildOp), move(probeOp));
    uint64_t matchedNumTuples = 0;
    while (true) {
        hashJoin->getNextTuples();
        auto result = hashJoin->getDataChunks();
        matchedNumTuples += result->getNumTuples();
        if (result->getNumTuples() == 0) {
            break;
        }
        ASSERT_EQ(result->getNumTuples(), 100);
        vector<DataType> types;
        for (uint64_t i = 0; i < result->getNumDataChunks(); i++) {
            for (auto& vector : result->getDataChunk(i)->valueVectors) {
                types.push_back(vector->getDataType());
            }
        }
        Tuple tuple(types);
        DataChunksIterator dataChunksIterator(*result);
        uint64_t tupleIndex = 0;
        while (dataChunksIterator.hasNextTuple()) {
            dataChunksIterator.getNextTuple(tuple);
            ASSERT_EQ(tuple.getValue(0)->nodeID.label, 38);
            ASSERT_EQ(tuple.getValue(0)->nodeID.offset, tupleIndex / 10);
            ASSERT_EQ(tuple.getValue(1)->primitive.boolean, ((tupleIndex / 10) / 2) == 1);
            ASSERT_EQ(tuple.getValue(2)->primitive.boolean, ((tupleIndex / 10) / 2) == 1);
            ASSERT_EQ(tuple.getValue(3)->nodeID.label, 18);
            ASSERT_EQ(tuple.getValue(3)->nodeID.offset, 0);
            ASSERT_EQ(tuple.getValue(4)->primitive.integer, 0);
            ASSERT_EQ(tuple.getValue(5)->nodeID.label, 28);
            ASSERT_EQ(tuple.getValue(5)->nodeID.offset, (10 - ((tupleIndex % 10) + 1)));
            ASSERT_EQ(
                tuple.getValue(6)->primitive.double_, (double)((10 - ((tupleIndex % 10) + 1)) / 2));
            ASSERT_EQ(tuple.getValue(7)->nodeID.label, 18);
            ASSERT_EQ(tuple.getValue(7)->nodeID.offset, 0);
            ASSERT_EQ(tuple.getValue(8)->primitive.integer, 0);
            ASSERT_EQ(tuple.getValue(9)->nodeID.label, 28);
            ASSERT_EQ(tuple.getValue(9)->nodeID.offset, (matchedNumTuples / 100) + 1);
            ASSERT_EQ(tuple.getValue(10)->primitive.double_, (double)(tupleIndex / 100 + 1));
            tupleIndex++;
        }
    }
    ASSERT_EQ(matchedNumTuples, 200);
}

TEST(HashJoinTests, HashJoinTest3) {
    MemoryManager memoryManager(1024 * 1024 * 1024);

    auto buildOp = make_unique<ScanMockOp>();
    auto probeOp = make_unique<ProbeScanMockOp>();
    unique_ptr<HashJoin> hashJoin =
        make_unique<HashJoin>(memoryManager, 1, 0, 1, 0, move(buildOp), move(probeOp));
    uint64_t matchedNumTuples = 0;
    while (true) {
        hashJoin->getNextTuples();
        auto result = hashJoin->getDataChunks();
        matchedNumTuples += result->getNumTuples();
        if (result->getNumTuples() == 0) {
            break;
        }
        ASSERT_EQ(result->getNumTuples(), 100);
        vector<DataType> types;
        for (uint64_t i = 0; i < result->getNumDataChunks(); i++) {
            for (auto& vector : result->getDataChunk(i)->valueVectors) {
                types.push_back(vector->getDataType());
            }
        }
        Tuple tuple(types);
        DataChunksIterator dataChunksIterator(*result);
        uint64_t tupleIndex = 0;
        while (dataChunksIterator.hasNextTuple()) {
            dataChunksIterator.getNextTuple(tuple);
            ASSERT_EQ(tuple.getValue(0)->nodeID.label, 28);
            ASSERT_EQ(tuple.getValue(0)->nodeID.offset, (matchedNumTuples / 100) + 1);
            ASSERT_EQ(tuple.getValue(1)->primitive.double_, (double)(tupleIndex / 100 + 1));
            ASSERT_EQ(tuple.getValue(2)->primitive.double_, (double)(tupleIndex / 100 + 1));
            ASSERT_EQ(tuple.getValue(3)->nodeID.label, 18);
            ASSERT_EQ(tuple.getValue(3)->nodeID.offset, 0);
            ASSERT_EQ(tuple.getValue(4)->primitive.integer, 0);
            ASSERT_EQ(tuple.getValue(5)->nodeID.label, 18);
            ASSERT_EQ(tuple.getValue(5)->nodeID.offset, 0);
            ASSERT_EQ(tuple.getValue(6)->primitive.integer, 0);
            ASSERT_EQ(tuple.getValue(7)->nodeID.label, 38);
            ASSERT_EQ(tuple.getValue(7)->nodeID.offset, tupleIndex / 10);
            ASSERT_EQ(tuple.getValue(8)->primitive.boolean, ((tupleIndex / 10) / 2) == 1);
            ASSERT_EQ(tuple.getValue(9)->nodeID.label, 38);
            ASSERT_EQ(tuple.getValue(9)->nodeID.offset, (tupleIndex % 10));
            ASSERT_EQ(tuple.getValue(10)->primitive.boolean, ((tupleIndex % 10) / 2) == 1);
            tupleIndex++;
        }
    }
    ASSERT_EQ(matchedNumTuples, 200);
}

TEST(HashJoinTests, HashJoinTest4) {
    MemoryManager memoryManager(1024 * 1024 * 1024);

    auto buildOp = make_unique<ScanMockOp>();
    auto probeOp = make_unique<ProbeScanMockOp>();
    unique_ptr<HashJoin> hashJoin =
        make_unique<HashJoin>(memoryManager, 1, 0, 2, 0, move(buildOp), move(probeOp));
    uint64_t matchedNumTuples = 0;
    while (true) {
        hashJoin->getNextTuples();
        auto result = hashJoin->getDataChunks();
        matchedNumTuples += result->getNumTuples();
        if (result->getNumTuples() == 0) {
            break;
        }
    }
    ASSERT_EQ(matchedNumTuples, 0);
}
*/
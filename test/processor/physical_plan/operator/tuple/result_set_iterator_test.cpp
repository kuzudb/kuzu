#include "gtest/gtest.h"

#include "src/common/include/vector/node_vector.h"
#include "src/processor/include/physical_plan/operator/result/result_set_iterator.h"

using namespace graphflow::processor;

class ResultSetIteratorTest : public ::testing::Test {

public:
    void SetUp() override {
        auto dataChunkA = make_shared<DataChunk>();
        auto dataChunkB = make_shared<DataChunk>();
        auto dataChunkC = make_shared<DataChunk>();

        auto memoryManager = make_unique<MemoryManager>();
        NodeIDCompressionScheme compressionScheme;
        auto vectorA1 = make_shared<NodeIDVector>(18, compressionScheme, false);
        auto vectorA2 = make_shared<ValueVector>(memoryManager.get(), INT32);
        auto vectorB1 = make_shared<NodeIDVector>(28, compressionScheme, false);
        auto vectorB2 = make_shared<ValueVector>(memoryManager.get(), DOUBLE);
        auto vectorC1 = make_shared<NodeIDVector>(38, compressionScheme, false);
        auto vectorC2 = make_shared<ValueVector>(memoryManager.get(), BOOL);
        auto vectorA1Data = (uint64_t*)vectorA1->values;
        auto vectorA2Data = (uint32_t*)vectorA2->values;
        auto vectorB1Data = (uint64_t*)vectorB1->values;
        auto vectorB2Data = (double*)vectorB2->values;
        auto vectorC1Data = (uint64_t*)vectorC1->values;
        auto vectorC2Data = (bool*)vectorC2->values;

        for (int32_t i = 0; i < 100; i++) {
            vectorA1Data[i] = (uint64_t)i;
            vectorA2Data[i] = (int32_t)(i * 2);
            vectorB1Data[i] = (uint64_t)(i);
            vectorB2Data[i] = (double)(i / 2);
            vectorC1Data[i] = (uint64_t)i;
            vectorC2Data[i] = (bool)((i / 2) == 1);
        }
        dataChunkA->append(vectorA1);
        dataChunkA->append(vectorA2);
        dataChunkB->append(vectorB1);
        dataChunkB->append(vectorB2);
        dataChunkC->append(vectorC1);
        dataChunkC->append(vectorC2);

        dataChunkA->state->size = 100;
        dataChunkB->state->size = 100;
        dataChunkC->state->size = 100;
        resultSet.append(dataChunkA);
        resultSet.append(dataChunkB);
        resultSet.append(dataChunkC);
    }

    static vector<DataType> GetDataChunksTypes(ResultSet& resultSet) {
        vector<DataType> vectorTypes;
        for (const auto& dataChunk : resultSet.dataChunks) {
            for (auto& vector : dataChunk->valueVectors) {
                vectorTypes.push_back(vector->dataType);
            }
        }
        return vectorTypes;
    }

public:
    ResultSet resultSet;
};

TEST_F(ResultSetIteratorTest, DataChunksIteratorTest1) {
    auto vectorTypes = GetDataChunksTypes(resultSet);
    Tuple tuple(vectorTypes);

    auto dataChunkA = resultSet.dataChunks[0];
    auto dataChunkB = resultSet.dataChunks[1];
    auto dataChunkC = resultSet.dataChunks[2];
    dataChunkA->state->currPos = 1;
    dataChunkB->state->currPos = -1;
    dataChunkC->state->currPos = 10;

    ResultSetIterator resultSetIterator(resultSet);
    auto tupleIndex = 0;
    while (resultSetIterator.hasNextTuple()) {
        resultSetIterator.getNextTuple(tuple);
        ASSERT_EQ(tuple.getValue(0)->val.nodeID.label, 18);
        ASSERT_EQ(tuple.getValue(0)->val.nodeID.offset, 1);
        ASSERT_EQ(tuple.getValue(1)->val.int32Val, (int32_t)(1 * 2));
        ASSERT_EQ(tuple.getValue(2)->val.nodeID.label, 28);
        ASSERT_EQ(tuple.getValue(2)->val.nodeID.offset, tupleIndex);
        ASSERT_EQ(tuple.getValue(3)->val.doubleVal, (double)(tupleIndex / 2));
        ASSERT_EQ(tuple.getValue(4)->val.nodeID.label, 38);
        ASSERT_EQ(tuple.getValue(4)->val.nodeID.offset, 10);
        ASSERT_EQ(tuple.getValue(5)->val.booleanVal, (bool)false);
        tupleIndex++;
    }
    ASSERT_EQ(tupleIndex, 100);

    dataChunkA->state->currPos = 1;
    dataChunkB->state->currPos = 9;
    dataChunkC->state->currPos = 99;
    resultSetIterator.setDataChunks(resultSet);
    tupleIndex = 0;
    while (resultSetIterator.hasNextTuple()) {
        resultSetIterator.getNextTuple(tuple);
        ASSERT_EQ(tuple.getValue(0)->val.nodeID.label, 18);
        ASSERT_EQ(tuple.getValue(0)->val.nodeID.offset, 1);
        ASSERT_EQ(tuple.getValue(1)->val.int32Val, (int32_t)(1 * 2));
        ASSERT_EQ(tuple.getValue(2)->val.nodeID.label, 28);
        ASSERT_EQ(tuple.getValue(2)->val.nodeID.offset, 9);
        ASSERT_EQ(tuple.getValue(3)->val.doubleVal, (double)(9 / 2));
        ASSERT_EQ(tuple.getValue(4)->val.nodeID.label, 38);
        ASSERT_EQ(tuple.getValue(4)->val.nodeID.offset, 99);
        ASSERT_EQ(tuple.getValue(5)->val.booleanVal, (bool)((99 / 2) == 1));
        tupleIndex++;
    }
    ASSERT_EQ(tupleIndex, 1);
}

TEST_F(ResultSetIteratorTest, DataChunksIteratorTest2) {
    auto vectorTypes = GetDataChunksTypes(resultSet);
    Tuple tuple(vectorTypes);

    auto dataChunkA = resultSet.dataChunks[0];
    auto dataChunkB = resultSet.dataChunks[1];
    auto dataChunkC = resultSet.dataChunks[2];

    auto tupleIndex = 0;
    dataChunkA->state->currPos = 1;
    dataChunkB->state->currPos = -1;
    dataChunkC->state->currPos = -1;
    ResultSetIterator resultSetIterator(resultSet);
    while (resultSetIterator.hasNextTuple()) {
        auto bid = tupleIndex / 100;
        auto cid = tupleIndex % 100;
        resultSetIterator.getNextTuple(tuple);
        ASSERT_EQ(tuple.getValue(0)->val.nodeID.label, 18);
        ASSERT_EQ(tuple.getValue(0)->val.nodeID.offset, 1);
        ASSERT_EQ(tuple.getValue(1)->val.int32Val, (int32_t)(1 * 2));
        ASSERT_EQ(tuple.getValue(2)->val.nodeID.label, 28);
        ASSERT_EQ(tuple.getValue(2)->val.nodeID.offset, bid);
        ASSERT_EQ(tuple.getValue(3)->val.doubleVal, (double)(bid / 2));
        ASSERT_EQ(tuple.getValue(4)->val.nodeID.label, 38);
        ASSERT_EQ(tuple.getValue(4)->val.nodeID.offset, cid);
        ASSERT_EQ(tuple.getValue(5)->val.booleanVal, (bool)((cid / 2) == 1));
        tupleIndex++;
    }
    ASSERT_EQ(tupleIndex, 10000);
}

TEST_F(ResultSetIteratorTest, DataChunksIteratorTest3) {
    auto vectorTypes = GetDataChunksTypes(resultSet);
    Tuple tuple(vectorTypes);

    auto dataChunkA = resultSet.dataChunks[0];
    auto dataChunkB = resultSet.dataChunks[1];
    auto dataChunkC = resultSet.dataChunks[2];

    auto tupleIndex = 0;
    dataChunkA->state->currPos = -1;
    dataChunkB->state->currPos = 10;
    dataChunkC->state->currPos = -1;
    ResultSetIterator resultSetIterator(resultSet);
    while (resultSetIterator.hasNextTuple()) {
        auto aid = tupleIndex / 100;
        auto cid = tupleIndex % 100;
        resultSetIterator.getNextTuple(tuple);
        ASSERT_EQ(tuple.getValue(0)->val.nodeID.label, 18);
        ASSERT_EQ(tuple.getValue(0)->val.nodeID.offset, aid);
        ASSERT_EQ(tuple.getValue(1)->val.int32Val, (int32_t)(aid * 2));
        ASSERT_EQ(tuple.getValue(2)->val.nodeID.label, 28);
        ASSERT_EQ(tuple.getValue(2)->val.nodeID.offset, 10);
        ASSERT_EQ(tuple.getValue(3)->val.doubleVal, (double)(10 / 2));
        ASSERT_EQ(tuple.getValue(4)->val.nodeID.label, 38);
        ASSERT_EQ(tuple.getValue(4)->val.nodeID.offset, cid);
        ASSERT_EQ(tuple.getValue(5)->val.booleanVal, (bool)((cid / 2) == 1));
        tupleIndex++;
    }
    ASSERT_EQ(tupleIndex, 10000);
}

TEST_F(ResultSetIteratorTest, DataChunksIteratorTest4) {
    auto vectorTypes = GetDataChunksTypes(resultSet);
    Tuple tuple(vectorTypes);

    auto dataChunkA = resultSet.dataChunks[0];
    auto dataChunkB = resultSet.dataChunks[1];
    auto dataChunkC = resultSet.dataChunks[2];
    dataChunkA->state->currPos = 1;
    dataChunkB->state->currPos = 10;
    dataChunkC->state->currPos = 20;

    auto tupleIndex = 0;
    ResultSetIterator resultSetIterator(resultSet);
    while (resultSetIterator.hasNextTuple()) {
        resultSetIterator.getNextTuple(tuple);
        string tupleStr = tuple.toString();
        string expected = "18:1|2|28:10|5.000000|38:20|False";
        ASSERT_EQ(tupleStr, expected);
        tupleIndex++;
    }
    ASSERT_EQ(tupleIndex, 1);
}

TEST_F(ResultSetIteratorTest, DataChunksIteratorTestWithSelector) {
    auto vectorTypes = GetDataChunksTypes(resultSet);
    Tuple tuple(vectorTypes);

    auto dataChunkA = resultSet.dataChunks[0];
    auto dataChunkB = resultSet.dataChunks[1];
    auto dataChunkC = resultSet.dataChunks[2];
    dataChunkA->state->selectedValuesPos[1] = 5;
    dataChunkA->state->currPos = 1;
    dataChunkB->state->selectedValuesPos[10] = 15;
    dataChunkB->state->currPos = 10;
    dataChunkC->state->selectedValuesPos[20] = 25;
    dataChunkC->state->currPos = 20;

    auto tupleIndex = 0;
    ResultSetIterator resultSetIterator(resultSet);
    while (resultSetIterator.hasNextTuple()) {
        resultSetIterator.getNextTuple(tuple);
        string tupleStr = tuple.toString();
        string expected = "18:5|10|28:15|7.000000|38:25|False";
        ASSERT_EQ(tupleStr, expected);
        tupleIndex++;
    }
    ASSERT_EQ(tupleIndex, 1);
}

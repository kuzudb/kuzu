#include "gtest/gtest.h"

#include "src/processor/include/physical_plan/operator/result/result_set_iterator.h"

using namespace graphflow::processor;

class ResultSetIteratorTest : public ::testing::Test {

public:
    void SetUp() override {
        vectorToCollectPos.emplace_back(0, 0);
        vectorToCollectPos.emplace_back(0, 1);
        vectorToCollectPos.emplace_back(1, 0);
        vectorToCollectPos.emplace_back(1, 1);
        vectorToCollectPos.emplace_back(2, 0);
        vectorToCollectPos.emplace_back(2, 1);

        resultSet = make_unique<ResultSet>(3);

        auto dataChunkA = make_shared<DataChunk>(2);
        auto dataChunkB = make_shared<DataChunk>(2);
        auto dataChunkC = make_shared<DataChunk>(2);

        auto memoryManager = make_unique<MemoryManager>();
        auto vectorA1 = make_shared<ValueVector>(memoryManager.get(), NODE);
        auto vectorA2 = make_shared<ValueVector>(memoryManager.get(), INT64);
        auto vectorB1 = make_shared<ValueVector>(memoryManager.get(), NODE);
        auto vectorB2 = make_shared<ValueVector>(memoryManager.get(), DOUBLE);
        auto vectorC1 = make_shared<ValueVector>(memoryManager.get(), NODE);
        auto vectorC2 = make_shared<ValueVector>(memoryManager.get(), BOOL);
        auto vectorA1Data = (nodeID_t*)vectorA1->values;
        auto vectorA2Data = (int64_t*)vectorA2->values;
        auto vectorB1Data = (nodeID_t*)vectorB1->values;
        auto vectorB2Data = (double*)vectorB2->values;
        auto vectorC1Data = (nodeID_t*)vectorC1->values;
        auto vectorC2Data = (bool*)vectorC2->values;

        for (int32_t i = 0; i < 100; i++) {
            vectorA1Data[i].label = 18;
            vectorA1Data[i].offset = (uint64_t)i;
            vectorA2Data[i] = (int64_t)(i * 2);
            vectorB1Data[i].label = 28;
            vectorB1Data[i].offset = (uint64_t)(i);
            vectorB2Data[i] = (double)(i / 2);
            vectorC1Data[i].label = 38;
            vectorC1Data[i].offset = (uint64_t)i;
            vectorC2Data[i] = (bool)((i / 2) == 1);
        }
        dataChunkA->insert(0, vectorA1);
        dataChunkA->insert(1, vectorA2);
        dataChunkB->insert(0, vectorB1);
        dataChunkB->insert(1, vectorB2);
        dataChunkC->insert(0, vectorC1);
        dataChunkC->insert(1, vectorC2);

        dataChunkA->state->selectedSize = 100;
        dataChunkB->state->selectedSize = 100;
        dataChunkC->state->selectedSize = 100;
        resultSet->insert(0, dataChunkA);
        resultSet->insert(1, dataChunkB);
        resultSet->insert(2, dataChunkC);
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
    unique_ptr<ResultSet> resultSet;
    vector<DataPos> vectorToCollectPos;
};

TEST_F(ResultSetIteratorTest, DataChunksIteratorTest1) {
    auto vectorTypes = GetDataChunksTypes(*resultSet);
    Tuple tuple(vectorTypes);

    auto dataChunkA = resultSet->dataChunks[0];
    auto dataChunkB = resultSet->dataChunks[1];
    auto dataChunkC = resultSet->dataChunks[2];
    dataChunkA->state->currIdx = 1;
    dataChunkB->state->currIdx = -1;
    dataChunkC->state->currIdx = 10;

    ResultSetIterator resultSetIterator(resultSet.get(), vectorToCollectPos);
    auto tupleIndex = 0;
    while (resultSetIterator.hasNextTuple()) {
        resultSetIterator.getNextTuple(tuple);
        ASSERT_EQ(tuple.getValue(0)->val.nodeID.label, 18);
        ASSERT_EQ(tuple.getValue(0)->val.nodeID.offset, 1);
        ASSERT_EQ(tuple.getValue(1)->val.int64Val, (int64_t)(1 * 2));
        ASSERT_EQ(tuple.getValue(2)->val.nodeID.label, 28);
        ASSERT_EQ(tuple.getValue(2)->val.nodeID.offset, tupleIndex);
        ASSERT_EQ(tuple.getValue(3)->val.doubleVal, (double)(tupleIndex / 2));
        ASSERT_EQ(tuple.getValue(4)->val.nodeID.label, 38);
        ASSERT_EQ(tuple.getValue(4)->val.nodeID.offset, 10);
        ASSERT_EQ(tuple.getValue(5)->val.booleanVal, (bool)false);
        tupleIndex++;
    }
    ASSERT_EQ(tupleIndex, 100);

    dataChunkA->state->currIdx = 1;
    dataChunkB->state->currIdx = 9;
    dataChunkC->state->currIdx = 99;
    resultSetIterator.setResultSet(resultSet.get());
    tupleIndex = 0;
    while (resultSetIterator.hasNextTuple()) {
        resultSetIterator.getNextTuple(tuple);
        ASSERT_EQ(tuple.getValue(0)->val.nodeID.label, 18);
        ASSERT_EQ(tuple.getValue(0)->val.nodeID.offset, 1);
        ASSERT_EQ(tuple.getValue(1)->val.int64Val, (int64_t)(1 * 2));
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
    auto vectorTypes = GetDataChunksTypes(*resultSet);
    Tuple tuple(vectorTypes);

    auto dataChunkA = resultSet->dataChunks[0];
    auto dataChunkB = resultSet->dataChunks[1];
    auto dataChunkC = resultSet->dataChunks[2];

    auto tupleIndex = 0;
    dataChunkA->state->currIdx = 1;
    dataChunkB->state->currIdx = -1;
    dataChunkC->state->currIdx = -1;
    ResultSetIterator resultSetIterator(resultSet.get(), vectorToCollectPos);
    while (resultSetIterator.hasNextTuple()) {
        auto bid = tupleIndex / 100;
        auto cid = tupleIndex % 100;
        resultSetIterator.getNextTuple(tuple);
        ASSERT_EQ(tuple.getValue(0)->val.nodeID.label, 18);
        ASSERT_EQ(tuple.getValue(0)->val.nodeID.offset, 1);
        ASSERT_EQ(tuple.getValue(1)->val.int64Val, (int64_t)(1 * 2));
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
    auto vectorTypes = GetDataChunksTypes(*resultSet);
    Tuple tuple(vectorTypes);

    auto dataChunkA = resultSet->dataChunks[0];
    auto dataChunkB = resultSet->dataChunks[1];
    auto dataChunkC = resultSet->dataChunks[2];

    auto tupleIndex = 0;
    dataChunkA->state->currIdx = -1;
    dataChunkB->state->currIdx = 10;
    dataChunkC->state->currIdx = -1;
    ResultSetIterator resultSetIterator(resultSet.get(), vectorToCollectPos);
    while (resultSetIterator.hasNextTuple()) {
        auto aid = tupleIndex / 100;
        auto cid = tupleIndex % 100;
        resultSetIterator.getNextTuple(tuple);
        ASSERT_EQ(tuple.getValue(0)->val.nodeID.label, 18);
        ASSERT_EQ(tuple.getValue(0)->val.nodeID.offset, aid);
        ASSERT_EQ(tuple.getValue(1)->val.int64Val, (int64_t)(aid * 2));
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
    auto vectorTypes = GetDataChunksTypes(*resultSet);
    Tuple tuple(vectorTypes);

    auto dataChunkA = resultSet->dataChunks[0];
    auto dataChunkB = resultSet->dataChunks[1];
    auto dataChunkC = resultSet->dataChunks[2];
    dataChunkA->state->currIdx = 1;
    dataChunkB->state->currIdx = 10;
    dataChunkC->state->currIdx = 20;

    auto tupleIndex = 0;
    ResultSetIterator resultSetIterator(resultSet.get(), vectorToCollectPos);
    while (resultSetIterator.hasNextTuple()) {
        resultSetIterator.getNextTuple(tuple);
        string tupleStr = tuple.toString(vector<uint32_t>(tuple.len(), 0));
        string expected = "18:1|2|28:10|5.000000|38:20|False";
        ASSERT_EQ(tupleStr, expected);
        tupleIndex++;
    }
    ASSERT_EQ(tupleIndex, 1);
}

TEST_F(ResultSetIteratorTest, DataChunksIteratorTestWithSelector) {
    auto vectorTypes = GetDataChunksTypes(*resultSet);
    Tuple tuple(vectorTypes);

    auto dataChunkA = resultSet->dataChunks[0];
    dataChunkA->state->resetSelectorToValuePosBuffer();
    auto dataChunkB = resultSet->dataChunks[1];
    dataChunkB->state->resetSelectorToValuePosBuffer();
    auto dataChunkC = resultSet->dataChunks[2];
    dataChunkC->state->resetSelectorToValuePosBuffer();
    dataChunkA->state->selectedPositions[1] = 5;
    dataChunkA->state->currIdx = 1;
    dataChunkB->state->selectedPositions[10] = 15;
    dataChunkB->state->currIdx = 10;
    dataChunkC->state->selectedPositions[20] = 25;
    dataChunkC->state->currIdx = 20;

    auto tupleIndex = 0;
    ResultSetIterator resultSetIterator(resultSet.get(), vectorToCollectPos);
    while (resultSetIterator.hasNextTuple()) {
        resultSetIterator.getNextTuple(tuple);
        string tupleStr = tuple.toString(vector<uint32_t>(tuple.len(), 0));
        string expected = "18:5|10|28:15|7.000000|38:25|False";
        ASSERT_EQ(tupleStr, expected);
        tupleIndex++;
    }
    ASSERT_EQ(tupleIndex, 1);
}

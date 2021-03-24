#include "gtest/gtest.h"

#include "src/processor/include/physical_plan/operator/tuple/data_chunks_iterator.h"

using namespace graphflow::processor;

/*
class DataChunksIteratorTest : public ::testing::Test {

public:
    void SetUp() override {
        auto dataChunkA = make_shared<DataChunk>();
        auto dataChunkB = make_shared<DataChunk>();
        auto dataChunkC = make_shared<DataChunk>();

        NodeIDCompressionScheme compressionScheme;
        auto vectorA1 = make_shared<NodeIDVector>(18, compressionScheme);
        auto vectorA2 = make_shared<ValueVector>(INT32);
        auto vectorB1 = make_shared<NodeIDVector>(28, compressionScheme);
        auto vectorB2 = make_shared<ValueVector>(DOUBLE);
        auto vectorC1 = make_shared<NodeIDVector>(38, compressionScheme);
        auto vectorC2 = make_shared<ValueVector>(BOOL);

        for (int32_t i = 0; i < 100; i++) {
            vectorA1->setValue<uint64_t>(i, (uint64_t)i);
            vectorA2->setValue<int32_t>(i, (int32_t)(i * 2));
            vectorB1->setValue<uint64_t>(i, (uint64_t)i);
            vectorB2->setValue(i, (double)(i / 2));
            vectorC1->setValue<uint64_t>(i, (uint64_t)i);
            vectorC2->setValue(i, (bool)((i / 2) == 1));
        }
        dataChunkA->append(vectorA1);
        vectorA1->setDataChunkOwner(dataChunkA);
        dataChunkA->append(vectorA2);
        vectorA2->setDataChunkOwner(dataChunkA);
        dataChunkB->append(vectorB1);
        vectorB1->setDataChunkOwner(dataChunkB);
        dataChunkB->append(vectorB2);
        vectorB2->setDataChunkOwner(dataChunkB);
        dataChunkC->append(vectorC1);
        vectorC1->setDataChunkOwner(dataChunkC);
        dataChunkC->append(vectorC2);
        vectorC2->setDataChunkOwner(dataChunkC);

        dataChunkA->size = 100;
        dataChunkB->size = 100;
        dataChunkC->size = 100;
        dataChunks.append(dataChunkA);
        dataChunks.append(dataChunkB);
        dataChunks.append(dataChunkC);
    }

    vector<DataType> GetDataChunksTypes(DataChunks& dataChunks) {
        vector<DataType> vectorTypes;
        for (uint64_t i = 0; i < dataChunks.getNumDataChunks(); i++) {
            auto dataChunk = dataChunks.getDataChunk(i);
            for (auto& vector : dataChunk->valueVectors) {
                vectorTypes.push_back(vector->getDataType());
            }
        }
        return vectorTypes;
    }

public:
    DataChunks dataChunks;
};

TEST_F(DataChunksIteratorTest, DataChunksIteratorTest1) {
    auto vectorTypes = GetDataChunksTypes(dataChunks);
    Tuple tuple(vectorTypes);

    auto dataChunkA = dataChunks.getDataChunk(0);
    auto dataChunkB = dataChunks.getDataChunk(1);
    auto dataChunkC = dataChunks.getDataChunk(2);
    dataChunkA->currPos = 1;
    dataChunkB->currPos = -1;
    dataChunkC->currPos = 10;

    DataChunksIterator dataChunksIterator(dataChunks);
    auto tupleIndex = 0;
    while (dataChunksIterator.hasNextTuple()) {
        dataChunksIterator.getNextTuple(tuple);
        ASSERT_EQ(tuple.getValue(0)->nodeID.label, 18);
        ASSERT_EQ(tuple.getValue(0)->nodeID.offset, 1);
        ASSERT_EQ(tuple.getValue(1)->primitive.integer, (int32_t)(1 * 2));
        ASSERT_EQ(tuple.getValue(2)->nodeID.label, 28);
        ASSERT_EQ(tuple.getValue(2)->nodeID.offset, tupleIndex);
        ASSERT_EQ(tuple.getValue(3)->primitive.double_, (double)(tupleIndex / 2));
        ASSERT_EQ(tuple.getValue(4)->nodeID.label, 38);
        ASSERT_EQ(tuple.getValue(4)->nodeID.offset, 10);
        ASSERT_EQ(tuple.getValue(5)->primitive.boolean, (bool)false);
        tupleIndex++;
    }
    ASSERT_EQ(tupleIndex, 100);

    dataChunkA->currPos = 1;
    dataChunkB->currPos = 9;
    dataChunkC->currPos = 99;
    dataChunksIterator.setDataChunks(dataChunks);
    tupleIndex = 0;
    while (dataChunksIterator.hasNextTuple()) {
        dataChunksIterator.getNextTuple(tuple);
        ASSERT_EQ(tuple.getValue(0)->nodeID.label, 18);
        ASSERT_EQ(tuple.getValue(0)->nodeID.offset, 1);
        ASSERT_EQ(tuple.getValue(1)->primitive.integer, (int32_t)(1 * 2));
        ASSERT_EQ(tuple.getValue(2)->nodeID.label, 28);
        ASSERT_EQ(tuple.getValue(2)->nodeID.offset, 9);
        ASSERT_EQ(tuple.getValue(3)->primitive.double_, (double)(9 / 2));
        ASSERT_EQ(tuple.getValue(4)->nodeID.label, 38);
        ASSERT_EQ(tuple.getValue(4)->nodeID.offset, 99);
        ASSERT_EQ(tuple.getValue(5)->primitive.boolean, (bool)((99 / 2) == 1));
        tupleIndex++;
    }
    ASSERT_EQ(tupleIndex, 1);
}

TEST_F(DataChunksIteratorTest, DataChunksIteratorTest2) {
    auto vectorTypes = GetDataChunksTypes(dataChunks);
    Tuple tuple(vectorTypes);

    auto dataChunkA = dataChunks.getDataChunk(0);
    auto dataChunkB = dataChunks.getDataChunk(1);
    auto dataChunkC = dataChunks.getDataChunk(2);

    auto tupleIndex = 0;
    dataChunkA->currPos = 1;
    dataChunkB->currPos = -1;
    dataChunkC->currPos = -1;
    DataChunksIterator dataChunksIterator(dataChunks);
    while (dataChunksIterator.hasNextTuple()) {
        auto bid = tupleIndex / 100;
        auto cid = tupleIndex % 100;
        dataChunksIterator.getNextTuple(tuple);
        ASSERT_EQ(tuple.getValue(0)->nodeID.label, 18);
        ASSERT_EQ(tuple.getValue(0)->nodeID.offset, 1);
        ASSERT_EQ(tuple.getValue(1)->primitive.integer, (int32_t)(1 * 2));
        ASSERT_EQ(tuple.getValue(2)->nodeID.label, 28);
        ASSERT_EQ(tuple.getValue(2)->nodeID.offset, bid);
        ASSERT_EQ(tuple.getValue(3)->primitive.double_, (double)(bid / 2));
        ASSERT_EQ(tuple.getValue(4)->nodeID.label, 38);
        ASSERT_EQ(tuple.getValue(4)->nodeID.offset, cid);
        ASSERT_EQ(tuple.getValue(5)->primitive.boolean, (bool)((cid / 2) == 1));
        tupleIndex++;
    }
    ASSERT_EQ(tupleIndex, 10000);
}

TEST_F(DataChunksIteratorTest, DataChunksIteratorTest3) {
    auto vectorTypes = GetDataChunksTypes(dataChunks);
    Tuple tuple(vectorTypes);

    auto dataChunkA = dataChunks.getDataChunk(0);
    auto dataChunkB = dataChunks.getDataChunk(1);
    auto dataChunkC = dataChunks.getDataChunk(2);

    auto tupleIndex = 0;
    dataChunkA->currPos = -1;
    dataChunkB->currPos = 10;
    dataChunkC->currPos = -1;
    DataChunksIterator dataChunksIterator(dataChunks);
    while (dataChunksIterator.hasNextTuple()) {
        auto aid = tupleIndex / 100;
        auto cid = tupleIndex % 100;
        dataChunksIterator.getNextTuple(tuple);
        ASSERT_EQ(tuple.getValue(0)->nodeID.label, 18);
        ASSERT_EQ(tuple.getValue(0)->nodeID.offset, aid);
        ASSERT_EQ(tuple.getValue(1)->primitive.integer, (int32_t)(aid * 2));
        ASSERT_EQ(tuple.getValue(2)->nodeID.label, 28);
        ASSERT_EQ(tuple.getValue(2)->nodeID.offset, 10);
        ASSERT_EQ(tuple.getValue(3)->primitive.double_, (double)(10 / 2));
        ASSERT_EQ(tuple.getValue(4)->nodeID.label, 38);
        ASSERT_EQ(tuple.getValue(4)->nodeID.offset, cid);
        ASSERT_EQ(tuple.getValue(5)->primitive.boolean, (bool)((cid / 2) == 1));
        tupleIndex++;
    }
    ASSERT_EQ(tupleIndex, 10000);
}

TEST_F(DataChunksIteratorTest, DataChunksIteratorTest4) {
    auto vectorTypes = GetDataChunksTypes(dataChunks);
    Tuple tuple(vectorTypes);

    auto dataChunkA = dataChunks.getDataChunk(0);
    auto dataChunkB = dataChunks.getDataChunk(1);
    auto dataChunkC = dataChunks.getDataChunk(2);
    dataChunkA->currPos = 1;
    dataChunkB->currPos = 10;
    dataChunkC->currPos = 20;

    auto tupleIndex = 0;
    DataChunksIterator dataChunksIterator(dataChunks);
    while (dataChunksIterator.hasNextTuple()) {
        dataChunksIterator.getNextTuple(tuple);
        string tupleStr = tuple.toString();
        string expected = "18:1|2|28:10|5.000000|38:20|False";
        ASSERT_EQ(tupleStr, expected);
        tupleIndex++;
    }
    ASSERT_EQ(tupleIndex, 1);
}
*/

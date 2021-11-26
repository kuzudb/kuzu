#include "gtest/gtest.h"

#include "src/processor/include/physical_plan/result/row_collection.h"

using namespace graphflow::processor;

class RowCollectionTest : public ::testing::Test {

public:
    static unique_ptr<ResultSet> initResultSet() {
        auto resultSet = make_unique<ResultSet>(2);
        auto dataChunkA = make_shared<DataChunk>(2);
        auto dataChunkB = make_shared<DataChunk>(2);
        auto vectorA1 = make_shared<ValueVector>(nullptr, NODE);
        auto vectorA2 = make_shared<ValueVector>(nullptr, INT64);
        auto vectorB1 = make_shared<ValueVector>(nullptr, NODE);
        auto vectorB2 = make_shared<ValueVector>(nullptr, DOUBLE);
        dataChunkA->insert(0, vectorA1);
        dataChunkA->insert(1, vectorA2);
        dataChunkB->insert(0, vectorB1);
        dataChunkB->insert(1, vectorB2);
        resultSet->insert(0, dataChunkA);
        resultSet->insert(1, dataChunkB);
        return resultSet;
    }

    void SetUp() override {
        resultSet = initResultSet();
        auto vectorA1 = resultSet->dataChunks[0]->valueVectors[0];
        auto vectorA2 = resultSet->dataChunks[0]->valueVectors[1];
        auto vectorB1 = resultSet->dataChunks[1]->valueVectors[0];
        auto vectorB2 = resultSet->dataChunks[1]->valueVectors[1];

        auto vectorA1Data = (nodeID_t*)vectorA1->values;
        auto vectorA2Data = (int64_t*)vectorA2->values;
        auto vectorB1Data = (nodeID_t*)vectorB1->values;
        auto vectorB2Data = (double*)vectorB2->values;
        for (auto i = 0u; i < 100; i++) {
            vectorA1Data[i].label = 18;
            vectorA1Data[i].offset = (uint64_t)i;
            vectorA2Data[i] = i * 2;
            vectorB1Data[i].label = 28;
            vectorB1Data[i].offset = (uint64_t)(i);
            vectorB2Data[i] = (double)((double)i / 2.0);
        }
        resultSet->dataChunks[0]->state->selectedSize = 100;
        resultSet->dataChunks[1]->state->selectedSize = 100;
    }

public:
    unique_ptr<ResultSet> resultSet;
};

TEST_F(RowCollectionTest, TestSingleUnFlatDataChunk) {
    auto memoryManager = make_unique<MemoryManager>();
    vector<FieldInLayout> fields;
    fields.emplace_back(TypeUtils::getDataTypeSize(NODE), false);
    fields.emplace_back(TypeUtils::getDataTypeSize(NODE), false);
    RowLayout rowLayout(fields);
    auto rowCollection = make_unique<RowCollection>(*memoryManager, rowLayout);
    resultSet->dataChunks[1]->state->currIdx = 0; // Flat dataChunks[1], only append dataChunks[0]
    vector<shared_ptr<ValueVector>> vectorsToAppend;
    vectorsToAppend.push_back(resultSet->dataChunks[0]->valueVectors[0]);
    vectorsToAppend.push_back(resultSet->dataChunks[0]->valueVectors[1]);
    rowCollection->append(vectorsToAppend, 100);

    vector<DataPos> readDataPos;
    readDataPos.emplace_back(0, 0);
    readDataPos.emplace_back(0, 1);
    auto readResultSet = initResultSet();
    auto vectorA1 = resultSet->dataChunks[0]->valueVectors[0];
    auto vectorA2 = resultSet->dataChunks[0]->valueVectors[1];
    readResultSet->dataChunks[0]->state->currIdx = -1;
    // Read from the first row.
    vector<uint64_t> fieldIdsToScan{0, 1};
    auto numRowsRead = rowCollection->scan(fieldIdsToScan, readDataPos, *readResultSet, 0, 100);
    ASSERT_EQ(numRowsRead, 100);
    ASSERT_EQ(readResultSet->dataChunks[0]->state->selectedSize, 100);
    auto vectorA1Data = (nodeID_t*)vectorA1->values;
    auto vectorA2Data = (int64_t*)vectorA2->values;
    for (auto i = 0u; i < 100; i++) {
        ASSERT_EQ(vectorA1Data[i].label, 18);
        ASSERT_EQ(vectorA1Data[i].offset, (uint64_t)i);
        ASSERT_EQ(vectorA2Data[i], i * 2);
    }
    // Read from the middle.
    numRowsRead = rowCollection->scan(fieldIdsToScan, readDataPos, *readResultSet, 50, 50);
    ASSERT_EQ(numRowsRead, 50);
    // Read from the last row.
    numRowsRead = rowCollection->scan(fieldIdsToScan, readDataPos, *readResultSet, 100, 0);
    ASSERT_EQ(numRowsRead, 0);
}

TEST_F(RowCollectionTest, TestDataChunksWithVectorOverflows) {
    auto memoryManager = make_unique<MemoryManager>();
    vector<FieldInLayout> fields;
    fields.emplace_back(TypeUtils::getDataTypeSize(NODE), false);
    fields.emplace_back(TypeUtils::getDataTypeSize(NODE), true);
    RowLayout rowLayout(fields);
    auto rowCollection = make_unique<RowCollection>(*memoryManager, rowLayout);
    resultSet->dataChunks[0]->state->currIdx = 10;
    resultSet->dataChunks[1]->state->currIdx = -1;
    vector<shared_ptr<ValueVector>> vectorsToAppend;
    vectorsToAppend.push_back(resultSet->dataChunks[0]->valueVectors[0]);
    vectorsToAppend.push_back(resultSet->dataChunks[1]->valueVectors[0]);
    rowCollection->append(vectorsToAppend, 1);

    vector<DataPos> readDataPos;
    readDataPos.emplace_back(0, 0);
    readDataPos.emplace_back(1, 0);
    auto readResultSet = initResultSet();
    auto vectorA1 = readResultSet->dataChunks[0]->valueVectors[0];
    auto vectorB1 = readResultSet->dataChunks[1]->valueVectors[0];
    readResultSet->dataChunks[0]->state->currIdx = 0;
    readResultSet->dataChunks[1]->state->currIdx = -1;
    vector<uint64_t> fieldIdsToScan{0, 1};
    // Read from the first row.
    auto numRowsRead = rowCollection->scan(fieldIdsToScan, readDataPos, *readResultSet, 0, 100);
    ASSERT_EQ(numRowsRead, 1);
    ASSERT_EQ(readResultSet->dataChunks[1]->state->selectedSize, 100);
    auto vectorA1Data = (nodeID_t*)vectorA1->values;
    auto vectorB1Data = (nodeID_t*)vectorB1->values;
    ASSERT_EQ(vectorA1Data[0].label, 18);
    ASSERT_EQ(vectorA1Data[0].offset, (uint64_t)10);
    for (auto i = 0u; i < 100; i++) {
        ASSERT_EQ(vectorB1Data[i].label, 28);
        ASSERT_EQ(vectorB1Data[i].offset, (uint64_t)(i));
    }
    // Read from the last row.
    numRowsRead = rowCollection->scan(fieldIdsToScan, readDataPos, *readResultSet, 100, 0);
    ASSERT_EQ(numRowsRead, 0);
}

TEST_F(RowCollectionTest, TestFlatAndUnFlatDataChunk) {
    auto memoryManager = make_unique<MemoryManager>();
    vector<FieldInLayout> fields;
    fields.emplace_back(TypeUtils::getDataTypeSize(NODE), false);
    fields.emplace_back(TypeUtils::getDataTypeSize(NODE), false);
    RowLayout rowLayout(fields);
    auto rowCollection = make_unique<RowCollection>(*memoryManager, rowLayout);
    resultSet->dataChunks[0]->state->currIdx = 10;
    resultSet->dataChunks[1]->state->currIdx = -1;
    vector<shared_ptr<ValueVector>> vectorsToAppend;
    vectorsToAppend.push_back(resultSet->dataChunks[0]->valueVectors[0]);
    vectorsToAppend.push_back(resultSet->dataChunks[1]->valueVectors[0]);
    rowCollection->append(vectorsToAppend, 100);

    vector<DataPos> readDataPos;
    readDataPos.emplace_back(0, 0);
    readDataPos.emplace_back(0, 1);
    auto readResultSet = make_unique<ResultSet>(1);
    auto readDataChunk = make_shared<DataChunk>(2);
    auto vectorA1 = make_shared<ValueVector>(nullptr, NODE, false);
    auto vectorA2 = make_shared<ValueVector>(nullptr, NODE, false);
    readDataChunk->insert(0, vectorA1);
    readDataChunk->insert(1, vectorA2);
    readResultSet->insert(0, readDataChunk);
    readResultSet->dataChunks[0]->state->currIdx = -1;
    vector<uint64_t> fieldIdsToScan{0, 1};
    // Read from the first row.
    auto numRowsRead = rowCollection->scan(fieldIdsToScan, readDataPos, *readResultSet, 0, 100);
    ASSERT_EQ(numRowsRead, 100);
    ASSERT_EQ(readResultSet->dataChunks[0]->state->selectedSize, 100);
    auto vectorA1Data = (nodeID_t*)vectorA1->values;
    auto vectorA2Data = (nodeID_t*)vectorA2->values;
    for (auto i = 0u; i < 100; i++) {
        ASSERT_EQ(vectorA1Data[0].label, 18);
        ASSERT_EQ(vectorA1Data[0].offset, (uint64_t)10);
        ASSERT_EQ(vectorA2Data[i].label, 28);
        ASSERT_EQ(vectorA2Data[i].offset, (uint64_t)(i));
    }
    // Read from the last row.
    numRowsRead = rowCollection->scan(fieldIdsToScan, readDataPos, *readResultSet, 100, 0);
    ASSERT_EQ(numRowsRead, 0);
}

#include "gtest/gtest.h"

#include "src/common/include/exception.h"
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
        memoryManager = make_unique<MemoryManager>();
        auto vectorA1 = resultSet->dataChunks[0]->valueVectors[0];
        auto vectorA2 = resultSet->dataChunks[0]->valueVectors[1];
        auto vectorB1 = resultSet->dataChunks[1]->valueVectors[0];
        auto vectorB2 = resultSet->dataChunks[1]->valueVectors[1];

        auto vectorA1Data = (nodeID_t*)vectorA1->values;
        auto vectorA2Data = (int64_t*)vectorA2->values;
        auto vectorB1Data = (nodeID_t*)vectorB1->values;
        auto vectorB2Data = (double*)vectorB2->values;
        for (auto i = 0u; i < 100; i++) {
            if (i % 15) {
                vectorA1Data[i].label = 18;
                vectorA1Data[i].offset = (uint64_t)i;
            } else {
                vectorA1->setNull(i, true);
            }

            if (i % 10) {
                vectorA2Data[i] = i << 1;
                vectorB1Data[i].label = 28;
                vectorB1Data[i].offset = (uint64_t)(i);
            } else {
                vectorA2->setNull(i, true);
                vectorB1->setNull(i, true);
            }
            vectorB2Data[i] = (double)((double)i / 2.0);
        }
        resultSet->dataChunks[0]->state->selectedSize = 100;
        resultSet->dataChunks[1]->state->selectedSize = 100;
    }

    static void checkA2OverflowValuesAndNulls(shared_ptr<ValueVector> vectorA2) {
        for (auto i = 0u; i < 100; i++) {
            if (i % 10) {
                ASSERT_EQ(vectorA2->isNull(i), false);
                ASSERT_EQ(((int64_t*)vectorA2->values)[i], i << 1);
            } else {
                ASSERT_EQ(vectorA2->isNull(i), true);
            }
        }
    }

    unique_ptr<RowCollection> appendMultipleRows(bool isAppendFlatVectorToOverFlowCol) {
        // Prepare the rowCollection and unflat vectors (B1, A2).
        RowLayout rowLayout({{NODE, TypeUtils::getDataTypeSize(NODE), false /* isVectorOverflow */},
            {NODE, TypeUtils::getDataTypeSize(NODE), true /* isVectorOverflow */}});
        auto rowCollection = make_unique<RowCollection>(*memoryManager, rowLayout);
        vector<shared_ptr<ValueVector>> vectorsToAppend = {
            resultSet->dataChunks[0]->valueVectors[0], resultSet->dataChunks[1]->valueVectors[0]};
        if (isAppendFlatVectorToOverFlowCol) {
            // Flat B1 will cause an exception in the append function, because we are trying to
            // append a flat valueVector to an overflow column
            resultSet->dataChunks[1]->state->currIdx = 1;
        }

        rowCollection->append(vectorsToAppend, 100);
        return rowCollection;
    }

public:
    unique_ptr<ResultSet> resultSet;
    unique_ptr<MemoryManager> memoryManager;
    vector<uint64_t> fieldIdsToScan = {0, 1};
};

TEST_F(RowCollectionTest, AppendAndReadOneRowAtATime) {
    // Prepare the rowCollection and vector B1 (flat), A2(unflat)
    RowLayout rowLayout({{NODE, TypeUtils::getDataTypeSize(NODE), false /* isVectorOverflow */},
        {INT64, sizeof(overflow_value_t), true /* isVectorOverflow */}});
    auto rowCollection = make_unique<RowCollection>(*memoryManager, rowLayout);
    resultSet->dataChunks[1]->state->currIdx = 0;
    vector<shared_ptr<ValueVector>> vectorsToAppend = {
        resultSet->dataChunks[1]->valueVectors[0], resultSet->dataChunks[0]->valueVectors[1]};

    // Append B1, A2 to the rowCollection where B1 is an unflat valueVector and A2 is a flat
    // valueVector. The first column in the rowCollection stores the values of B1, and the second
    // column stores the overflow pointer to an overflow buffer which contains the values of A2.
    for (auto i = 0u; i < 100; i++) {
        rowCollection->append(vectorsToAppend, 1);
        resultSet->dataChunks[1]->state->currIdx++;
    }

    // Prepare the valueVectors where B1 is flat and A2 is unflat. We will read the first column to
    // B1, and the second column to A2.
    vector<DataPos> readDataPos = {{1, 0}, {0, 1}};
    auto readResultSet = initResultSet();
    readResultSet->dataChunks[0]->state->currIdx = -1;
    readResultSet->dataChunks[1]->state->currIdx = 0;
    auto vectorB1 = readResultSet->dataChunks[1]->valueVectors[0];
    auto vectorB1Data = (nodeID_t*)vectorB1->values;

    for (auto i = 0u; i < 100; i++) {
        // Since A2 is an overflow column, we can only read one row from rowCollection at a time.
        auto numRowsRead = rowCollection->scan(fieldIdsToScan, readDataPos, *readResultSet, i, 1);
        ASSERT_EQ(numRowsRead, 1);
        ASSERT_EQ(readResultSet->dataChunks[0]->state->selectedSize, 100);
        ASSERT_EQ(readResultSet->dataChunks[1]->state->selectedSize, 1);
        if (i % 10) {
            ASSERT_EQ(vectorB1->isNull(i), false);
            ASSERT_EQ(vectorB1Data[vectorB1->state->currIdx].label, 28);
            ASSERT_EQ(vectorB1Data[vectorB1->state->currIdx].offset, (uint64_t)i);
        } else {
            ASSERT_EQ(vectorB1->isNull(i), true);
        }
        checkA2OverflowValuesAndNulls(readResultSet->dataChunks[0]->valueVectors[1]);
        readResultSet->dataChunks[1]->state->currIdx++;
    }
}

TEST_F(RowCollectionTest, AppendMultipleRowsReadOneAtAtime) {
    // Prepare the append rowCollection and unflat vectors (B1, A2).
    RowLayout rowLayout({{NODE, TypeUtils::getDataTypeSize(NODE), false /* isVectorOverflow */},
        {INT64, sizeof(overflow_value_t), true /* isVectorOverflow */}});
    auto rowCollection = make_unique<RowCollection>(*memoryManager, rowLayout);
    vector<shared_ptr<ValueVector>> vectorsToAppend = {
        resultSet->dataChunks[0]->valueVectors[0], resultSet->dataChunks[0]->valueVectors[1]};

    // The first column in the rowCollection stores the values of B1, and the second column stores
    // the same valuePtr which points to the overflow buffer of A2
    rowCollection->append(vectorsToAppend, 100);

    // Prepare the valueVectors where B1 and A2 are both unflat. We will read the first column to
    // B1, and the second column to A2.
    vector<DataPos> readDataPos = {{1, 0}, {0, 1}};
    auto readResultSet = initResultSet();
    auto vectorB1 = readResultSet->dataChunks[1]->valueVectors[0];
    auto vectorB1Data = (nodeID_t*)vectorB1->values;

    for (auto i = 0u; i < 100; i++) {
        // Since A2 is an overflow column, we can only read one row from rowCollection at a time.
        auto numRowsRead = rowCollection->scan(fieldIdsToScan, readDataPos, *readResultSet, i, 1);
        ASSERT_EQ(numRowsRead, 1);
        ASSERT_EQ(readResultSet->dataChunks[0]->state->selectedSize, 100);
        ASSERT_EQ(readResultSet->dataChunks[1]->state->selectedSize, 1);
        // Since we only read one row at a time, we can only read one value from the nonOverflow
        // column
        if (i % 15) {
            ASSERT_EQ(vectorB1->isNull(0), false);
            ASSERT_EQ(vectorB1Data[0].label, 18);
            ASSERT_EQ(vectorB1Data[0].offset, (uint64_t)i);
        } else {
            ASSERT_EQ(vectorB1->isNull(0), true);
        }
        checkA2OverflowValuesAndNulls(readResultSet->dataChunks[0]->valueVectors[1]);
    }
}

TEST_F(RowCollectionTest, AppendMultipleRowsReadMultipleRowsWithOverflows) {
    auto rowCollection = appendMultipleRows(false /* isAppendFlatVectorToOverFlowCol */);

    // Prepare the read valueVectors where A1 is flat and B1 is unflat.
    vector<DataPos> readDataPos = {{0, 0}, {1, 0}};
    auto readResultSet = initResultSet();
    readResultSet->dataChunks[0]->state->currIdx = 0;
    auto vectorA1 = readResultSet->dataChunks[0]->valueVectors[0];
    auto vectorB1 = readResultSet->dataChunks[1]->valueVectors[0];
    auto vectorB1Data = (nodeID_t*)vectorB1->values;

    // If there is an overflow column, and we are trying to read more than one row,
    // the scan function will just read one row from the rowCollection instead of 100 rows.
    // A1 will only contain one element, and B1 will contain 100 elements since B1 is an overflow
    // column.
    auto numRowsRead = rowCollection->scan(fieldIdsToScan, readDataPos, *readResultSet, 0, 100);
    ASSERT_EQ(numRowsRead, 1);
    ASSERT_EQ(readResultSet->dataChunks[0]->state->selectedSize, 1);
    ASSERT_EQ(readResultSet->dataChunks[1]->state->selectedSize, 100);
    ASSERT_EQ(vectorA1->isNull(0), true);
    for (auto i = 0u; i < 100; i++) {
        if (i % 10) {
            ASSERT_EQ(vectorB1->isNull(i), false);
            ASSERT_EQ(vectorB1Data[i].label, 28);
            ASSERT_EQ(vectorB1Data[i].offset, (uint64_t)(i));
        } else {
            ASSERT_EQ(vectorB1->isNull(i), true);
        }
    }
}

TEST_F(RowCollectionTest, AppendMultipleRowsReadMultipleRowsNoOverflow) {
    // Prepare the rowCollection, valueVector A1(flat) and B1(unflat)
    RowLayout rowLayout({{NODE, TypeUtils::getDataTypeSize(NODE), false /* isVectorOverflow */},
        {NODE, TypeUtils::getDataTypeSize(NODE), false /* isVectorOverflow */}});
    auto rowCollection = make_unique<RowCollection>(*memoryManager, rowLayout);
    resultSet->dataChunks[0]->state->currIdx = 10;
    vector<shared_ptr<ValueVector>> vectorsToAppend = {
        resultSet->dataChunks[0]->valueVectors[0], resultSet->dataChunks[1]->valueVectors[0]};

    // The valueVectorA1 will be replicated 100 times, since B1 is unflat and not overflow
    rowCollection->append(vectorsToAppend, 100);

    // Prepare the rowCollection where both A1 and B1 are unflat because we will read multiple rows
    // from rowCollection
    vector<DataPos> readDataPos = {{0, 0}, {1, 0}};
    auto readResultSet = initResultSet();
    auto vectorB1 = readResultSet->dataChunks[1]->valueVectors[0];
    auto vectorA1Data = (nodeID_t*)readResultSet->dataChunks[0]->valueVectors[0]->values;
    auto vectorB1Data = (nodeID_t*)vectorB1->values;

    // we can read multiple rows at a time if the read vectors are unflat, and there is no overflow
    // column to read
    auto numRowsRead = rowCollection->scan(fieldIdsToScan, readDataPos, *readResultSet, 0, 100);
    ASSERT_EQ(numRowsRead, 100);
    ASSERT_EQ(readResultSet->dataChunks[0]->state->selectedSize, 100);
    ASSERT_EQ(readResultSet->dataChunks[1]->state->selectedSize, 100);
    for (auto i = 0u; i < 100; i++) {
        // A1 data is duplicated 100 times during append, so each entry of A1 is exactly the same
        ASSERT_EQ(vectorA1Data[i].label, 18);
        ASSERT_EQ(vectorA1Data[i].offset, (uint64_t)10);
        if (i % 10) {
            ASSERT_EQ(vectorB1->isNull(i), false);
            ASSERT_EQ(vectorB1Data[i].label, 28);
            ASSERT_EQ(vectorB1Data[i].offset, (uint64_t)(i));
        } else {
            ASSERT_EQ(vectorB1->isNull(i), true);
        }
    }
}

TEST_F(RowCollectionTest, AppendFlatVectorToOverflowColFails) {
    // Users are not allowed to append a flat vector to an overflow column
    try {
        appendMultipleRows(true /* isAppendFlatVectorToOverFlowCol */);
        FAIL();
    } catch (RowCollectionException& e) {
        ASSERT_STREQ(e.what(), "RowCollection exception: Append an unflat vector to an overflow "
                               "column is not allowed!");
    } catch (exception& e) { FAIL(); }
}

TEST_F(RowCollectionTest, ReadOverflowColToFlatVectorFails) {
    // The first column is not overflow, the second column is an overflow column.
    auto rowCollection = appendMultipleRows(false /* isAppendFlatVectorToOverFlowCol */);

    // Prepare the read valueVector where A1 is unflat and B1 is flat
    auto readResultSet = initResultSet();
    vector<DataPos> readDataPos = {{0, 0}, {1, 0}};
    readResultSet->dataChunks[1]->state->currIdx = 0;

    // Users are not allowed to read an overflow column to a flat valueVector
    try {
        auto numRowsRead = rowCollection->scan(fieldIdsToScan, readDataPos, *readResultSet, 0, 100);
        FAIL();
    } catch (RowCollectionException& e) {
        ASSERT_STREQ(e.what(), "RowCollection exception: Read an overflow column to a flat "
                               "valueVector is not allowed!");
    } catch (exception& e) { FAIL(); }
}

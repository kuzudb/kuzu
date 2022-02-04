#include "gtest/gtest.h"

#include "src/common/include/exception.h"
#include "src/processor/include/physical_plan/result/factorized_table.h"

using namespace graphflow::processor;

class FactorizedTableTest : public ::testing::Test {

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

    static void checkA2UnflatValuesAndNulls(shared_ptr<ValueVector> vectorA2) {
        for (auto i = 0u; i < 100; i++) {
            if (i % 10) {
                ASSERT_EQ(vectorA2->isNull(i), false);
                ASSERT_EQ(((int64_t*)vectorA2->values)[i], i << 1);
            } else {
                ASSERT_EQ(vectorA2->isNull(i), true);
            }
        }
    }

    unique_ptr<FactorizedTable> appendMultipleTuples(bool isAppendFlatVectorToUnflatCol) {
        // Prepare the factorizedTable and unflat vectors (A1, B1).
        TupleSchema tupleSchema({{NODE, false /* isUnflat */, 0 /* dataChunkPos */},
            {NODE, true /* isUnflat */, 1 /* dataChunkPos */}});
        auto factorizedTable = make_unique<FactorizedTable>(*memoryManager, tupleSchema);
        vector<shared_ptr<ValueVector>> vectorsToAppend = {
            resultSet->dataChunks[0]->valueVectors[0], resultSet->dataChunks[1]->valueVectors[0]};
        if (isAppendFlatVectorToUnflatCol) {
            // Flat B1 will cause an exception in the append function, because we are trying to
            // append a flat valueVector to an unflat column
            resultSet->dataChunks[1]->state->currIdx = 1;
        }

        factorizedTable->append(vectorsToAppend, 100 /* numTuplesToAppend */);
        return factorizedTable;
    }

    void checkFlatTupleIteratorResult(FlatTupleIterator& flatTupleIterator) {
        for (auto i = 0; i < 100; i++) {
            for (auto j = 0; j < 100; j++) {
                ASSERT_EQ(flatTupleIterator.hasNextFlatTuple(), true);
                auto resultFlatTuple = flatTupleIterator.getNextFlatTuple();
                if (i % 15) {
                    ASSERT_EQ(resultFlatTuple.nullMask[0], false);
                    auto val = resultFlatTuple.getValue(0)->val.nodeID;
                    ASSERT_EQ(val.label, 18);
                    ASSERT_EQ(val.offset, i);
                } else {
                    ASSERT_EQ(resultFlatTuple.nullMask[0], true);
                }
                if (j % 10) {
                    ASSERT_EQ(resultFlatTuple.nullMask[1], false);
                    auto val = resultFlatTuple.getValue(1)->val.nodeID;
                    ASSERT_EQ(val.label, 28);
                    ASSERT_EQ(val.offset, j);
                } else {
                    ASSERT_EQ(resultFlatTuple.nullMask[1], true);
                }
            }
        }
    }

public:
    unique_ptr<ResultSet> resultSet;
    unique_ptr<MemoryManager> memoryManager;
    vector<uint64_t> columnIdxesToScan = {0, 1};
};

TEST_F(FactorizedTableTest, AppendAndReadOneTupleAtATime) {
    // Prepare the factorizedTable and vector B1 (flat), A2(unflat)
    TupleSchema tupleSchema({{NODE, false /* isUnflat */, 1 /* dataChunkPos */},
        {INT64, true /* isUnflat */, 0 /* dataChunkPos */}});
    auto factorizedTable = make_unique<FactorizedTable>(*memoryManager, tupleSchema);
    resultSet->dataChunks[1]->state->currIdx = 0;
    vector<shared_ptr<ValueVector>> vectorsToAppend = {
        resultSet->dataChunks[1]->valueVectors[0], resultSet->dataChunks[0]->valueVectors[1]};

    // Append B1, A2 to the factorizedTable where B1 is an unflat valueVector and A2 is a flat
    // valueVector. The first column in the factorizedTable stores the values of B1, and the second
    // column stores the overflow pointer to an overflow buffer which contains the values of A2.
    for (auto i = 0u; i < 100; i++) {
        factorizedTable->append(vectorsToAppend, 1);
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
        // Since A2 is an unflat column, we can only read one tuple from factorizedTable at a time.
        auto numTuplesRead =
            factorizedTable->scan(columnIdxesToScan, readDataPos, *readResultSet, i, 1);
        ASSERT_EQ(numTuplesRead, 1);
        ASSERT_EQ(readResultSet->dataChunks[0]->state->selectedSize, 100);
        ASSERT_EQ(readResultSet->dataChunks[1]->state->selectedSize, 1);
        if (i % 10) {
            ASSERT_EQ(vectorB1->isNull(i), false);
            ASSERT_EQ(vectorB1Data[vectorB1->state->currIdx].label, 28);
            ASSERT_EQ(vectorB1Data[vectorB1->state->currIdx].offset, (uint64_t)i);
        } else {
            ASSERT_EQ(vectorB1->isNull(i), true);
        }
        checkA2UnflatValuesAndNulls(readResultSet->dataChunks[0]->valueVectors[1]);
        readResultSet->dataChunks[1]->state->currIdx++;
    }
}

TEST_F(FactorizedTableTest, AppendMultipleTuplesReadOneAtAtime) {
    // Prepare the append factorizedTable and unflat vectors (B1, A2).
    TupleSchema tupleSchema({{NODE, false /* isUnflat */, 0 /* dataChunkPos */},
        {INT64, true /* isUnflat */, 0 /* dataChunkPos */}});
    auto factorizedTable = make_unique<FactorizedTable>(*memoryManager, tupleSchema);
    vector<shared_ptr<ValueVector>> vectorsToAppend = {
        resultSet->dataChunks[0]->valueVectors[0], resultSet->dataChunks[0]->valueVectors[1]};

    // The first column in the factorizedTable stores the values of B1, and the second column stores
    // the same valuePtr which points to the overflow buffer of A2
    factorizedTable->append(vectorsToAppend, 100);

    // Prepare the valueVectors where B1 and A2 are both unflat. We will read the first column to
    // B1, and the second column to A2.
    vector<DataPos> readDataPos = {{1, 0}, {0, 1}};
    auto readResultSet = initResultSet();
    auto vectorB1 = readResultSet->dataChunks[1]->valueVectors[0];
    auto vectorB1Data = (nodeID_t*)vectorB1->values;

    for (auto i = 0u; i < 100; i++) {
        // Since A2 is an unflat column, we can only read one tuple from factorizedTable at a time.
        auto numTuplesRead =
            factorizedTable->scan(columnIdxesToScan, readDataPos, *readResultSet, i, 1);
        ASSERT_EQ(numTuplesRead, 1);
        ASSERT_EQ(readResultSet->dataChunks[0]->state->selectedSize, 100);
        ASSERT_EQ(readResultSet->dataChunks[1]->state->selectedSize, 1);
        // Since we only read one tuple at a time, we can only read one value from the flat
        // column
        if (i % 15) {
            ASSERT_EQ(vectorB1->isNull(0), false);
            ASSERT_EQ(vectorB1Data[0].label, 18);
            ASSERT_EQ(vectorB1Data[0].offset, (uint64_t)i);
        } else {
            ASSERT_EQ(vectorB1->isNull(0), true);
        }
        checkA2UnflatValuesAndNulls(readResultSet->dataChunks[0]->valueVectors[1]);
    }
}

TEST_F(FactorizedTableTest, AppendMultipleTuplesReadMultipleTuplesWithUnflat) {
    auto factorizedTable = appendMultipleTuples(false /* isAppendFlatVectorToUnflatCol */);

    // Prepare the read valueVectors where A1 is flat and B1 is unflat.
    vector<DataPos> readDataPos = {{0, 0}, {1, 0}};
    auto readResultSet = initResultSet();
    readResultSet->dataChunks[0]->state->currIdx = 0;
    auto vectorA1 = readResultSet->dataChunks[0]->valueVectors[0];
    auto vectorB1 = readResultSet->dataChunks[1]->valueVectors[0];
    auto vectorB1Data = (nodeID_t*)vectorB1->values;

    // If there is an unflat column, and we are trying to read more than one tuple,
    // the scan function will just read one tuple from the factorizedTable instead of 100 tuples.
    // A1 will only contain one element, and B1 will contain 100 elements since B1 is an unflat
    // column.
    auto numTuplesRead =
        factorizedTable->scan(columnIdxesToScan, readDataPos, *readResultSet, 0, 100);
    ASSERT_EQ(numTuplesRead, 1);
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

TEST_F(FactorizedTableTest, AppendMultipleTuplesReadMultipleTuplesNoUnflat) {
    // Prepare the factorizedTable, valueVector A1(flat) and B1(unflat)
    TupleSchema tupleSchema({{NODE, false /* isUnflat */, 0 /* dataChunkPos */},
        {NODE, false /* isUnflat */, 1 /* dataChunkPos */}});
    auto factorizedTable = make_unique<FactorizedTable>(*memoryManager, tupleSchema);
    resultSet->dataChunks[0]->state->currIdx = 10;
    vector<shared_ptr<ValueVector>> vectorsToAppend = {
        resultSet->dataChunks[0]->valueVectors[0], resultSet->dataChunks[1]->valueVectors[0]};

    // The valueVectorA1 will be replicated 100 times, since B1 is unflat
    factorizedTable->append(vectorsToAppend, 100);

    // Prepare the factorizedTable where both A1 and B1 are unflat because we will read multiple
    // tuples from factorizedTable
    vector<DataPos> readDataPos = {{0, 0}, {1, 0}};
    auto readResultSet = initResultSet();
    auto vectorB1 = readResultSet->dataChunks[1]->valueVectors[0];
    auto vectorA1Data = (nodeID_t*)readResultSet->dataChunks[0]->valueVectors[0]->values;
    auto vectorB1Data = (nodeID_t*)vectorB1->values;

    // we can read multiple tuples at a time if the read vectors are unflat, and there is no unflat
    // column to read
    auto numTuplesRead =
        factorizedTable->scan(columnIdxesToScan, readDataPos, *readResultSet, 0, 100);
    ASSERT_EQ(numTuplesRead, 100);
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

TEST_F(FactorizedTableTest, AppendFlatVectorToUnflatColFails) {
    // Users are not allowed to append a flat vector to an unflat column
    try {
        appendMultipleTuples(true /* isAppendFlatVectorToUnflatCol */);
        FAIL();
    } catch (FactorizedTableException& e) {
        ASSERT_STREQ(e.what(), "FactorizedTable exception: Append a flat vector to an unflat "
                               "column is not allowed!");
    } catch (exception& e) { FAIL(); }
}

TEST_F(FactorizedTableTest, ReadUnflatColToFlatVectorFails) {
    // The first column is flat, the second column is unflat.
    auto factorizedTable = appendMultipleTuples(false /* isAppendFlatVectorToUnflatCol */);

    // Prepare the read valueVector where A1 is unflat and B1 is flat
    auto readResultSet = initResultSet();
    vector<DataPos> readDataPos = {{0, 0}, {1, 0}};
    readResultSet->dataChunks[1]->state->currIdx = 0;

    // Users are not allowed to read an unflat column to a flat valueVector
    try {
        auto numTuplesRead =
            factorizedTable->scan(columnIdxesToScan, readDataPos, *readResultSet, 0, 100);
        FAIL();
    } catch (FactorizedTableException& e) {
        ASSERT_STREQ(e.what(), "FactorizedTable exception: Reading an unflat column to a flat "
                               "valueVector is not allowed!");
    } catch (exception& e) { FAIL(); }
}

TEST_F(FactorizedTableTest, ReadFlatTuplesFromFactorizedTable) {
    auto factorizedTable = appendMultipleTuples(false /* isAppendFlatVectorToUnflatCol */);
    auto flatTupleIterator = factorizedTable->getFlatTuples();
    checkFlatTupleIteratorResult(flatTupleIterator);
    ASSERT_EQ(flatTupleIterator.hasNextFlatTuple(), false);
}

TEST_F(FactorizedTableTest, factorizedTableMergeTest) {
    auto factorizedTable = appendMultipleTuples(false /* isAppendFlatVectorToUnflatCol */);
    auto factorizedTable1 = appendMultipleTuples(false /* isAppendFlatVectorToUnflatCol */);
    factorizedTable->merge(*factorizedTable1);
    auto flatTupleIterator = factorizedTable->getFlatTuples();
    checkFlatTupleIteratorResult(flatTupleIterator);
    checkFlatTupleIteratorResult(flatTupleIterator);
    ASSERT_EQ(flatTupleIterator.hasNextFlatTuple(), false);
}

TEST_F(FactorizedTableTest, factorizedTableMergeStringBufferTest) {
    auto numRowsToAppend = 1000;
    auto resultSet = make_unique<ResultSet>(1);
    auto dataChunk = make_shared<DataChunk>(1);
    dataChunk->state->selectedSize = numRowsToAppend;
    dataChunk->state->currIdx = 0;
    shared_ptr<ValueVector> strValueVector = make_shared<ValueVector>(memoryManager.get(), STRING);
    resultSet->insert(0, dataChunk);
    dataChunk->insert(0, strValueVector);
    for (auto i = 0u; i < numRowsToAppend; i++) {
        strValueVector->addString(i, to_string(i) + "with long string overflow");
    }
    TupleSchema tupleSchema({
        {STRING, false /* isUnflat */, 0 /* dataChunkPos */},
    });
    auto factorizedTable = make_unique<FactorizedTable>(*memoryManager, tupleSchema);
    auto factorizedTable1 = make_unique<FactorizedTable>(*memoryManager, tupleSchema);

    // Append same testing data to factorizedTable and factorizedTable1.
    for (auto i = 0u; i < numRowsToAppend; i++) {
        factorizedTable->append({resultSet->dataChunks[0]->valueVectors[0]}, 1);
        factorizedTable1->append({resultSet->dataChunks[0]->valueVectors[0]}, 1);
        dataChunk->state->currIdx++;
    }

    factorizedTable->merge(*factorizedTable1);
    // Test whether the tuples in factorizedTable are still valid after factorizedTable1 is
    // destructed.
    factorizedTable1.reset();
    // Test whether the string that are inserted after merging will be stored correctly in the
    // stringBuffer.
    dataChunk->state->currIdx = 0;
    for (auto i = 0u; i < numRowsToAppend; i++) {
        factorizedTable->append({resultSet->dataChunks[0]->valueVectors[0]}, 1);
        dataChunk->state->currIdx++;
    }

    auto flatTupleIterator = factorizedTable->getFlatTuples();
    for (auto i = 0; i < 3 * numRowsToAppend; i++) {
        ASSERT_EQ(flatTupleIterator.hasNextFlatTuple(), true);
        ASSERT_EQ(flatTupleIterator.getNextFlatTuple().getValue(0)->val.strVal.getAsString(),
            to_string(i % numRowsToAppend) + "with long string overflow");
    }
    ASSERT_EQ(flatTupleIterator.hasNextFlatTuple(), false);
}

#include "gtest/gtest.h"

#include "src/common/include/exception.h"
#include "src/processor/include/physical_plan/result/factorized_table.h"

using namespace graphflow::processor;

class FactorizedTableTest : public ::testing::Test {

public:
    unique_ptr<ResultSet> initResultSet() {
        auto result = make_unique<ResultSet>(2);
        auto dataChunkA = make_shared<DataChunk>(2);
        auto dataChunkB = make_shared<DataChunk>(2);
        auto vectorA1 = make_shared<ValueVector>(memoryManager.get(), NODE_ID);
        auto vectorA2 = make_shared<ValueVector>(memoryManager.get(), INT64);
        auto vectorB1 = make_shared<ValueVector>(memoryManager.get(), NODE_ID);
        auto vectorB2 = make_shared<ValueVector>(memoryManager.get(), DOUBLE);
        dataChunkA->insert(0, vectorA1);
        dataChunkA->insert(1, vectorA2);
        dataChunkB->insert(0, vectorB1);
        dataChunkB->insert(1, vectorB2);
        result->insert(0, dataChunkA);
        result->insert(1, dataChunkB);
        return result;
    }

    void SetUp() override {
        bufferManager = make_unique<BufferManager>();
        memoryManager = make_unique<MemoryManager>(bufferManager.get());
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
        resultSet->dataChunks[0]->state->selVector->selectedSize = 100;
        resultSet->dataChunks[1]->state->selVector->selectedSize = 100;
    }

    unique_ptr<FactorizedTable> appendMultipleTuples(bool isAppendFlatVectorToUnflatCol) {
        // Prepare the factorizedTable and unflat vectors (A1, B1).
        unique_ptr<TableSchema> tableSchema = make_unique<TableSchema>();
        tableSchema->appendColumn(make_unique<ColumnSchema>(
            false /* isUnflat */, 0 /* dataChunkPos */, sizeof(nodeID_t)));
        tableSchema->appendColumn(make_unique<ColumnSchema>(
            true /* isUnflat */, 1 /* dataChunkPos */, sizeof(overflow_value_t)));
        auto factorizedTable = make_unique<FactorizedTable>(memoryManager.get(), move(tableSchema));
        vector<shared_ptr<ValueVector>> vectorsToAppend = {
            resultSet->dataChunks[0]->valueVectors[0], resultSet->dataChunks[1]->valueVectors[0]};
        if (isAppendFlatVectorToUnflatCol) {
            // Flat B1 will cause an exception in the append function, because we are trying
            // to append a flat valueVector to an unflat column
            resultSet->dataChunks[1]->state->currIdx = 1;
        }
        // Since the first column, which stores A1, is a flat column in factorizedTable, the
        // factorizedTable will automatically flatten A1 and B1 will be replicated 100 times(the
        // size of A1).
        factorizedTable->append(vectorsToAppend);
        return factorizedTable;
    }

    static void checkFlatTupleIteratorResult(FlatTupleIterator& flatTupleIterator) {
        for (auto i = 0; i < 100; i++) {
            for (auto j = 0; j < 100; j++) {
                vector<DataType> dataTypesInFlatTuple = {DataType(NODE_ID), DataType(NODE_ID)};
                ASSERT_EQ(flatTupleIterator.hasNextFlatTuple(), true);
                auto& resultFlatTuple = *flatTupleIterator.getNextFlatTuple();
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
    unique_ptr<BufferManager> bufferManager;
    unique_ptr<MemoryManager> memoryManager;
    unique_ptr<ResultSet> resultSet;
};

TEST_F(FactorizedTableTest, AppendAndScanOneTupleAtATime) {
    // Prepare the factorizedTable and vector B1 (flat), A2(unflat).
    unique_ptr<TableSchema> tableSchema = make_unique<TableSchema>();
    tableSchema->appendColumn(
        make_unique<ColumnSchema>(false /* isUnflat */, 1 /* dataChunkPos */, sizeof(nodeID_t)));
    tableSchema->appendColumn(make_unique<ColumnSchema>(
        true /* isUnflat */, 0 /* dataChunkPos */, sizeof(overflow_value_t)));
    auto factorizedTable = make_unique<FactorizedTable>(memoryManager.get(), move(tableSchema));
    resultSet->dataChunks[1]->state->currIdx = 0;
    vector<shared_ptr<ValueVector>> vectorsToAppend = {
        resultSet->dataChunks[1]->valueVectors[0], resultSet->dataChunks[0]->valueVectors[1]};

    // Append B1, A2 to the factorizedTable where B1 is a flat valueVector and A2 is an unflat
    // valueVector. The first column in the factorizedTable stores the values of B1, and the
    // second column stores the overflow pointer to an overflow buffer which contains the values
    // of A2.
    for (auto i = 0u; i < 100; i++) {
        factorizedTable->append(vectorsToAppend);
        resultSet->dataChunks[1]->state->currIdx++;
    }

    // Prepare the valueVectors where B1 is flat and A2 is unflat. We will read the first column
    // to B1, and the second column to A2.
    auto readResultSet = initResultSet();
    readResultSet->dataChunks[0]->state->currIdx = -1;
    auto dataChunk1 = readResultSet->dataChunks[1];
    dataChunk1->state = DataChunkState::getSingleValueDataChunkState();
    for (auto& vector : dataChunk1->valueVectors) {
        vector->state = dataChunk1->state;
    }
    vector<shared_ptr<ValueVector>> vectorsToRead = {readResultSet->dataChunks[1]->valueVectors[0],
        readResultSet->dataChunks[0]->valueVectors[1]};
    auto vectorB1 = vectorsToRead[0];
    auto vectorB1Data = (nodeID_t*)vectorB1->values;
    auto vectorA2 = vectorsToRead[1];

    for (auto i = 0u; i < 100; i++) {
        // Since A2 is an unflat column, we can only read one tuple from factorizedTable at a
        // time.
        factorizedTable->scan(vectorsToRead, i, 1);
        ASSERT_EQ(readResultSet->dataChunks[0]->state->selVector->selectedSize, 100);
        ASSERT_EQ(readResultSet->dataChunks[1]->state->selVector->selectedSize, 1);
        if (i % 10) {
            ASSERT_EQ(vectorB1->isNull(i), false);
            ASSERT_EQ(vectorB1Data[vectorB1->state->currIdx].label, 28);
            ASSERT_EQ(vectorB1Data[vectorB1->state->currIdx].offset, (uint64_t)i);
        } else {
            ASSERT_EQ(vectorB1->isNull(i), true);
        }
        for (auto j = 0u; j < 100; j++) {
            if (j % 10) {
                ASSERT_EQ(vectorA2->isNull(j), false);
                ASSERT_EQ(((int64_t*)vectorA2->values)[j], j << 1);
            } else {
                ASSERT_EQ(vectorA2->isNull(j), true);
            }
        }
        readResultSet->dataChunks[1]->state->currIdx++;
    }
}

TEST_F(FactorizedTableTest, AppendMultipleTuplesScanOneAtAtime) {
    auto factorizedTable = appendMultipleTuples(false /* isAppendFlatVectorToUnflatCol */);
    ASSERT_EQ(
        factorizedTable->getNumTuples(), resultSet->dataChunks[0]->state->selVector->selectedSize);

    auto readResultSet = initResultSet();
    readResultSet->dataChunks[0]->state->currIdx = -1;
    readResultSet->dataChunks[1]->state->currIdx = -1;
    vector<shared_ptr<ValueVector>> vectorsToScan = {readResultSet->dataChunks[0]->valueVectors[0],
        readResultSet->dataChunks[1]->valueVectors[0]};
    auto vectorA1 = vectorsToScan[0];
    auto vectorA1Data = (nodeID_t*)vectorA1->values;
    auto vectorB1 = vectorsToScan[1];
    auto vectorB1Data = (nodeID_t*)vectorB1->values;

    for (auto i = 0u; i < 100; i++) {
        // Since B1 is an unflat column in factorizedTable , we can only read one tuple from
        // factorizedTable at a time.
        factorizedTable->scan(vectorsToScan, i, 1);
        ASSERT_EQ(readResultSet->dataChunks[0]->state->selVector->selectedSize, 1);
        ASSERT_EQ(readResultSet->dataChunks[1]->state->selVector->selectedSize, 100);
        if (i % 15) {
            ASSERT_EQ(vectorA1->isNull(0), false);
            ASSERT_EQ(vectorA1Data[0].label, 18);
            ASSERT_EQ(vectorA1Data[0].offset, (uint64_t)i);
        } else {
            ASSERT_EQ(vectorA1->isNull(0), true);
        }
        // Since B1 is duplicated 100 times in the factorizedTable, values in B1 are exactly the
        // same for each scan.
        for (auto j = 0u; j < 100; j++) {
            if (j % 10) {
                ASSERT_EQ(vectorB1->isNull(j), false);
                ASSERT_EQ(vectorB1Data[j].label, 28);
                ASSERT_EQ(vectorB1Data[j].offset, (uint64_t)j);
            } else {
                ASSERT_EQ(vectorB1->isNull(j), true);
            }
        }
    }
}

TEST_F(FactorizedTableTest, ReadFlatTuplesFromFactorizedTable) {
    auto factorizedTable = appendMultipleTuples(false /* isAppendFlatVectorToUnflatCol */);
    auto flatTupleIterator =
        FlatTupleIterator(*factorizedTable, vector<DataType>{DataType(NODE_ID), DataType(NODE_ID)});
    checkFlatTupleIteratorResult(flatTupleIterator);
    ASSERT_EQ(flatTupleIterator.hasNextFlatTuple(), false);
}

TEST_F(FactorizedTableTest, FactorizedTableMergeTest) {
    auto factorizedTable = appendMultipleTuples(false /* isAppendFlatVectorToUnflatCol */);
    auto factorizedTable1 = appendMultipleTuples(false /* isAppendFlatVectorToUnflatCol */);
    ASSERT_EQ(factorizedTable->getTotalNumFlatTuples(), 10000);
    ASSERT_EQ(factorizedTable1->getTotalNumFlatTuples(), 10000);
    factorizedTable->merge(*factorizedTable1);
    ASSERT_EQ(factorizedTable->getTotalNumFlatTuples(), 20000);
    auto flatTupleIterator =
        FlatTupleIterator(*factorizedTable, vector<DataType>{DataType(NODE_ID), DataType(NODE_ID)});
    checkFlatTupleIteratorResult(flatTupleIterator);
    checkFlatTupleIteratorResult(flatTupleIterator);
    ASSERT_EQ(flatTupleIterator.hasNextFlatTuple(), false);
}

TEST_F(FactorizedTableTest, FactorizedTableMergeOverflowBufferTest) {
    auto numRowsToAppend = 1000;
    auto resultSet = make_unique<ResultSet>(1);
    auto dataChunk = make_shared<DataChunk>(1);
    dataChunk->state->selVector->selectedSize = numRowsToAppend;
    dataChunk->state->currIdx = 0;
    shared_ptr<ValueVector> strValueVector = make_shared<ValueVector>(memoryManager.get(), STRING);
    resultSet->insert(0, dataChunk);
    dataChunk->insert(0, strValueVector);
    for (auto i = 0u; i < numRowsToAppend; i++) {
        strValueVector->addString(i, to_string(i) + "with long string overflow");
    }
    unique_ptr<TableSchema> tableSchema = make_unique<TableSchema>();
    tableSchema->appendColumn(
        make_unique<ColumnSchema>(false /* isUnflat */, 0 /* dataChunkPos */, sizeof(gf_string_t)));
    auto factorizedTable =
        make_unique<FactorizedTable>(memoryManager.get(), make_unique<TableSchema>(*tableSchema));
    auto factorizedTable1 = make_unique<FactorizedTable>(memoryManager.get(), move(tableSchema));

    // Append same testing data to factorizedTable and factorizedTable1.
    for (auto i = 0u; i < numRowsToAppend; i++) {
        factorizedTable->append({resultSet->dataChunks[0]->valueVectors[0]});
        factorizedTable1->append({resultSet->dataChunks[0]->valueVectors[0]});
        dataChunk->state->currIdx++;
    }

    factorizedTable->merge(*factorizedTable1);
    // Test whether the tuples in factorizedTable are still valid after factorizedTable1 is
    // destructed.
    factorizedTable1.reset();
    // Test whether the string that are inserted after merging will be stored correctly in the
    // overflowBuffer.
    dataChunk->state->currIdx = 0;
    for (auto i = 0u; i < numRowsToAppend; i++) {
        factorizedTable->append({resultSet->dataChunks[0]->valueVectors[0]});
        dataChunk->state->currIdx++;
    }

    vector<DataType> dataTypes = {DataType(STRING)};
    auto flatTupleIterator = FlatTupleIterator(*factorizedTable, dataTypes);
    for (auto i = 0; i < 3 * numRowsToAppend; i++) {
        ASSERT_EQ(flatTupleIterator.hasNextFlatTuple(), true);
        auto resultFlatTuple = flatTupleIterator.getNextFlatTuple();
        ASSERT_EQ(resultFlatTuple->getValue(0)->val.strVal.getAsString(),
            to_string(i % numRowsToAppend) + "with long string overflow");
    }
    ASSERT_EQ(flatTupleIterator.hasNextFlatTuple(), false);
}

#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

class RelInsertionTest : public DBTest {

public:
    void SetUp() override {
        bufferManager = make_unique<BufferManager>();
        memoryManager = make_unique<MemoryManager>(bufferManager.get());
        DBTest::SetUp();
        relIDProperty = make_shared<ValueVector>(INT64, memoryManager.get());
        relIDValues = (int64_t*)relIDProperty->values;
        lengthProperty = make_shared<ValueVector>(INT64, memoryManager.get());
        lengthValues = (int64_t*)lengthProperty->values;
        placeProperty = make_shared<ValueVector>(STRING, memoryManager.get());
        placeValues = (gf_string_t*)placeProperty->values;
        srcNodeVector = make_shared<ValueVector>(NODE_ID, memoryManager.get());
        dstNodeVector = make_shared<ValueVector>(NODE_ID, memoryManager.get());
        tagProperty = make_shared<ValueVector>(
            DataType{LIST, make_unique<DataType>(STRING)}, memoryManager.get());
        tagValues = (gf_list_t*)tagProperty->values;
        dataChunk->insert(0, relIDProperty);
        dataChunk->insert(1, lengthProperty);
        dataChunk->insert(2, placeProperty);
        dataChunk->insert(3, tagProperty);
        dataChunk->insert(4, srcNodeVector);
        dataChunk->insert(5, dstNodeVector);
        dataChunk->state->currIdx = 0;
    }

    void commitOrRollbackConnectionAndInitDBIfNecessary(
        bool isCommit, TransactionTestType transactionTestType) {
        commitOrRollbackConnection(isCommit, transactionTestType);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            createDBAndConn();
        }
    }

    string getInputCSVDir() override { return "dataset/rel-insertion-tests/"; }

    void insertToRelUpdateStore() {
        database->getStorageManager()
            ->getRelsStore()
            .getRel(0 /* relTableID */)
            ->getRelUpdateStore()
            ->addRel(vector<shared_ptr<ValueVector>>{srcNodeVector, dstNodeVector, lengthProperty,
                placeProperty, tagProperty, relIDProperty});
    }

    void insertRels(table_id_t srcTableID, table_id_t dstTableID, uint64_t numValuesToInsert = 100,
        bool insertNullValues = false, bool testLongString = false) {
        auto placeStr = gf_string_t();
        if (testLongString) {
            placeStr.overflowPtr =
                reinterpret_cast<uint64_t>(placeProperty->getOverflowBuffer().allocateSpace(100));
        }
        auto tagStr = gf_list_t();
        tagStr.overflowPtr =
            reinterpret_cast<uint64_t>(tagProperty->getOverflowBuffer().allocateSpace(100));
        tagStr.size = 1;
        for (auto i = 0u; i < numValuesToInsert; i++) {
            relIDValues[0] = 1051 + i;
            lengthValues[0] = i;
            placeStr.set((testLongString ? "long long string prefix " : "") + to_string(i));
            placeValues[0] = placeStr;
            tagStr.set((uint8_t*)&placeStr, DataType(LIST, make_unique<DataType>(STRING)));
            tagValues[0] = tagStr;
            if (insertNullValues) {
                lengthProperty->setNull(0, i % 2);
                placeProperty->setNull(0, true /* isNull */);
            }
            ((nodeID_t*)srcNodeVector->values)[0] = nodeID_t(1, srcTableID);
            ((nodeID_t*)dstNodeVector->values)[0] = nodeID_t(i + 1, dstTableID);
            insertToRelUpdateStore();
        }
    }

    void incorrectVectorErrorTest(
        vector<shared_ptr<ValueVector>> srcDstNodeIDAndRelProperties, string errorMsg) {
        try {
            database->getStorageManager()
                ->getRelsStore()
                .getRel(0 /* relTableID */)
                ->getRelUpdateStore()
                ->addRel(srcDstNodeIDAndRelProperties);
            FAIL();
        } catch (InternalException& exception) {
            ASSERT_EQ(exception.what(), errorMsg);
        } catch (Exception& e) { FAIL(); }
    }

    static void validateInsertedRels(QueryResult* queryResult, uint64_t numInsertedRels,
        bool containNullValues, bool containLongString) {
        for (auto i = 0u; i < numInsertedRels; i++) {
            auto tuple = queryResult->getNext();
            ASSERT_EQ(tuple->isNull(0), containNullValues && (i % 2 != 0));
            if (!containNullValues || i % 2 == 0) {
                ASSERT_EQ(tuple->getValue(0)->val.int64Val, i);
            }
            ASSERT_EQ(tuple->isNull(1), containNullValues);
            if (!containNullValues) {
                ASSERT_EQ(tuple->getValue(1)->val.strVal.getAsString(),
                    (containLongString ? "long long string prefix " : "") + to_string(i));
            }
            ASSERT_EQ(((gf_string_t*)tuple->getValue(2)->val.listVal.overflowPtr)[0].getAsString(),
                (containLongString ? "long long string prefix " : "") + to_string(i));
        }
    }

    void insertRelsToEmptyListTest(bool isCommit, TransactionTestType transactionTestType,
        uint64_t numRelsToInsert = 100, bool insertNullValues = false,
        bool testLongString = false) {
        insertRels(0 /* srcTableID */, 0 /* dstTableID */, numRelsToInsert, insertNullValues,
            testLongString);
        conn->beginWriteTransaction();
        auto result =
            conn->query("match (a:animal)-[e:knows]->(b:animal) return e.length, e.place, e.tag");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), numRelsToInsert);
        validateInsertedRels(result.get(), numRelsToInsert, insertNullValues, testLongString);
        // Note that the queryResult contains a factorizedTable whose dataBlock is allocated from
        // memoryManager. We must unpin the dataBlock's page before restarting the database, that
        // is to say, we need to manually release the factorizedTable before testing recovery.
        result.reset();
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        result =
            conn->query("match (a:animal)-[e:knows]->(b:animal) return e.length, e.place, e.tag");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), isCommit ? numRelsToInsert : 0);
        if (isCommit) {
            validateInsertedRels(result.get(), numRelsToInsert, insertNullValues, testLongString);
        }
    }

    void validateListAfterInsertion(QueryResult* result, uint64_t numInsertedRels) {
        for (auto i = 0u; i < 51; i++) {
            auto tuple = result->getNext();
            ASSERT_EQ(tuple->getValue(0)->val.int64Val, i);
            ASSERT_EQ(tuple->getValue(1)->val.strVal.getAsString(), to_string(1000 - i));
            ASSERT_EQ(((gf_string_t*)tuple->getValue(2)->val.listVal.overflowPtr)[0].getAsString(),
                to_string(1000 - i));
            if (i == 1) {
                validateInsertedRels(result, numInsertedRels, false /* containNullValues */,
                    false /* testLongString */);
            }
        }
    }

    void insertRelsToSmallListTest(
        bool isCommit, TransactionTestType transactionTestType, uint64_t numRelsToInsert = 100) {
        auto numRelsAfterInsertion = 51 + numRelsToInsert;
        insertRels(0 /* srcTableID */, 1 /* dstTableID */, numRelsToInsert);
        conn->beginWriteTransaction();
        auto result =
            conn->query("match (:animal)-[e:knows]->(b:person) return e.length, e.place, e.tag");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), numRelsAfterInsertion);
        validateListAfterInsertion(result.get(), numRelsToInsert);
        result.reset();
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        result =
            conn->query("match (:animal)-[e:knows]->(b:person) return e.length, e.place, e.tag");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), isCommit ? numRelsAfterInsertion : 51);
        if (isCommit) {
            validateListAfterInsertion(result.get(), numRelsToInsert);
        }
    }

    void validateLargeListAfterInsertion(
        QueryResult* result, uint64_t numInsertedRels, bool containNullValues) {
        for (auto i = 0u; i < 2500; i++) {
            auto tuple = result->getNext();
            ASSERT_EQ(tuple->getValue(0)->val.int64Val, 3 * i);
            ASSERT_EQ(tuple->getValue(1)->val.strVal.getAsString(), to_string(3000 - i));
            ASSERT_EQ(((gf_string_t*)tuple->getValue(2)->val.listVal.overflowPtr)[0].getAsString(),
                to_string(3000 - i));
        }
        validateInsertedRels(
            result, numInsertedRels, containNullValues, false /* testLongString */);
    }

    void insertRelsToLargeListTest(bool isCommit, TransactionTestType transactionTestType,
        uint64_t numRelsToInsert = 100, bool insertNullValues = false) {
        auto numRelsAfterInsertion = 2500 + numRelsToInsert;
        insertRels(1 /* srcTableID */, 1 /* dstTableID */, numRelsToInsert, insertNullValues);
        conn->beginWriteTransaction();
        auto result =
            conn->query("match (:person)-[e:knows]->(:person) return e.length, e.place, e.tag");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), numRelsAfterInsertion);
        validateLargeListAfterInsertion(result.get(), numRelsToInsert, insertNullValues);
        result.reset();
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        result =
            conn->query("match (:person)-[e:knows]->(:person) return e.length, e.place, e.tag");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), isCommit ? numRelsAfterInsertion : 2500);
        if (isCommit) {
            validateLargeListAfterInsertion(result.get(), numRelsToInsert, insertNullValues);
        }
    }

    void insertRelsAndQueryBWDListTest(bool isCommit, TransactionTestType transactionTestType) {
        auto numRelsToInsert = 100;
        insertRels(1 /* srcTableID */, 0 /* dstTableID */, numRelsToInsert);
        conn->beginWriteTransaction();
        auto result =
            conn->query("match (:person)-[e:knows]->(:animal) return e.length, e.place, e.tag");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), numRelsToInsert);
        validateInsertedRels(result.get(), numRelsToInsert, false /* containNullValues */,
            false /* testLongString */);
        result.reset();
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        result =
            conn->query("match (:person)-[e:knows]->(:animal) return e.length, e.place, e.tag");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), isCommit ? numRelsToInsert : 0);
        if (isCommit) {
            validateInsertedRels(result.get(), numRelsToInsert, false /* containNullValues */,
                false /* testLongString */);
        }
    }

    void insertRelsToNode(node_offset_t srcNodeOffset) {
        auto placeStr = gf_string_t();
        auto tagStr = gf_list_t();
        tagStr.overflowPtr =
            reinterpret_cast<uint64_t>(tagProperty->getOverflowBuffer().allocateSpace(100));
        tagStr.size = 1;
        ((nodeID_t*)srcNodeVector->values)[0] = nodeID_t(srcNodeOffset, 0);
        placeValues[0] = placeStr;
        tagValues[0] = tagStr;
        // If the srcNodeOffset is an odd number, we insert 100 rels to it (person 700-799
        // (inclusive)). Otherwise, we insert 1000 rels to it (person 500-1499(inclusive)).
        // Note: inserting 1000 rels to a node will convert the original small list to large list.
        if (srcNodeOffset % 2) {
            for (auto i = 0u; i < 100; i++) {
                auto dstNodeOffset = 700 + srcNodeOffset + i;
                ((nodeID_t*)dstNodeVector->values)[0] = nodeID_t(dstNodeOffset, 1);
                lengthValues[0] = dstNodeOffset * 3;
                placeStr.set(to_string(dstNodeOffset - 5));
                tagStr.set((uint8_t*)&placeStr, DataType(LIST, make_unique<DataType>(STRING)));
                insertToRelUpdateStore();
            }
        } else {
            for (auto i = 0u; i < 1000; i++) {
                auto dstNodeOffset = 500 + srcNodeOffset + i;
                ((nodeID_t*)dstNodeVector->values)[0] = nodeID_t(dstNodeOffset, 1);
                lengthValues[0] = dstNodeOffset * 2;
                placeStr.set(to_string(dstNodeOffset - 25));
                tagStr.set((uint8_t*)&placeStr, DataType(LIST, make_unique<DataType>(STRING)));
                insertToRelUpdateStore();
            }
        }
    }

    void validateRelsForNodes(QueryResult* result) {
        shared_ptr<FlatTuple> tuple;
        tuple = result->getNext();
        for (auto i = 0u; i < 50; i++) {
            // Check rels stored in the persistent store.
            ASSERT_EQ(tuple->getValue(0)->val.int64Val, i);
            ASSERT_EQ(tuple->getValue(1)->val.strVal.getAsString(), to_string(1000 - i));
            ASSERT_EQ(((gf_string_t*)tuple->getValue(2)->val.listVal.overflowPtr)[0].getAsString(),
                to_string(1000 - i));
            if (i % 10 == 0 || i >= 30) {
                // For nodes with nodeOffset 0,10,20 or nodeOffset >= 30, we don't insert new rels
                // for them. So we only need to check the rels in the persistent store.
                return;
            }
            // For other nodes, we need to check the newly inserted edges.
            if (i % 2) {
                for (auto j = 0u; j < 100; j++) {
                    tuple = result->getNext();
                    auto dstNodeOffset = 700 + i + j;
                    ASSERT_EQ(tuple->getValue(0)->val.int64Val, dstNodeOffset * 3);
                    ASSERT_EQ(
                        tuple->getValue(1)->val.strVal.getAsString(), to_string(dstNodeOffset - 5));
                    ASSERT_EQ(((gf_string_t*)tuple->getValue(2)->val.listVal.overflowPtr)[0]
                                  .getAsString(),
                        to_string(dstNodeOffset - 5));
                }
            } else {
                for (auto j = 0u; j < 1000; j++) {
                    tuple = result->getNext();
                    auto dstNodeOffset = 500 + i + j;
                    ASSERT_EQ(tuple->getValue(0)->val.int64Val, dstNodeOffset * 2);
                    ASSERT_EQ(tuple->getValue(1)->val.strVal.getAsString(),
                        to_string(dstNodeOffset - 25));
                    ASSERT_EQ(((gf_string_t*)tuple->getValue(2)->val.listVal.overflowPtr)[0]
                                  .getAsString(),
                        to_string(dstNodeOffset - 25));
                }
            }
        }
    }

    void insertDifferentRelsToNodesTest(bool isCommit, TransactionTestType transactionTestType) {
        // We insert 100 dstNodes for nodes with odd nodeOffset value and insert 1000 dstNodes
        // for nodes with even nodeOffset value. We don't insert any dstNodes for node 0, 10, 20.
        // In total: we are going to insert: 15 * 100 + 12 * 1000 = 13500 number of nodes.
        auto numNodesToInsert = 13500;
        auto numNodesInList = 51;
        auto totalNumNodesAfterInsertion = numNodesToInsert + numNodesInList;
        for (auto i = 0u; i < 30; i++) {
            if (i % 10) {
                insertRelsToNode(i);
            }
        }
        conn->beginWriteTransaction();
        auto result =
            conn->query("match (:animal)-[e:knows]->(b:person) return e.length, e.place, e.tag");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), totalNumNodesAfterInsertion);
        validateRelsForNodes(result.get());
        result.reset();
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        result =
            conn->query("match (:animal)-[e:knows]->(b:person) return e.length, e.place, e.tag");
        ASSERT_TRUE(result->isSuccess());
        ASSERT_EQ(result->getNumTuples(), isCommit ? totalNumNodesAfterInsertion : numNodesInList);
        if (isCommit) {
            validateRelsForNodes(result.get());
        }
    }

public:
    unique_ptr<BufferManager> bufferManager;
    unique_ptr<MemoryManager> memoryManager;
    shared_ptr<ValueVector> relIDProperty;
    int64_t* relIDValues;
    shared_ptr<ValueVector> lengthProperty;
    int64_t* lengthValues;
    shared_ptr<ValueVector> placeProperty;
    gf_string_t* placeValues;
    shared_ptr<ValueVector> tagProperty;
    gf_list_t* tagValues;
    shared_ptr<ValueVector> srcNodeVector;
    shared_ptr<ValueVector> dstNodeVector;
    shared_ptr<DataChunk> dataChunk = make_shared<DataChunk>(6 /* numValueVectors */);
};

TEST_F(RelInsertionTest, InsertRelsToEmptyListCommitNormalExecutionTest) {
    insertRelsToEmptyListTest(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsToEmptyListCommitRecoveryTest) {
    insertRelsToEmptyListTest(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertRelsToEmptyListRollbackNormalExecutionTest) {
    insertRelsToEmptyListTest(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsToEmptyListRollbackRecoveryTest) {
    insertRelsToEmptyListTest(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertRelsToEmptyListCommitWithNullNormalExecutioniTest) {
    insertRelsToEmptyListTest(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION,
        100 /* numRelsToInsert */, true /* insertNullValues */);
}

TEST_F(RelInsertionTest, InsertRelsToEmptyListCommitWithNullRecoveryTest) {
    insertRelsToEmptyListTest(true /* isCommit */, TransactionTestType::RECOVERY,
        100 /* numRelsToInsert */, true /* insertNullValues */);
}

// Our page size is 4096 bytes, so we can put 504 number of elements in a single page (4032 bytes
// for data, 64 bytes for nullMask). We are trying to add 510 elements to the relPropertyList for
// nodeOffset 1, this will make the relPropertyList for nodeOffset1 become largetList.
TEST_F(RelInsertionTest, InsertLargeNumRelsToEmptyListCommitNoNullNormalExecutionTest) {
    insertRelsToEmptyListTest(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION,
        510 /* numRelsToInsert */, false /* insertNullValues */);
}

TEST_F(RelInsertionTest, InsertLargeNumRelsToEmptyListCommitNoNullRecoveryTest) {
    insertRelsToEmptyListTest(true /* isCommit */, TransactionTestType::RECOVERY,
        510 /* numRelsToInsert */, false /* insertNullValues */);
}

TEST_F(RelInsertionTest, InsertLargeNumRelsToEmptyListCommitWithNullNormalExecutionTest) {
    insertRelsToEmptyListTest(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION,
        510 /* numRelsToInsert */, true /* insertNullValues */);
}

TEST_F(RelInsertionTest, InsertLargeNumRelsToEmptyListCommitWithNullRecoveryTest) {
    insertRelsToEmptyListTest(true /* isCommit */, TransactionTestType::RECOVERY,
        510 /* numRelsToInsert */, true /* insertNullValues */);
}

TEST_F(RelInsertionTest, InsertLargeNumRelsToEmptyListCommitLongStringNormalExecutionTest) {
    insertRelsToEmptyListTest(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION,
        510 /* numRelsToInsert */, false /* insertNullValues */, true /* testLongString */);
}

TEST_F(RelInsertionTest, InsertLargeNumRelsToEmptyListCommitLongStringRecoveryTest) {
    insertRelsToEmptyListTest(true /* isCommit */, TransactionTestType::RECOVERY,
        510 /* numRelsToInsert */, false /* insertNullValues */, true /* testLongString */);
}

TEST_F(RelInsertionTest, InsertRelsToSmallListCommitNormalExecutionTest) {
    insertRelsToSmallListTest(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsToSmallListCommitRecoveryTest) {
    insertRelsToSmallListTest(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertRelsToSmallListRollbackNormalExecutionTest) {
    insertRelsToSmallListTest(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsToSmallListRollbackRecoveryTest) {
    insertRelsToSmallListTest(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertLargeNumRelsToSmallListCommitNormalExecutionTest) {
    insertRelsToSmallListTest(
        true /* isCommit */, TransactionTestType::NORMAL_EXECUTION, 1000 /* numRelsToInsert */);
}

TEST_F(RelInsertionTest, InsertLargeNumRelsToSmallListCommitRecoveryTest) {
    insertRelsToSmallListTest(
        true /* isCommit */, TransactionTestType::RECOVERY, 1000 /* numRelsToInsert */);
}

TEST_F(RelInsertionTest, InsertRelsToLargeListRollbackNormalExecutionTest) {
    insertRelsToLargeListTest(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsToLargeListRollbackRecoveryTest) {
    insertRelsToLargeListTest(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertRelsToLargeListCommitNormalExecutionNoNullTest) {
    insertRelsToLargeListTest(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsToLargeListCommitRecoveryNoNullTest) {
    insertRelsToLargeListTest(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertRelsToLargeListCommitNormalExecutionWithNullTest) {
    insertRelsToLargeListTest(
        true /* isCommit */, TransactionTestType::NORMAL_EXECUTION, true /* insertNullValues */);
}

TEST_F(RelInsertionTest, InsertRelsToLargeListCommitRecoveryWithNullTest) {
    insertRelsToLargeListTest(
        true /* isCommit */, TransactionTestType::RECOVERY, true /* insertNullValues */);
}

TEST_F(RelInsertionTest, InsertRelsAndQueryBWDListCommitNormalExecutionTest) {
    insertRelsAndQueryBWDListTest(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsAndQueryBWDListRollbackNormalExecutionTest) {
    insertRelsAndQueryBWDListTest(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsAndQueryBWDListCommitRecoveryTest) {
    insertRelsAndQueryBWDListTest(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertRelsAndQueryBWDListRollbackRecoveryTest) {
    insertRelsAndQueryBWDListTest(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertRelsToDifferentNodesCommitNormalExecutionTest) {
    insertDifferentRelsToNodesTest(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsToDifferentNodesRollbackNormalExecutionTest) {
    insertDifferentRelsToNodesTest(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(RelInsertionTest, InsertRelsToDifferentNodesCommitRecoveryTest) {
    insertDifferentRelsToNodesTest(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InsertRelsToDifferentNodesRollbackRecoveryTest) {
    insertDifferentRelsToNodesTest(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(RelInsertionTest, InCorrectVectorErrorTest) {
    incorrectVectorErrorTest(
        vector<shared_ptr<ValueVector>>{lengthProperty, placeProperty, relIDProperty, srcNodeVector,
            relIDProperty, tagProperty, dstNodeVector},
        "Expected number of valueVectors: 6. Given: 7.");
    incorrectVectorErrorTest(vector<shared_ptr<ValueVector>>{srcNodeVector, dstNodeVector,
                                 placeProperty, lengthProperty, tagProperty, relIDProperty},
        "Expected vector with type INT64, Given: STRING.");
    incorrectVectorErrorTest(vector<shared_ptr<ValueVector>>{srcNodeVector, relIDProperty,
                                 lengthProperty, placeProperty, tagProperty, relIDProperty},
        "The first two vectors of srcDstNodeIDAndRelProperties should be src/dstNodeVector.");
    ((nodeID_t*)srcNodeVector->values)[0].tableID = 5;
    incorrectVectorErrorTest(vector<shared_ptr<ValueVector>>{srcNodeVector, dstNodeVector,
                                 lengthProperty, placeProperty, tagProperty, relIDProperty},
        "ListUpdateStore for tableID: 5 doesn't exist.");
}

#include "test/test_utility/include/test_helper.h"

#include "src/common/include/overflow_buffer_utils.h"

using namespace graphflow::storage;
using namespace graphflow::testing;

// Note: ID and nodeOffsetForPropKeys in this test are equal for each node, so we use nodeID and
// nodeOffsetForPropKeys interchangeably.
class UnstructuredPropertyListsUpdateTests : public BaseGraphTest {

public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        initWithoutLoadingGraph(TransactionTestType::NORMAL_EXECUTION);
    }

    void initWithoutLoadingGraph(TransactionTestType transactionTestType) {
        if (overflowBuffer) {
            overflowBuffer.reset();
        }
        createDBAndConn(transactionTestType);
        overflowBuffer = make_unique<OverflowBuffer>(database->getMemoryManager());
        readConn = make_unique<Connection>(database.get());
        personNodeLabel =
            database->getCatalog()->getReadOnlyVersion()->getNodeLabelFromName("person");
        personNodeTable = database->getStorageManager()->getNodesStore().getNode(personNodeLabel);

        existingStrVal.dataType = DataType(DataTypeID::STRING);
        string existingStr = "abcdefghijklmn";
        OverflowBufferUtils::copyString(
            existingStr.c_str(), existingStr.size(), existingStrVal.val.strVal, *overflowBuffer);
        existingIntVal.dataType = DataType(DataTypeID::INT64);
        existingIntVal.val.int64Val = 123456;

        shortStrVal.dataType = DataType(DataTypeID::STRING);
        string shortStr = "short";
        OverflowBufferUtils::copyString(
            shortStr.c_str(), shortStr.size(), shortStrVal.val.strVal, *overflowBuffer);
        longStrVal.dataType = DataType(DataTypeID::STRING);
        string longStr = "new-long-string";
        OverflowBufferUtils::copyString(
            longStr.c_str(), longStr.size(), longStrVal.val.strVal, *overflowBuffer);
        intVal.dataType = DataType(DataTypeID::INT64);
        intVal.val.int64Val = 677121;
        conn->beginWriteTransaction();
    }

    void initGraph() override {
        conn->query(createNodeCmdPrefix + "person (ID INT64, PRIMARY KEY (ID))");
        conn->query(
            "COPY person FROM \"dataset/unstructured-property-lists-updates-tests/vPerson.csv\"");
    }

    void commitOrRollbackConnectionAndInitDBIfNecessary(
        bool isCommit, TransactionTestType transactionTestType) {
        commitOrRollbackConnection(isCommit, transactionTestType);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            // This creates a new database/conn/readConn and should run the recovery algorithm
            initWithoutLoadingGraph(transactionTestType);
        }
    }

    // TODO(Xiyang): Currently we are manually calling set in getUnstrPropertyLists. Once we have
    // frontend support, these sets should also be done through Cypher queries.
    void setPropertyOfNode(node_offset_t nodeOffset, string propKey, Value& newVal) {
        uint32_t unstrPropKey = database->getCatalog()
                                    ->getReadOnlyVersion()
                                    ->getNodeProperty(personNodeLabel, propKey)
                                    .propertyID;
        personNodeTable->getUnstrPropertyLists()->setProperty(nodeOffset, unstrPropKey, &newVal);
    }

    void removeProperty(node_offset_t nodeOffset, string propKey) {
        uint32_t unstrPropKey = database->getCatalog()
                                    ->getReadOnlyVersion()
                                    ->getNodeProperty(personNodeLabel, propKey)
                                    .propertyID;
        personNodeTable->getUnstrPropertyLists()->removeProperty(nodeOffset, unstrPropKey);
    }

    void queryAndVerifyResults(node_offset_t nodeOffset, string intPropKey, string strPropKey,
        Value* expectedIntForWriteTrx, Value* expectedStrValueForWriteTrx,
        Value* expectedIntForReadTrx, Value* expectedStrValueForReadTrx) {
        string query = "MATCH (a:person) WHERE a.ID = " + to_string(nodeOffset) + " RETURN a." +
                       intPropKey + ", a." + strPropKey;
        auto writeQResult = conn->query(query);
        auto writeConTuple = writeQResult->getNext();
        if (expectedIntForWriteTrx == nullptr) {
            ASSERT_TRUE(writeConTuple->isNull(0));
        } else {
            ASSERT_FALSE(writeConTuple->isNull(0));
            ASSERT_EQ(
                writeConTuple->getValue(0)->val.int64Val, expectedIntForWriteTrx->val.int64Val);
        }
        if (expectedStrValueForWriteTrx == nullptr) {
            ASSERT_TRUE(writeConTuple->isNull(1));
        } else {
            ASSERT_FALSE(writeConTuple->isNull(1));
            ASSERT_EQ(
                writeConTuple->getValue(1)->val.strVal, expectedStrValueForWriteTrx->val.strVal);
        }

        auto readQResult = readConn->query(query);
        auto readConTuple = readQResult->getNext();
        if (expectedIntForReadTrx == nullptr) {
            ASSERT_TRUE(readConTuple->isNull(0));
        } else {
            ASSERT_FALSE(readConTuple->isNull(0));
            ASSERT_EQ(readConTuple->getValue(0)->val.int64Val, expectedIntForReadTrx->val.int64Val);
        }
        if (expectedStrValueForReadTrx == nullptr) {
            ASSERT_TRUE(readConTuple->isNull(1));
        } else {
            ASSERT_FALSE(readConTuple->isNull(1));
            ASSERT_EQ(
                readConTuple->getValue(1)->val.strVal, expectedStrValueForReadTrx->val.strVal);
        }
    }

    void updateExistingFixedLenPropertiesUseShortStringTest(
        bool isCommit, TransactionTestType transactionTestType) {
        setPropertyOfNode(123, "ui123", intVal);
        setPropertyOfNode(123, "us123", shortStrVal);
        queryAndVerifyResults(123, "ui123", "us123", &intVal /* expected int for write trx */,
            &shortStrVal /* expected str for write trx */,
            &existingIntVal /* expected int for read trx */,
            &existingStrVal /* expected str for read trx */);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        if (isCommit) {
            queryAndVerifyResults(123, "ui123", "us123", &intVal /* expected int for write trx */,
                &shortStrVal /* expected str for write trx */,
                &intVal /* expected int for read trx */,
                &shortStrVal /* expected str for read trx */);
        } else {
            queryAndVerifyResults(123, "ui123", "us123",
                &existingIntVal /* expected int for write trx */,
                &existingStrVal /* expected str for write trx */,
                &existingIntVal /* expected int for read trx */,
                &existingStrVal /* expected str for read trx */);
        }
    }

    void updateLongStringPropertyTest(bool isCommit, TransactionTestType transactionTestType) {
        setPropertyOfNode(123, "us123", longStrVal);
        queryAndVerifyResults(123, "ui123", "us123",
            &existingIntVal /* expected int for write trx */,
            &longStrVal /* expected str for write trx */,
            &existingIntVal /* expected int for read trx */,
            &existingStrVal /* expected str for write trx */);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        if (isCommit) {
            queryAndVerifyResults(123, "ui123", "us123",
                &existingIntVal /* expected int for write trx */,
                &longStrVal /* expected str for write trx */,
                &existingIntVal /* expected int for read trx */,
                &longStrVal /* expected str for read trx */);
        } else {
            queryAndVerifyResults(123, "ui123", "us123",
                &existingIntVal /* expected int for write trx */,
                &existingStrVal /* expected str for write trx */,
                &existingIntVal /* expected int for read trx */,
                &existingStrVal /* expected str for read trx */);
        }
    }

    void insertNonExistingPropsTest(bool isCommit, TransactionTestType transactionTestType) {
        setPropertyOfNode(123, "us125", longStrVal);
        setPropertyOfNode(123, "ui124", intVal);
        queryAndVerifyResults(123, "ui124", "us125", &intVal /* expected int for write trx */,
            &longStrVal /* expected str for write trx */, nullptr /*expected int for read trx */,
            nullptr /* expected str for read trx */);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        if (isCommit) {
            queryAndVerifyResults(123, "ui124", "us125", &intVal /* expected int for write trx */,
                &longStrVal /* expected str for write trx */,
                &intVal /*expected int for read trx */,
                &longStrVal /* expected str for read trx */);
        } else {
            queryAndVerifyResults(123, "ui124", "us125", nullptr /* expected int for write trx */,
                nullptr /* expected str for write trx */, nullptr /*expected int for read trx */,
                nullptr /* expected str for read trx */);
        }
    }

    void removeExistingPropsTest(bool isCommit, TransactionTestType transactionTestType) {
        removeProperty(123, "us123");
        removeProperty(123, "ui123");
        queryAndVerifyResults(123, "ui123", "us123", nullptr /* expected int for write trx */,
            nullptr /* expected str for write trx */,
            &existingIntVal /*expected int for read trx */,
            &existingStrVal /* expected str for read trx */);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        if (isCommit) {
            queryAndVerifyResults(123, "ui123", "us123", nullptr /* expected int for write trx */,
                nullptr /* expected str for write trx */, nullptr /*expected int for read trx */,
                nullptr /* expected str for read trx */);
        } else {
            queryAndVerifyResults(123, "ui123", "us123",
                &existingIntVal /* expected int for write trx */,
                &existingStrVal /* expected str for write trx */,
                &existingIntVal /*expected int for read trx */,
                &existingStrVal /* expected str for read trx */);
        }
    }

    void removeNonExistingPropsTest(bool isCommit, TransactionTestType transactionTestType) {
        removeProperty(123, "us125");
        removeProperty(123, "ui124");
        queryAndVerifyResults(123, "ui123", "us123",
            &existingIntVal /* expected int for write trx */,
            &existingStrVal /* expected str for write trx */,
            &existingIntVal /* expected int for read trx */,
            &existingStrVal /* expected str for write trx */);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        // Regardless of whether we are committing or rollbacking, because there is no change
        // the results should be the same.
        queryAndVerifyResults(123, "ui123", "us123",
            &existingIntVal /* expected int for write trx */,
            &existingStrVal /* expected str for write trx */,
            &existingIntVal /* expected int for read trx */,
            &existingStrVal /* expected str for write trx */);
    }

    void verifyWriteOrCommittedTrxForRemoveNewlyAddedPropertiesTest(
        unique_ptr<QueryResult> qResult) {
        auto tuple = qResult->getNext();
        ASSERT_TRUE(tuple->isNull(0));
        ASSERT_EQ(tuple->getValue(1)->val.strVal, existingStrVal.val.strVal);
        ASSERT_TRUE(tuple->isNull(2));
    }

    void verifyReadOrRolledbackTrxForRemoveNewlyAddedPropertiesTest(
        unique_ptr<QueryResult> qResult) {
        auto tuple = qResult->getNext();
        ASSERT_EQ(tuple->getValue(0)->val.int64Val, existingIntVal.val.int64Val);
        ASSERT_EQ(tuple->getValue(1)->val.strVal, existingStrVal.val.strVal);
        ASSERT_TRUE(tuple->isNull(2));
    }

    void removeNewlyAddedPropertiesTest(bool isCommit, TransactionTestType transactionTestType) {
        setPropertyOfNode(123, "us125", longStrVal);
        removeProperty(123, "us125");
        setPropertyOfNode(123, "ui123", intVal);
        removeProperty(123, "ui123");

        string query = "MATCH (a:person) WHERE a.ID = 123 RETURN a.ui123, a.us123, a.us125";
        auto writeQResult = conn->query(query);
        verifyWriteOrCommittedTrxForRemoveNewlyAddedPropertiesTest(move(writeQResult));
        auto readQResult = readConn->query(query);
        verifyReadOrRolledbackTrxForRemoveNewlyAddedPropertiesTest(move(readQResult));

        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        if (isCommit) {
            writeQResult = conn->query(query);
            verifyWriteOrCommittedTrxForRemoveNewlyAddedPropertiesTest(move(writeQResult));
            readQResult = readConn->query(query);
            verifyWriteOrCommittedTrxForRemoveNewlyAddedPropertiesTest(move(readQResult));
        } else {
            writeQResult = conn->query(query);
            verifyReadOrRolledbackTrxForRemoveNewlyAddedPropertiesTest(move(writeQResult));
            readQResult = readConn->query(query);
            verifyReadOrRolledbackTrxForRemoveNewlyAddedPropertiesTest(move(readQResult));
        }
    }

    void removeAllPropertiesBySetPropertyListEmptyTest(
        bool isCommit, TransactionTestType transactionTestType) {
        personNodeTable->getUnstrPropertyLists()->setPropertyListEmpty(123);
        queryAndVerifyResults(123, "ui123", "us123", nullptr /* expected int for write trx */,
            nullptr /* expected str for write trx */,
            &existingIntVal /* expected int for read trx */,
            &existingStrVal /* expected str for write trx */);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        if (isCommit) {
            queryAndVerifyResults(123, "ui123", "us123", nullptr /* expected int for write trx */,
                nullptr /* expected str for write trx */, nullptr /*expected int for read trx */,
                nullptr /* expected str for read trx */);
        } else {
            queryAndVerifyResults(123, "ui123", "us123",
                &existingIntVal /* expected int for write trx */,
                &existingStrVal /* expected str for write trx */,
                &existingIntVal /*expected int for read trx */,
                &existingStrVal /* expected str for read trx */);
        }
    }

    void verifyEmptyDB(Connection* connection) {
        for (uint64_t nodeOffsetForPropKeys = 0; nodeOffsetForPropKeys <= 600;
             nodeOffsetForPropKeys++) {
            string query = "MATCH (a:person) WHERE a.ui" + to_string(nodeOffsetForPropKeys) +
                           " IS NOT NULL OR a.us" + to_string(nodeOffsetForPropKeys) +
                           " IS NOT NULL RETURN count (*)";
            auto qResult = connection->query(query);
            auto tuple = qResult->getNext();
            ASSERT_EQ(tuple->getValue(0)->val.int64Val, 0);
        }
    }

    void verifyInitialDBState(Connection* connection) {
        for (uint64_t nodeOffsetForPropKeys = 0; nodeOffsetForPropKeys <= 600;
             nodeOffsetForPropKeys++) {
            string query = "MATCH (a:person) WHERE a.ui" + to_string(nodeOffsetForPropKeys) +
                           " IS NOT NULL OR a.us" + to_string(nodeOffsetForPropKeys) +
                           " IS NOT NULL RETURN count (*)";
            auto qResult = connection->query(query);
            auto tuple = qResult->getNext();
            if (nodeOffsetForPropKeys == 250) {
                // If we are looking for 250's properties, e.g., ui250 and us250, then we expect the
                // count to be only 1 because only node 250 contains these
                ASSERT_EQ(tuple->getValue(0)->val.int64Val, 1);
            } else {
                // If we are not looking for 250's properties, e.g., ui0 and us0, then we expect the
                // count to be 2 because there are 2 nodes with those properties: 1 is the node with
                // nodeID=nodeOffsetForPropKeys; and 2 is node 250 which has all unstructured
                // properties.
                ASSERT_EQ(tuple->getValue(0)->val.int64Val, 2);
            }
        }
    }

    void removeAllPropertiesOfAllNodesTest(bool isCommit, TransactionTestType transactionTestType) {
        // We blindly remove all unstructured properties from all nodes one by one
        for (uint64_t queryNodeOffset = 0; queryNodeOffset <= 600; queryNodeOffset++) {
            for (uint64_t nodeOffsetForPropKeys = 0; nodeOffsetForPropKeys <= 600;
                 ++nodeOffsetForPropKeys) {
                removeProperty(queryNodeOffset, "ui" + to_string(nodeOffsetForPropKeys));
                removeProperty(queryNodeOffset, "us" + to_string(nodeOffsetForPropKeys));
            }
        }

        verifyEmptyDB(conn.get());
        verifyInitialDBState(readConn.get());

        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);

        if (isCommit) {
            verifyEmptyDB(conn.get());
            verifyEmptyDB(readConn.get());
        } else {
            verifyInitialDBState(conn.get());
            verifyInitialDBState(readConn.get());
        }
    }

    void verifyWriteOrCommittedReadTrxInsertALargeNumberOfPropertiesTest(Connection* connection) {
        for (uint64_t nodeOffsetForPropKeys = 0; nodeOffsetForPropKeys <= 600;
             ++nodeOffsetForPropKeys) {
            string query = "MATCH (a:person) WHERE a.ID = 123 RETURN a.ui" +
                           to_string(nodeOffsetForPropKeys) + ", a.us" +
                           to_string(nodeOffsetForPropKeys);
            auto qResult = connection->query(query);
            auto tuple = qResult->getNext();
            ASSERT_FALSE(tuple->isNull(0));
            ASSERT_EQ(tuple->getValue(0)->val.int64Val, intVal.val.int64Val);
            ASSERT_FALSE(tuple->isNull(1));
            ASSERT_EQ(tuple->getValue(1)->val.strVal, longStrVal.val.strVal);
        }
    }

    void verifyReadOrRolledbackTrxInsertALargeNumberOfPropertiesTest(Connection* connection) {
        for (uint64_t nodeOffsetForPropKeys = 0; nodeOffsetForPropKeys <= 600;
             ++nodeOffsetForPropKeys) {
            string query = "MATCH (a:person) WHERE a.ID = 123 RETURN a.ui" +
                           to_string(nodeOffsetForPropKeys) + ", a.us" +
                           to_string(nodeOffsetForPropKeys);
            auto qResult = connection->query(query);
            auto tuple = qResult->getNext();
            if (nodeOffsetForPropKeys == 123) {
                ASSERT_FALSE(tuple->isNull(0));
                ASSERT_EQ(tuple->getValue(0)->val.int64Val, existingIntVal.val.int64Val);
                ASSERT_FALSE(tuple->isNull(1));
                ASSERT_EQ(tuple->getValue(1)->val.strVal, existingStrVal.val.strVal);
            } else {
                ASSERT_TRUE(tuple->isNull(0));
                ASSERT_TRUE(tuple->isNull(1));
            }
        }
    }

    void insertALargeNumberOfPropertiesTest(
        bool isCommit, TransactionTestType transactionTestType) {
        for (uint64_t nodeOffsetForPropKeys = 0; nodeOffsetForPropKeys <= 600;
             ++nodeOffsetForPropKeys) {
            setPropertyOfNode(123, "ui" + to_string(nodeOffsetForPropKeys), intVal);
            setPropertyOfNode(123, "us" + to_string(nodeOffsetForPropKeys), longStrVal);
        }

        verifyWriteOrCommittedReadTrxInsertALargeNumberOfPropertiesTest(conn.get());
        verifyReadOrRolledbackTrxInsertALargeNumberOfPropertiesTest(readConn.get());

        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);

        if (isCommit) {
            verifyWriteOrCommittedReadTrxInsertALargeNumberOfPropertiesTest(conn.get());
            verifyWriteOrCommittedReadTrxInsertALargeNumberOfPropertiesTest(readConn.get());
        } else {
            verifyReadOrRolledbackTrxInsertALargeNumberOfPropertiesTest(conn.get());
            verifyReadOrRolledbackTrxInsertALargeNumberOfPropertiesTest(readConn.get());
        }
    }

    // This function assumes that the current state of the db is empty.
    void writeInitialDBStateTest(
        bool isInitialStateEmpty, bool isCommit, TransactionTestType transactionTestType) {
        for (uint64_t queryNodeOffset = 0; queryNodeOffset <= 600; queryNodeOffset++) {
            if (queryNodeOffset == 250) {
                for (uint64_t nodeOffsetForPropKeys = 0; nodeOffsetForPropKeys <= 600;
                     ++nodeOffsetForPropKeys) {
                    setPropertyOfNode(
                        queryNodeOffset, "ui" + to_string(nodeOffsetForPropKeys), existingIntVal);
                    setPropertyOfNode(
                        queryNodeOffset, "us" + to_string(nodeOffsetForPropKeys), existingStrVal);
                }
            } else {
                setPropertyOfNode(
                    queryNodeOffset, "ui" + to_string(queryNodeOffset), existingIntVal);
                setPropertyOfNode(
                    queryNodeOffset, "us" + to_string(queryNodeOffset), existingStrVal);
            }
        }

        verifyInitialDBState(conn.get());
        if (isInitialStateEmpty) {
            verifyEmptyDB(readConn.get());
        } else {
            verifyInitialDBState(readConn.get());
        }
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);

        if (isCommit) {
            verifyInitialDBState(conn.get());
            verifyInitialDBState(readConn.get());
        } else {
            if (isInitialStateEmpty) {
                verifyEmptyDB(conn.get());
                verifyEmptyDB(readConn.get());
            } else {
                verifyInitialDBState(conn.get());
                verifyInitialDBState(readConn.get());
            }
        }
    }

    void removeAndWriteDataTwiceTest(bool isCommit, TransactionTestType transactionTestType) {
        removeAllPropertiesOfAllNodesTest(isCommit, transactionTestType);
        writeInitialDBStateTest(isCommit ? true /* initial state is empty db */ :
                                           false /* initial state is inital db state */,
            isCommit, transactionTestType);
        removeAllPropertiesOfAllNodesTest(isCommit, transactionTestType);
        writeInitialDBStateTest(isCommit ? true /* initial state is empty db */ :
                                           false /* initial state is inital db state */,
            isCommit, transactionTestType);
    }

    void addNewListsTest(bool isCommit, TransactionTestType transactionTestType) {
        uint32_t unstrIntPropKey = database->getCatalog()
                                       ->getReadOnlyVersion()
                                       ->getNodeProperty(personNodeLabel, "ui0")
                                       .propertyID;
        uint32_t unstrStrPropKey = database->getCatalog()
                                       ->getReadOnlyVersion()
                                       ->getNodeProperty(personNodeLabel, "us0")
                                       .propertyID;
        // Inserting until 1100 ensures that chunk 1 is filled and a new chunk 2 is started
        for (node_offset_t nodeOffset = 601; nodeOffset < 1100; ++nodeOffset) {
            personNodeTable->getUnstrPropertyLists()->setPropertyListEmpty(nodeOffset);
            personNodeTable->getUnstrPropertyLists()->setProperty(
                nodeOffset, unstrIntPropKey, &existingIntVal);
            personNodeTable->getUnstrPropertyLists()->setProperty(
                nodeOffset, unstrStrPropKey, &existingStrVal);
        }

        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        if (isCommit) {
            // We test that the newly lists are inserted through callin ghte
            // readUnstructuredPropertiesOfNode(), which was a function designed for tests because
            // currently the frontend to add new nodes and unstructured properties is not supported.
            for (node_offset_t nodeOffset = 601; nodeOffset < 1100; ++nodeOffset) {
                auto unstrProperties =
                    personNodeTable->getUnstrPropertyLists()->readUnstructuredPropertiesOfNode(
                        nodeOffset);
                ASSERT_EQ(2, unstrProperties->size());
                auto actualIntVal = unstrProperties->find(unstrIntPropKey)->second;
                ASSERT_FALSE(actualIntVal.isNull());
                ASSERT_EQ(existingIntVal.val.int64Val, actualIntVal.val.int64Val);
                auto actualStrVal = unstrProperties->find(unstrStrPropKey)->second;
                ASSERT_FALSE(actualStrVal.isNull());
                ASSERT_EQ(existingStrVal.val.strVal.getAsString(), actualStrVal.strVal);
            }
        } else {
            ASSERT_EQ(601, personNodeTable->getUnstrPropertyLists()
                               ->getHeaders()
                               ->headersDiskArray->getNumElements(TransactionType::WRITE));
            ASSERT_EQ(601, personNodeTable->getUnstrPropertyLists()
                               ->getHeaders()
                               ->headersDiskArray->getNumElements(TransactionType::READ_ONLY));
        }
    }

public:
    unique_ptr<Connection> readConn;
    NodeTable* personNodeTable;
    // Overflow is needed to update unstructured properties with long strings
    unique_ptr<OverflowBuffer> overflowBuffer;
    label_t personNodeLabel;
    Value existingStrVal, existingIntVal, shortStrVal, longStrVal, intVal;
};

TEST_F(UnstructuredPropertyListsUpdateTests,
    ExistingFixedLenPropertiesShortStringCommitNormalExecution) {
    updateExistingFixedLenPropertiesUseShortStringTest(
        true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UnstructuredPropertyListsUpdateTests,
    ExistingFixedLenPropertiesShortStringRollbackNormalExecution) {
    updateExistingFixedLenPropertiesUseShortStringTest(
        false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UnstructuredPropertyListsUpdateTests, ExistingFixedLenPropertiesShortStringCommitRecovery) {
    updateExistingFixedLenPropertiesUseShortStringTest(
        true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(
    UnstructuredPropertyListsUpdateTests, ExistingFixedLenPropertiesShortStringRollbackRecovery) {
    updateExistingFixedLenPropertiesUseShortStringTest(
        false /* is rollback */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests, LongStringPropTestCommitNormalExecution) {
    updateLongStringPropertyTest(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UnstructuredPropertyListsUpdateTests, LongStringPropTestRollbackNormalExecution) {
    updateLongStringPropertyTest(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UnstructuredPropertyListsUpdateTests, LongStringPropTestCommitRecovery) {
    updateLongStringPropertyTest(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests, LongStringPropTestRollbackRecovery) {
    updateLongStringPropertyTest(false /* rollback */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests, InsertNonExistingPropsCommitNormalExecution) {
    insertNonExistingPropsTest(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests, InsertNonExistingPropsRollbackNormalExecution) {
    insertNonExistingPropsTest(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UnstructuredPropertyListsUpdateTests, InsertNonExistingPropsCommitRecovery) {
    insertNonExistingPropsTest(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests, InsertNonExistingPropsRollbackRecovery) {
    insertNonExistingPropsTest(false /* rollback */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveExistingPropertiesCommitNormalExecution) {
    removeExistingPropsTest(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveExistingPropertiesRollbackNormalExecution) {
    removeExistingPropsTest(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveExistingPropertiesCommitRecovery) {
    removeExistingPropsTest(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveExistingPropertiesRollbackRecovery) {
    removeExistingPropsTest(false /* rollback */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveNonExistingPropertiesCommitNormalExecution) {
    removeNonExistingPropsTest(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveNonExistingPropertiesRollbackNormalExecution) {
    removeNonExistingPropsTest(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveNonExistingPropertiesCommitRecovery) {
    removeNonExistingPropsTest(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveNonExistingPropertiesRollbackRecovery) {
    removeNonExistingPropsTest(false /* rollback */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveNewlyAddedPropertiesCommitNormalExecution) {
    removeNewlyAddedPropertiesTest(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveNewlyAddedPropertiesRollbackNormalExecution) {
    removeNewlyAddedPropertiesTest(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveNewlyAddedPropertiesCommitRecovery) {
    removeNewlyAddedPropertiesTest(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveNewlyAddedPropertiesRollbackRecovery) {
    removeNewlyAddedPropertiesTest(false /* rollback */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests,
    RemoveAllPropertiesBySetPropertyListEmptyCommitNormalExecution) {
    removeAllPropertiesBySetPropertyListEmptyTest(
        true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UnstructuredPropertyListsUpdateTests,
    RemoveAllPropertiesBySetPropertyListEmptyRollbackNormalExecution) {
    removeAllPropertiesBySetPropertyListEmptyTest(
        false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(
    UnstructuredPropertyListsUpdateTests, RemoveAllPropertiesBySetPropertyListEmptyCommitRecovery) {
    removeAllPropertiesBySetPropertyListEmptyTest(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests,
    RemoveAllPropertiesBySetPropertyListEmptyRollbackRecovery) {
    removeAllPropertiesBySetPropertyListEmptyTest(
        false /* rollback */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests, InsertALargeNumberOfPropertiesCommitNormalExecution) {
    insertALargeNumberOfPropertiesTest(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(
    UnstructuredPropertyListsUpdateTests, InsertALargeNumberOfPropertiesRollbackNormalExecution) {
    insertALargeNumberOfPropertiesTest(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UnstructuredPropertyListsUpdateTests, InsertALargeNumberOfPropertiesCommitRecovery) {
    insertALargeNumberOfPropertiesTest(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests, InsertALargeNumberOfPropertiesRollbackRecovery) {
    insertALargeNumberOfPropertiesTest(false /* rollback */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveAllPropertiesOfAllNodesCommitNormalExecution) {
    removeAllPropertiesOfAllNodesTest(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveAllPropertiesOfAllNodesRollbackNormalExecution) {
    removeAllPropertiesOfAllNodesTest(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveAllPropertiesOfAllNodesCommitRecovery) {
    removeAllPropertiesOfAllNodesTest(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveAllPropertiesOfAllNodesRollbackRecovery) {
    removeAllPropertiesOfAllNodesTest(false /* rollback */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveAndWriteDataTwiceTestCommitNormalExecution) {
    removeAndWriteDataTwiceTest(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveAndWriteDataTwiceTestRollbackNormalExecution) {
    removeAndWriteDataTwiceTest(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveAndWriteDataTwiceTestCommitRecovery) {
    removeAndWriteDataTwiceTest(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests, RemoveAndWriteDataTwiceTestRollbackRecovery) {
    removeAndWriteDataTwiceTest(false /* rollback */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests, AddNewListTestCommitNormalExecution) {
    addNewListsTest(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UnstructuredPropertyListsUpdateTests, AddNewListTestRollbackNormalExecution) {
    addNewListsTest(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UnstructuredPropertyListsUpdateTests, AddNewListTestCommitRecovery) {
    addNewListsTest(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(UnstructuredPropertyListsUpdateTests, AddNewListTestRollbackRecovery) {
    addNewListsTest(false /* rollback */, TransactionTestType::RECOVERY);
}

#include "test/test_utility/include/test_helper.h"

#include "src/common/include/in_mem_overflow_buffer_utils.h"

using namespace kuzu::storage;
using namespace kuzu::testing;

// Note: ID and nodeOffsetForPropKeys in this test are equal for each node, so we use nodeID and
// nodeOffsetForPropKeys interchangeably.
class UnstructuredPropertyListsUpdateTests : public DBTest {

public:
    void SetUp() override {
        DBTest::SetUp();
        initWithoutLoadingGraph();
    }

    string getInputCSVDir() override {
        return "dataset/unstructured-property-lists-updates-tests/";
    }

    void initWithoutLoadingGraph() {
        if (inMemOverflowBuffer) {
            inMemOverflowBuffer.reset();
        }
        createDBAndConn();
        inMemOverflowBuffer = make_unique<InMemOverflowBuffer>(database->getMemoryManager());
        readConn = make_unique<Connection>(database.get());
        personTableID =
            database->getCatalog()->getReadOnlyVersion()->getNodeTableIDFromName("person");
        personNodeTable =
            database->getStorageManager()->getNodesStore().getNodeTable(personTableID);

        existingStrVal.dataType = DataType(DataTypeID::STRING);
        string existingStr = "abcdefghijklmn";
        InMemOverflowBufferUtils::copyString(existingStr.c_str(), existingStr.size(),
            existingStrVal.val.strVal, *inMemOverflowBuffer);
        existingIntVal.dataType = DataType(DataTypeID::INT64);
        existingIntVal.val.int64Val = 123456;

        shortStrVal.dataType = DataType(DataTypeID::STRING);
        string shortStr = "short";
        InMemOverflowBufferUtils::copyString(
            shortStr.c_str(), shortStr.size(), shortStrVal.val.strVal, *inMemOverflowBuffer);
        longStrVal.dataType = DataType(DataTypeID::STRING);
        string longStr = "new-long-string";
        InMemOverflowBufferUtils::copyString(
            longStr.c_str(), longStr.size(), longStrVal.val.strVal, *inMemOverflowBuffer);
        intVal.dataType = DataType(DataTypeID::INT64);
        intVal.val.int64Val = 677121;
        conn->beginWriteTransaction();
    }

    void commitOrRollbackConnectionAndInitDBIfNecessary(
        bool isCommit, TransactionTestType transactionTestType) {
        commitOrRollbackConnection(isCommit, transactionTestType);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            // This creates a new database/conn/readConn and should run the recovery algorithm
            initWithoutLoadingGraph();
        }
    }

    // TODO(Xiyang): Currently we are manually calling set in getUnstrPropertyLists. Once we have
    // frontend support, these sets should also be done through Cypher queries.
    void setPropertyOfNode(node_offset_t nodeOffset, string propKey, Value& newVal) {
        uint32_t unstrPropKey = database->getCatalog()
                                    ->getReadOnlyVersion()
                                    ->getNodeProperty(personTableID, propKey)
                                    .propertyID;
        personNodeTable->getUnstrPropertyLists()->setProperty(nodeOffset, unstrPropKey, &newVal);
    }

    void removeProperty(node_offset_t nodeOffset, string propKey) {
        uint32_t unstrPropKey = database->getCatalog()
                                    ->getReadOnlyVersion()
                                    ->getNodeProperty(personTableID, propKey)
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
            ASSERT_TRUE(writeConTuple->getResultValue(0)->isNullVal());
        } else {
            ASSERT_FALSE(writeConTuple->getResultValue(0)->isNullVal());
            ASSERT_EQ(writeConTuple->getResultValue(0)->getInt64Val(),
                expectedIntForWriteTrx->val.int64Val);
        }
        if (expectedStrValueForWriteTrx == nullptr) {
            ASSERT_TRUE(writeConTuple->getResultValue(1)->isNullVal());
        } else {
            ASSERT_FALSE(writeConTuple->getResultValue(1)->isNullVal());
            ASSERT_EQ(writeConTuple->getResultValue(1)->getStringVal(),
                expectedStrValueForWriteTrx->val.strVal.getAsString());
        }

        auto readQResult = readConn->query(query);
        auto readConTuple = readQResult->getNext();
        if (expectedIntForReadTrx == nullptr) {
            ASSERT_TRUE(readConTuple->getResultValue(0)->isNullVal());
        } else {
            ASSERT_FALSE(readConTuple->getResultValue(0)->isNullVal());
            ASSERT_EQ(readConTuple->getResultValue(0)->getInt64Val(),
                expectedIntForReadTrx->val.int64Val);
        }
        if (expectedStrValueForReadTrx == nullptr) {
            ASSERT_TRUE(readConTuple->getResultValue(1)->isNullVal());
        } else {
            ASSERT_FALSE(readConTuple->getResultValue(1)->isNullVal());
            ASSERT_EQ(readConTuple->getResultValue(1)->getStringVal(),
                expectedStrValueForReadTrx->val.strVal.getAsString());
        }
    }

    void removeAllPropertiesBySetPropertyListEmptyTest(
        bool isCommit, TransactionTestType transactionTestType) {
        personNodeTable->getUnstrPropertyLists()->initEmptyPropertyLists(123);
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
            ASSERT_EQ(tuple->getResultValue(0)->getInt64Val(), 0);
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
                ASSERT_EQ(tuple->getResultValue(0)->getInt64Val(), 1);
            } else {
                // If we are not looking for 250's properties, e.g., ui0 and us0, then we expect the
                // count to be 2 because there are 2 nodes with those properties: 1 is the node with
                // nodeID=nodeOffsetForPropKeys; and 2 is node 250 which has all unstructured
                // properties.
                ASSERT_EQ(tuple->getResultValue(0)->getInt64Val(), 2);
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
                                       ->getNodeProperty(personTableID, "ui0")
                                       .propertyID;
        uint32_t unstrStrPropKey = database->getCatalog()
                                       ->getReadOnlyVersion()
                                       ->getNodeProperty(personTableID, "us0")
                                       .propertyID;
        // Inserting until 1100 ensures that chunk 1 is filled and a new chunk 2 is started
        for (node_offset_t nodeOffset = 601; nodeOffset < 1100; ++nodeOffset) {
            personNodeTable->getUnstrPropertyLists()->initEmptyPropertyLists(nodeOffset);
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
    unique_ptr<InMemOverflowBuffer> inMemOverflowBuffer;
    table_id_t personTableID;
    Value existingStrVal, existingIntVal, shortStrVal, longStrVal, intVal;
};

// TODO(Semih): Uncomment when enabling ad-hoc properties
// TODO(Xiyang): migrate to delete
// TEST_F(UnstructuredPropertyListsUpdateTests,
//    RemoveAllPropertiesBySetPropertyListEmptyCommitNormalExecution) {
//    removeAllPropertiesBySetPropertyListEmptyTest(
//        true /* commit */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(UnstructuredPropertyListsUpdateTests,
//    RemoveAllPropertiesBySetPropertyListEmptyRollbackNormalExecution) {
//    removeAllPropertiesBySetPropertyListEmptyTest(
//        false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(
//    UnstructuredPropertyListsUpdateTests, RemoveAllPropertiesBySetPropertyListEmptyCommitRecovery)
//    { removeAllPropertiesBySetPropertyListEmptyTest(true /* commit */,
//    TransactionTestType::RECOVERY);
//}
//
// TEST_F(UnstructuredPropertyListsUpdateTests,
//    RemoveAllPropertiesBySetPropertyListEmptyRollbackRecovery) {
//    removeAllPropertiesBySetPropertyListEmptyTest(
//        false /* rollback */, TransactionTestType::RECOVERY);
//}
//
//// TODO(Semih/Ziyi): the following tests should be removed and moved into benchmark.
// TEST_F(UnstructuredPropertyListsUpdateTests, RemoveAllPropertiesOfAllNodesCommitNormalExecution)
// {
//    removeAllPropertiesOfAllNodesTest(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(UnstructuredPropertyListsUpdateTests,
// RemoveAllPropertiesOfAllNodesRollbackNormalExecution) {
//    removeAllPropertiesOfAllNodesTest(false /* rollback */,
//    TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(UnstructuredPropertyListsUpdateTests, RemoveAllPropertiesOfAllNodesCommitRecovery) {
//    removeAllPropertiesOfAllNodesTest(true /* commit */, TransactionTestType::RECOVERY);
//}
//
// TEST_F(UnstructuredPropertyListsUpdateTests, RemoveAllPropertiesOfAllNodesRollbackRecovery) {
//    removeAllPropertiesOfAllNodesTest(false /* rollback */, TransactionTestType::RECOVERY);
//}
//
// TEST_F(UnstructuredPropertyListsUpdateTests, RemoveAndWriteDataTwiceTestCommitNormalExecution) {
//    removeAndWriteDataTwiceTest(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(UnstructuredPropertyListsUpdateTests, RemoveAndWriteDataTwiceTestRollbackNormalExecution)
// {
//    removeAndWriteDataTwiceTest(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(UnstructuredPropertyListsUpdateTests, RemoveAndWriteDataTwiceTestCommitRecovery) {
//    removeAndWriteDataTwiceTest(true /* commit */, TransactionTestType::RECOVERY);
//}
//
// TEST_F(UnstructuredPropertyListsUpdateTests, RemoveAndWriteDataTwiceTestRollbackRecovery) {
//    removeAndWriteDataTwiceTest(false /* rollback */, TransactionTestType::RECOVERY);
//}
//
//// TODO(Xiyang): migrate to create
// TEST_F(UnstructuredPropertyListsUpdateTests, AddNewListTestCommitNormalExecution) {
//    addNewListsTest(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(UnstructuredPropertyListsUpdateTests, AddNewListTestRollbackNormalExecution) {
//    addNewListsTest(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(UnstructuredPropertyListsUpdateTests, AddNewListTestCommitRecovery) {
//    addNewListsTest(true /* commit */, TransactionTestType::RECOVERY);
//}
//
// TEST_F(UnstructuredPropertyListsUpdateTests, AddNewListTestRollbackRecovery) {
//    addNewListsTest(false /* rollback */, TransactionTestType::RECOVERY);
//}

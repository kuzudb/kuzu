#include "test/test_utility/include/test_helper.h"

#include "src/storage/include/wal_replayer.h"

using namespace graphflow::storage;
using namespace graphflow::testing;

// Note: ID and nodeOffset in this test are equal for each node, so we use nodeID and nodeOffset
// interchangeably.
class NodeInsertionDeletionTests : public DBTest {

public:
    void SetUp() override {
        DBTest::SetUp();
        initDBAndConnection();
    }

    string getInputCSVDir() override { return "dataset/node-insertion-deletion-tests/"; }

    void initDBAndConnection() {
        createDBAndConn();
        readConn = make_unique<Connection>(database.get());
        table_id_t personTableID =
            database->getCatalog()->getReadOnlyVersion()->getNodeTableIDFromName("person");
        personNodeTable = database->getStorageManager()->getNodesStore().getNode(personTableID);
        uint32_t idPropertyID = database->getCatalog()
                                    ->getReadOnlyVersion()
                                    ->getNodeProperty(personTableID, "ID")
                                    .propertyID;
        idColumn = database->getStorageManager()->getNodesStore().getNodePropertyColumn(
            personTableID, idPropertyID);
        conn->beginWriteTransaction();
    }

    void commitOrRollbackConnectionAndInitDBIfNecessary(
        bool isCommit, TransactionTestType transactionTestType) {
        commitOrRollbackConnection(isCommit, transactionTestType);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            // This creates a new database/conn/readConn and should run the recovery algorithm
            initDBAndConnection();
        }
    }

    node_offset_t addNode() {
        // TODO(Guodong/Semih/Xiyang): Currently it is not clear when and from where the hash index,
        // structured columns, unstructured property lists, adjacency lists, and adj columns of a
        // newly added node should be informed that a new node is being inserted, so these data
        // structures either write values or NULLs or empty lists etc. Within the scope of these
        // tests we only have an ID column and we are manually from outside NodesMetadata adding
        // a NULL value for the ID. This should change later.
        node_offset_t nodeOffset =
            personNodeTable->getNodesMetadata()->addNode(personNodeTable->getTableID());
        auto dataChunk = make_shared<DataChunk>(2);
        // Flatten the data chunk
        dataChunk->state->currIdx = 0;
        auto nodeIDVector = make_shared<ValueVector>(NODE_ID, database->getMemoryManager());
        dataChunk->insert(0, nodeIDVector);
        auto idVector = make_shared<ValueVector>(INT64, database->getMemoryManager());
        dataChunk->insert(1, idVector);
        ((nodeID_t*)nodeIDVector->values)[0].offset = nodeOffset;
        idVector->setNull(0, true /* is null */);
        idColumn->writeValues(nodeIDVector, idVector);
        return nodeOffset;
    }

    void checkNodeWithIDExists(Connection* connection, uint64_t nodeID) {
        auto result =
            connection->query("MATCH (a:person) WHERE a.ID=" + to_string(nodeID) + " RETURN a.ID");
        ASSERT_EQ(result->getNext()->getValue(0)->val.int64Val, nodeID);
    }

    void checkNodeWithIDDoesNotExist(Connection* connection, uint64_t nodeID) {
        auto result =
            connection->query("MATCH (a:person) WHERE a.ID=" + to_string(nodeID) + " RETURN a.ID");
        ASSERT_FALSE(result->hasNext());
    }

    void testScanAfterDeletion(bool isCommit) {
        checkNodeWithIDExists(readConn.get(), 10);
        checkNodeWithIDExists(conn.get(), 10);
        auto tableID = personNodeTable->getTableID();
        // Delete 2 from morsel 0 and 1 from morsel 3
        personNodeTable->getNodesMetadata()->deleteNode(
            tableID, 10 /* offset of person w/ ID 10, who has no edges */);
        personNodeTable->getNodesMetadata()->deleteNode(
            tableID, 1400 /* offset of person w/ ID 1400, who has no edges */);
        personNodeTable->getNodesMetadata()->deleteNode(
            tableID, 6000 /* offset of person w/ ID 6000, who has no edges */);

        checkNodeWithIDExists(readConn.get(), 10);
        checkNodeWithIDExists(readConn.get(), 1400);
        checkNodeWithIDExists(readConn.get(), 6000);

        checkNodeWithIDDoesNotExist(conn.get(), 10);
        checkNodeWithIDDoesNotExist(conn.get(), 1400);
        checkNodeWithIDDoesNotExist(conn.get(), 6000);

        // Further test after commit. Further delete one more and commit. Further verify numNodes.
        // We need to commit read transaction; otherwise the write trx will timeout.
        if (isCommit) {
            conn->commit();
        } else {
            conn->rollback();
        }

        conn->beginWriteTransaction();
        if (isCommit) {
            checkNodeWithIDDoesNotExist(readConn.get(), 10);
            checkNodeWithIDDoesNotExist(readConn.get(), 1400);
            checkNodeWithIDDoesNotExist(readConn.get(), 6000);
            checkNodeWithIDDoesNotExist(conn.get(), 10);
            checkNodeWithIDDoesNotExist(conn.get(), 1400);
            checkNodeWithIDDoesNotExist(conn.get(), 6000);
        } else {
            checkNodeWithIDExists(readConn.get(), 10);
            checkNodeWithIDExists(readConn.get(), 1400);
            checkNodeWithIDExists(readConn.get(), 6000);
            checkNodeWithIDExists(conn.get(), 10);
            checkNodeWithIDExists(conn.get(), 1400);
            checkNodeWithIDExists(conn.get(), 6000);
        }
    }

    void testDeleteAllNodes(bool isCommit, TransactionTestType transactionTestType) {
        for (node_offset_t nodeOffset = 0; nodeOffset <= 9999; ++nodeOffset) {
            personNodeTable->getNodesMetadata()->deleteNode(
                personNodeTable->getTableID(), nodeOffset);
        }
        string query = "MATCH (a:person) RETURN count(DISTINCT a.ID)";
        ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->val.int64Val, 0);
        ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->val.int64Val, 10000);

        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        if (isCommit) {
            ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->val.int64Val, 0);
            ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->val.int64Val, 0);
        } else {
            ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->val.int64Val, 10000);
            ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->val.int64Val, 10000);
        }
    }

    void testSimpleAdd(bool isCommit, TransactionTestType transactionTestType) {
        for (int i = 0; i < 3000; ++i) {
            addNode();
        }
        auto tableID = personNodeTable->getTableID();
        ASSERT_EQ(personNodeTable->getNodesMetadata()->getMaxNodeOffset(
                      TransactionType::READ_ONLY, tableID),
            9999);
        ASSERT_EQ(
            personNodeTable->getNodesMetadata()->getMaxNodeOffset(TransactionType::WRITE, tableID),
            12999);
        string query = "MATCH (a:person) RETURN count(*)";
        ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->val.int64Val, 13000);
        ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->val.int64Val, 10000);

        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);

        if (isCommit) {
            ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->val.int64Val, 13000);
            ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->val.int64Val, 13000);
            ASSERT_EQ(personNodeTable->getNodesMetadata()->getMaxNodeOffset(
                          TransactionType::READ_ONLY, tableID),
                12999);
            ASSERT_EQ(personNodeTable->getNodesMetadata()->getMaxNodeOffset(
                          TransactionType::WRITE, tableID),
                12999);
        } else {
            ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->val.int64Val, 10000);
            ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->val.int64Val, 10000);
            ASSERT_EQ(personNodeTable->getNodesMetadata()->getMaxNodeOffset(
                          TransactionType::READ_ONLY, tableID),
                9999);
            ASSERT_EQ(personNodeTable->getNodesMetadata()->getMaxNodeOffset(
                          TransactionType::WRITE, tableID),
                9999);
        }
    }

public:
    unique_ptr<Connection> readConn;
    NodeTable* personNodeTable;
    Column* idColumn;
};

TEST_F(NodeInsertionDeletionTests, DeletingSameNodeOffsetErrorsTest) {
    personNodeTable->getNodesMetadata()->deleteNode(
        personNodeTable->getTableID(), 3 /* person w/ offset/ID 3 */);
    try {
        personNodeTable->getNodesMetadata()->deleteNode(personNodeTable->getTableID(),
            3 /* person w/ offset/ID 3 again, which should error  */);
        FAIL();
    } catch (RuntimeException& e) {
    } catch (Exception& e) { FAIL(); }
}

TEST_F(NodeInsertionDeletionTests, ScanAfterCommittedDeletionTest) {
    testScanAfterDeletion(true /* is commit */);
}

TEST_F(NodeInsertionDeletionTests, ScanAfterRolledbackDeletionTest) {
    testScanAfterDeletion(false /* is rollback */);
}

TEST_F(NodeInsertionDeletionTests, DeleteEntireMorselTest) {
    for (node_offset_t nodeOffset = 2048; nodeOffset < 4096; ++nodeOffset) {
        personNodeTable->getNodesMetadata()->deleteNode(personNodeTable->getTableID(), nodeOffset);
    }
    string query = "MATCH (a:person) WHERE a.ID >= 2048 AND a.ID < 4096 RETURN count(*)";
    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->val.int64Val, 0);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->val.int64Val, 2048);
    conn->commit();
    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->val.int64Val, 0);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->val.int64Val, 0);
}

TEST_F(NodeInsertionDeletionTests, DeleteAllNodesCommitTest) {
    testDeleteAllNodes(true /* is commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(NodeInsertionDeletionTests, DeleteAllNodesCommitRecoveryTest) {
    testDeleteAllNodes(true /* is commit */, TransactionTestType::RECOVERY);
}

TEST_F(NodeInsertionDeletionTests, DeleteAllNodesRollbackTest) {
    testDeleteAllNodes(false /* is rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(NodeInsertionDeletionTests, DeleteAllNodesRollbackRecoveryTest) {
    testDeleteAllNodes(false /* is rollback */, TransactionTestType::RECOVERY);
}

// TODO(Guodong): We need to extend these tests with queries that verify that the IDs are deleted,
// added, and can be queried correctly.
TEST_F(NodeInsertionDeletionTests, SimpleAddCommitTest) {
    testSimpleAdd(true /* is commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(NodeInsertionDeletionTests, SimpleAddCommitRecoveryTest) {
    testSimpleAdd(true /* is commit */, TransactionTestType::RECOVERY);
}

TEST_F(NodeInsertionDeletionTests, SimpleAddRollbackTest) {
    testSimpleAdd(false /* is rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(NodeInsertionDeletionTests, SimpleAddRollbackRecoveryTest) {
    testSimpleAdd(false /* is rollback */, TransactionTestType::RECOVERY);
}

TEST_F(NodeInsertionDeletionTests, DeleteAddMixedTest) {
    for (node_offset_t nodeOffset = 1000; nodeOffset < 9000; ++nodeOffset) {
        personNodeTable->getNodesMetadata()->deleteNode(personNodeTable->getTableID(), nodeOffset);
    }
    for (int i = 0; i < 8000; ++i) {
        auto nodeOffset = addNode();
        ASSERT_TRUE(nodeOffset >= 1000 && nodeOffset < 9000);
    }

    // Add two additional node offsets
    for (int i = 0; i < 10; ++i) {
        auto nodeOffset = addNode();
        ASSERT_EQ(nodeOffset, 10000 + i);
    }

    string query = "MATCH (a:person) RETURN count(*)";
    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->val.int64Val, 10010);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->val.int64Val, 10000);
    conn->commit();
    conn->beginWriteTransaction();
    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->val.int64Val, 10010);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->val.int64Val, 10010);

    for (node_offset_t nodeOffset = 0; nodeOffset < 10010; ++nodeOffset) {
        personNodeTable->getNodesMetadata()->deleteNode(personNodeTable->getTableID(), nodeOffset);
    }

    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->val.int64Val, 0);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->val.int64Val, 10010);
    conn->commit();
    conn->beginWriteTransaction();
    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->val.int64Val, 0);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->val.int64Val, 0);

    for (int i = 0; i < 5000; ++i) {
        auto nodeOffset = addNode();
        ASSERT_TRUE(nodeOffset >= 0 && nodeOffset < 10010);
    }

    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->val.int64Val, 5000);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->val.int64Val, 0);
    conn->commit();
    conn->beginWriteTransaction();
    ASSERT_EQ(conn->query(query)->getNext()->getValue(0)->val.int64Val, 5000);
    ASSERT_EQ(readConn->query(query)->getNext()->getValue(0)->val.int64Val, 5000);
}

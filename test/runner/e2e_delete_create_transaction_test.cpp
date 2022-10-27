#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

class DeleteCreateTransactionTest : public DBTest {
public:
    void SetUp() override {
        DBTest::SetUp();
        readConn = make_unique<Connection>(database.get());
    }

    string getInputCSVDir() override { return "dataset/node-insertion-deletion-tests/"; }

    void checkNodeExists(Connection* connection, uint64_t nodeID) {
        auto result =
            connection->query("MATCH (a:person) WHERE a.ID=" + to_string(nodeID) + " RETURN a.ID");
        ASSERT_EQ(result->getNext()->getResultValue(0)->getInt64Val(), nodeID);
    }

    void checkNodeNotExists(Connection* connection, uint64_t nodeID) {
        auto result =
            connection->query("MATCH (a:person) WHERE a.ID=" + to_string(nodeID) + " RETURN a.ID");
        ASSERT_FALSE(result->hasNext());
    }

    void deleteNodes(Connection* connection, vector<uint64_t> nodeIDs) {
        auto predicate = "WHERE a.ID=" + to_string(nodeIDs[0]);
        for (auto i = 1u; i < nodeIDs.size(); ++i) {
            predicate += " OR a.ID=" + to_string(nodeIDs[i]);
        }
        auto query = "MATCH (a:person) " + predicate + " DELETE a";
        auto result = connection->query(query);
        ASSERT_TRUE(result->isSuccess());
    }

    int64_t getCountStarVal(Connection* connection, const string& query) {
        auto result = connection->query(query);
        return result->getNext()->getResultValue(0)->getInt64Val();
    }

    int64_t getNumNodes(Connection* connection) {
        auto result = connection->query("MATCH (:person) RETURN count(*)");
        return result->getNext()->getResultValue(0)->getInt64Val();
    }

    void addNodes(Connection* connection, uint64_t numNodes) {
        for (auto i = 0u; i < numNodes; ++i) {
            auto id = 10000 + i;
            auto result = conn->query("CREATE (a:person {ID: " + to_string(id) + "})");
            ASSERT_TRUE(result->isSuccess());
        }
    }

public:
    unique_ptr<Connection> readConn;
};

TEST_F(DeleteCreateTransactionTest, ReadBeforeCommit) {
    auto nodeIDsToDelete = vector<uint64_t>{10, 1400, 6000};
    conn->beginWriteTransaction();
    deleteNodes(conn.get(), nodeIDsToDelete);
    for (auto& nodeIDToDelete : nodeIDsToDelete) {
        checkNodeExists(readConn.get(), nodeIDToDelete);
        checkNodeNotExists(conn.get(), nodeIDToDelete);
    }
}

TEST_F(DeleteCreateTransactionTest, ReadAfterCommitNormalExecution) {
    auto nodeIDsToDelete = vector<uint64_t>{10, 1400, 6000};
    conn->beginWriteTransaction();
    deleteNodes(conn.get(), nodeIDsToDelete);
    conn->commit();
    for (auto& nodeIDToDelete : nodeIDsToDelete) {
        checkNodeNotExists(readConn.get(), nodeIDToDelete);
        checkNodeNotExists(conn.get(), nodeIDToDelete);
    }
}

TEST_F(DeleteCreateTransactionTest, ReadAfterRollbackNormalExecution) {
    auto nodeIDsToDelete = vector<uint64_t>{10, 1400, 6000};
    conn->beginWriteTransaction();
    deleteNodes(conn.get(), nodeIDsToDelete);
    conn->rollback();
    for (auto& nodeIDToDelete : nodeIDsToDelete) {
        checkNodeExists(readConn.get(), nodeIDToDelete);
        checkNodeExists(conn.get(), nodeIDToDelete);
    }
}

TEST_F(DeleteCreateTransactionTest, DeleteEntireMorselCommitNormalExecution) {
    conn->beginWriteTransaction();
    conn->query("MATCH (a:person) WHERE a.ID < 4096 DELETE a");
    auto query = "MATCH (a:person) WHERE a.ID < 4096 RETURN count(*)";
    ASSERT_EQ(getCountStarVal(conn.get(), query), 0);
    ASSERT_EQ(getCountStarVal(readConn.get(), query), 4096);
    conn->commit();
    ASSERT_EQ(getCountStarVal(conn.get(), query), 0);
    ASSERT_EQ(getCountStarVal(readConn.get(), query), 0);
}

TEST_F(DeleteCreateTransactionTest, DeleteAllNodesCommitNormalExecution) {
    conn->beginWriteTransaction();
    conn->query("MATCH (a:person) DELETE a");
    auto query = "MATCH (a:person) RETURN count(DISTINCT a.ID)";
    ASSERT_EQ(getCountStarVal(conn.get(), query), 0);
    ASSERT_EQ(getCountStarVal(readConn.get(), query), 10000);
    conn->commit();
    ASSERT_EQ(getCountStarVal(conn.get(), query), 0);
    ASSERT_EQ(getCountStarVal(readConn.get(), query), 0);
}

TEST_F(DeleteCreateTransactionTest, DeleteAllNodesCommitRecovery) {
    conn->beginWriteTransaction();
    conn->query("MATCH (a:person) DELETE a");
    conn->commitButSkipCheckpointingForTestingRecovery();
    // This should run the recovery algorithm
    createDBAndConn();
    auto query = "MATCH (a:person) RETURN count(DISTINCT a.ID)";
    ASSERT_EQ(getCountStarVal(conn.get(), query), 0);
}

TEST_F(DeleteCreateTransactionTest, DeleteAllNodesRollbackNormalExecution) {
    conn->beginWriteTransaction();
    conn->query("MATCH (a:person) DELETE a");
    conn->rollback();
    auto query = "MATCH (a:person) RETURN count(DISTINCT a.ID)";
    ASSERT_EQ(getCountStarVal(conn.get(), query), 10000);
    ASSERT_EQ(getCountStarVal(readConn.get(), query), 10000);
}

TEST_F(DeleteCreateTransactionTest, DeleteAllNodesRollbackRecovery) {
    conn->beginWriteTransaction();
    conn->query("MATCH (a:person) DELETE a");
    conn->rollbackButSkipCheckpointingForTestingRecovery();
    // This should run the recovery algorithm
    createDBAndConn();
    auto query = "MATCH (a:person) RETURN count(DISTINCT a.ID)";
    ASSERT_EQ(getCountStarVal(conn.get(), query), 10000);
}

// TODO(Guodong): We need to extend these tests with queries that verify that the IDs are deleted,
// added, and can be queried correctly.
TEST_F(DeleteCreateTransactionTest, SimpleAddCommitInTrx) {
    conn->beginWriteTransaction();
    addNodes(conn.get(), 1000);
    ASSERT_EQ(getNumNodes(conn.get()), 11000);
    ASSERT_EQ(getNumNodes(readConn.get()), 10000);
}

TEST_F(DeleteCreateTransactionTest, SimpleAddCommitNormalExecution) {
    conn->beginWriteTransaction();
    addNodes(conn.get(), 1000);
    conn->commit();
    ASSERT_EQ(getNumNodes(conn.get()), 11000);
}

TEST_F(DeleteCreateTransactionTest, SimpleAddCommitRecovery) {
    conn->beginWriteTransaction();
    addNodes(conn.get(), 1000);
    conn->commitButSkipCheckpointingForTestingRecovery();
    createDBAndConn(); // run recovery
    ASSERT_EQ(getNumNodes(conn.get()), 11000);
}

TEST_F(DeleteCreateTransactionTest, SimpleAddRollbackNormalExecution) {
    conn->beginWriteTransaction();
    addNodes(conn.get(), 1000);
    conn->rollback();
    ASSERT_EQ(getNumNodes(conn.get()), 10000);
}

TEST_F(DeleteCreateTransactionTest, SimpleAddRollbackRecovery) {
    conn->beginWriteTransaction();
    addNodes(conn.get(), 1000);
    conn->rollbackButSkipCheckpointingForTestingRecovery();
    createDBAndConn(); // run recovery
    ASSERT_EQ(getNumNodes(conn.get()), 10000);
}

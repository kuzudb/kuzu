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
        ASSERT_EQ(result->getNext()->getValue(0)->val.int64Val, nodeID);
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
        assert(result->isSuccess());
    }

    int64_t getCountStarVal(Connection* connection, const string& query) {
        auto result = connection->query(query);
        return result->getNext()->getValue(0)->val.int64Val;
    }

    void add100Nodes(Connection* connection) {
        for (auto i = 0u; i < 100; ++i) {
            auto id = 10000 + i;
            auto result = conn->query("CREATE (a:person {ID: " + to_string(id) + "})");
            assert(result->isSuccess());
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

TEST_F(DeleteCreateTransactionTest, SimpleAddCommitNormalExecution) {
    conn->beginWriteTransaction();
    add100Nodes(conn.get());
    string query = "MATCH (a:person) RETURN count(*)";
    ASSERT_EQ(getCountStarVal(conn.get(), query), 10100);
    ASSERT_EQ(getCountStarVal(readConn.get(), query), 10000);
    conn->commit();
    ASSERT_EQ(getCountStarVal(conn.get(), query), 10100);
    ASSERT_EQ(getCountStarVal(readConn.get(), query), 10100);
}

TEST_F(DeleteCreateTransactionTest, SimpleAddCommitRecovery) {
    conn->beginWriteTransaction();
    add100Nodes(conn.get());
    conn->commitButSkipCheckpointingForTestingRecovery();
    // This should run the recovery algorithm
    createDBAndConn();
    string query = "MATCH (a:person) RETURN count(*)";
    ASSERT_EQ(getCountStarVal(conn.get(), query), 10100);
}

TEST_F(DeleteCreateTransactionTest, SimpleAddRollbackNormalExecution) {
    conn->beginWriteTransaction();
    add100Nodes(conn.get());
    conn->rollback();
    string query = "MATCH (a:person) RETURN count(*)";
    ASSERT_EQ(getCountStarVal(conn.get(), query), 10000);
    ASSERT_EQ(getCountStarVal(readConn.get(), query), 10000);
}

TEST_F(DeleteCreateTransactionTest, SimpleAddRollbackRecovery) {
    conn->beginWriteTransaction();
    add100Nodes(conn.get());
    conn->rollbackButSkipCheckpointingForTestingRecovery();
    // This should run the recovery algorithm
    createDBAndConn();
    string query = "MATCH (a:person) RETURN count(*)";
    ASSERT_EQ(getCountStarVal(conn.get(), query), 10000);
}

// TODO(Guodong/Xiyang): Fix this test once we support properties in CREATE clause
// TEST_F(DeleteCreateTransactionTest, DeleteAddMixedTest)  {
//    conn->beginWriteTransaction();
//    conn->query("MATCH (a:person) WHERE a.ID >= 1000 AND a.ID < 1100 DELETE a");
//    add100Nodes(conn.get());
//    add100Nodes(conn.get());
//    string query = "MATCH (a:person) RETURN count(*)";
//    ASSERT_EQ(getCountStarVal(conn.get(), query), 10100);
//    ASSERT_EQ(getCountStarVal(readConn.get(), query), 10000);
//    conn->commit();
//    ASSERT_EQ(getCountStarVal(readConn.get(), query), 10100);
//    conn->beginWriteTransaction();
//    conn->query("MATCH (a:person) DELETE a");
//    ASSERT_EQ(getCountStarVal(conn.get(), query), 0);
//    ASSERT_EQ(getCountStarVal(readConn.get(), query), 10100);
//    conn->commit();
//    ASSERT_EQ(getCountStarVal(readConn.get(), query), 0);
//    conn->beginWriteTransaction();
//    add100Nodes(conn.get());
//    ASSERT_EQ(getCountStarVal(conn.get(), query), 100);
//    ASSERT_EQ(getCountStarVal(readConn.get(), query), 0);
//    conn->commit();
//    ASSERT_EQ(getCountStarVal(readConn.get(), query), 100);
//}

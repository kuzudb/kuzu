#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

class DeleteTransactionTest : public BaseGraphLoadingTest {

public:
    string getInputCSVDir() override { return "dataset/node-insertion-deletion-tests/"; }

    void SetUp() override {
        BaseGraphLoadingTest::SetUp();
        createDBAndConn();
        readConn = make_unique<Connection>(database.get());
    }

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

public:
    unique_ptr<Connection> readConn;
};

TEST_F(DeleteTransactionTest, ReadBeforeCommit) {
    auto nodeIDsToDelete = vector<uint64_t>{10, 1400, 6000};
    conn->beginWriteTransaction();
    deleteNodes(conn.get(), nodeIDsToDelete);
    for (auto& nodeIDToDelete : nodeIDsToDelete) {
        checkNodeExists(readConn.get(), nodeIDToDelete);
        checkNodeNotExists(conn.get(), nodeIDToDelete);
    }
}

TEST_F(DeleteTransactionTest, ReadAfterCommit) {
    auto nodeIDsToDelete = vector<uint64_t>{10, 1400, 6000};
    conn->beginWriteTransaction();
    deleteNodes(conn.get(), nodeIDsToDelete);
    conn->commit();
    for (auto& nodeIDToDelete : nodeIDsToDelete) {
        checkNodeNotExists(readConn.get(), nodeIDToDelete);
        checkNodeNotExists(conn.get(), nodeIDToDelete);
    }
}

TEST_F(DeleteTransactionTest, ReadAfterRollback) {
    auto nodeIDsToDelete = vector<uint64_t>{10, 1400, 6000};
    conn->beginWriteTransaction();
    deleteNodes(conn.get(), nodeIDsToDelete);
    conn->rollback();
    for (auto& nodeIDToDelete : nodeIDsToDelete) {
        checkNodeExists(readConn.get(), nodeIDToDelete);
        checkNodeExists(conn.get(), nodeIDToDelete);
    }
}

TEST_F(DeleteTransactionTest, DeleteSameNodeErrorTest) {
    auto nodeIDsToDelete = vector<uint64_t>{3};
    deleteNodes(conn.get(), nodeIDsToDelete);
    auto result =
        conn->query("MATCH (a:person) WHERE a.ID=" + to_string(nodeIDsToDelete[0]) + " DELETE a");
    ASSERT_FALSE(result->isSuccess());
    ASSERT_STREQ(result->getErrorMessage().c_str(),
        "Runtime exception: Node with offset 3 is already deleted.");
}

TEST_F(DeleteTransactionTest, DeleteEntireMorselTest) {
    conn->beginWriteTransaction();
    conn->query("MATCH (a:person) WHERE a.ID < 4096 DELETE a");
    auto query = "MATCH (a:person) WHERE a.ID < 4096 RETURN count(*)";
    ASSERT_EQ(getCountStarVal(conn.get(), query), 0);
    ASSERT_EQ(getCountStarVal(readConn.get(), query), 4096);
    conn->commit();
    ASSERT_EQ(getCountStarVal(conn.get(), query), 0);
    ASSERT_EQ(getCountStarVal(readConn.get(), query), 0);
}

TEST_F(DeleteTransactionTest, DeleteAllNodesCommitTest) {
    conn->beginWriteTransaction();
    conn->query("MATCH (a:person) DELETE a");
    auto query = "MATCH (a:person) RETURN count(DISTINCT a.ID)";
    ASSERT_EQ(getCountStarVal(conn.get(), query), 0);
    ASSERT_EQ(getCountStarVal(readConn.get(), query), 10000);
    conn->commit();
    ASSERT_EQ(getCountStarVal(conn.get(), query), 0);
    ASSERT_EQ(getCountStarVal(readConn.get(), query), 0);
}

TEST_F(DeleteTransactionTest, DeleteAllNodesCommitRecoveryTest) {
    conn->beginWriteTransaction();
    conn->query("MATCH (a:person) DELETE a");
    conn->commitButSkipCheckpointingForTestingRecovery();
    // This should run the recovery algorithm
    createDBAndConn();
    auto query = "MATCH (a:person) RETURN count(DISTINCT a.ID)";
    ASSERT_EQ(getCountStarVal(conn.get(), query), 0);
}

TEST_F(DeleteTransactionTest, DeleteAllNodesRollbackTest) {
    conn->beginWriteTransaction();
    conn->query("MATCH (a:person) DELETE a");
    conn->rollback();
    auto query = "MATCH (a:person) RETURN count(DISTINCT a.ID)";
    ASSERT_EQ(getCountStarVal(conn.get(), query), 10000);
    ASSERT_EQ(getCountStarVal(readConn.get(), query), 10000);
}

TEST_F(DeleteTransactionTest, DeleteAllNodesRollbackRecoveryTest) {
    conn->beginWriteTransaction();
    conn->query("MATCH (a:person) DELETE a");
    conn->rollbackButSkipCheckpointingForTestingRecovery();
    // This should run the recovery algorithm
    createDBAndConn();
    auto query = "MATCH (a:person) RETURN count(DISTINCT a.ID)";
    ASSERT_EQ(getCountStarVal(conn.get(), query), 10000);
}

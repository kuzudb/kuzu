#include "graph_test/graph_test.h"

using namespace kuzu::testing;

class BaseDeleteCreateTrxTest : public DBTest {
public:
    void SetUp() override {
        DBTest::SetUp();
        readConn = std::make_unique<Connection>(database.get());
    }

    void initDBAndConnection() {
        createDBAndConn();
        readConn = std::make_unique<Connection>(database.get());
    }

    void commitOrRollbackConnectionAndInitDBIfNecessary(
        bool isCommit, TransactionTestType transactionTestType) {
        commitOrRollbackConnection(isCommit, transactionTestType);
        if (transactionTestType == TransactionTestType::RECOVERY) {
            // This creates a new database/conn/readConn and should run the recovery algorithm
            initDBAndConnection();
            conn->beginWriteTransaction();
        }
    }

    // Create nodes
    void createNodes(const std::vector<std::string>& nodeIDs) {
        for (auto& nodeID : nodeIDs) {
            createNode(nodeID);
        }
    }
    void createNodes(uint64_t numNodes, bool isStringPK) {
        for (auto i = 0u; i < numNodes; ++i) {
            auto key = 10000 + i;
            if (isStringPK) {
                createNode("'" + std::to_string(key) + "'");
            } else {
                createNode(std::to_string(key));
            }
        }
    }
    void createNode(const std::string& nodeID) {
        auto query = "CREATE (a:person {ID: " + nodeID + "})";
        ASSERT_TRUE(conn->query(query)->isSuccess());
    }
    // Delete nodes
    void deleteNodesInRange(uint64_t startOffset, uint64_t endOffset, bool isStringPK) {
        auto query =
            "MATCH (a:person) WHERE a.ID>=" +
            (isStringPK ? "'" + std::to_string(startOffset) + "'" : std::to_string(startOffset)) +
            " AND a.ID<" +
            (isStringPK ? "'" + std::to_string(startOffset) + "'" : std::to_string(startOffset)) +
            " DELETE a";
        ASSERT_TRUE(conn->query(query)->isSuccess());
    }
    void deleteNodes(const std::vector<std::string>& nodeIDs) {
        for (auto& nodeID : nodeIDs) {
            deleteNode(nodeID);
        }
    }
    void deleteNode(const std::string& nodeID) {
        auto query = "MATCH (a:person) WHERE a.ID=" + nodeID + " DELETE a";
        ASSERT_TRUE(conn->query(query)->isSuccess());
    }
    static int64_t getCount(Connection* connection, const std::string& queryPrefix) {
        auto result = connection->query(queryPrefix + " RETURN COUNT(*)");
        return result->getNext()->getValue(0)->getValue<int64_t>();
    }
    int64_t getNumNodes(Connection* connection) { return getCount(connection, "MATCH (:person)"); }
    void validateNodesExistOrNot(
        Connection* connection, const std::vector<std::string>& nodeIDs, bool exist) {
        for (auto& nodeID : nodeIDs) {
            validateNodeExistOrNot(connection, nodeID, exist);
        }
    }
    // Will trigger index scan.
    void validateNodeExistOrNot(Connection* connection, const std::string& nodeID, bool exist) {
        auto query = "MATCH (a:person) WHERE a.ID=" + nodeID;
        ASSERT_EQ(getCount(connection, query), exist ? 1 : 0);
    }

public:
    std::unique_ptr<Connection> readConn;
};

class CreateDeleteInt64NodeTrxTest : public BaseDeleteCreateTrxTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/node-insertion-deletion-tests/int64-pk/");
    }

    void testIndexScanAfterInsertion(bool isCommit, TransactionTestType trxTestType) {
        auto nodeIDsToInsert = std::vector<std::string>{"10003", "10005"};
        conn->beginWriteTransaction();
        createNodes(nodeIDsToInsert);
        validateNodesExistOrNot(conn.get(), nodeIDsToInsert, true /* exist */);
        validateNodesExistOrNot(readConn.get(), nodeIDsToInsert, false /* exist */);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, trxTestType);
        if (isCommit) {
            validateNodesExistOrNot(conn.get(), nodeIDsToInsert, true /* exist */);
        } else {
            validateNodesExistOrNot(conn.get(), nodeIDsToInsert, false /* exist */);
        }
    }

    void testIndexScanAfterDeletion(bool isCommit, TransactionTestType trxTestType) {
        auto nodeIDsToDelete = std::vector<std::string>{"10", "1400", "6000"};
        conn->beginWriteTransaction();
        deleteNodes(nodeIDsToDelete);
        validateNodesExistOrNot(conn.get(), nodeIDsToDelete, false /* exist */);
        validateNodesExistOrNot(readConn.get(), nodeIDsToDelete, true /* exist */);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, trxTestType);
        if (isCommit) {
            validateNodesExistOrNot(conn.get(), nodeIDsToDelete, false /* exist */);
        } else {
            validateNodesExistOrNot(conn.get(), nodeIDsToDelete, true /* exist */);
        }
    }

    void testDeleteAllNodes(bool isCommit, TransactionTestType trxTestType) {
        conn->beginWriteTransaction();
        conn->query("MATCH (a:person) DELETE a");
        ASSERT_EQ(getNumNodes(conn.get()), 0);
        ASSERT_EQ(getNumNodes(readConn.get()), 10000);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, trxTestType);
        if (isCommit) {
            ASSERT_EQ(getNumNodes(conn.get()), 0);
        } else {
            ASSERT_EQ(getNumNodes(conn.get()), 10000);
        }
    }

    void testSimpleInsertions(bool isCommit, TransactionTestType trxTestType) {
        conn->beginWriteTransaction();
        createNodes(1000, false /* int pk */);
        ASSERT_EQ(getNumNodes(conn.get()), 11000);
        ASSERT_EQ(getNumNodes(readConn.get()), 10000);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, trxTestType);
        if (isCommit) {
            ASSERT_EQ(getNumNodes(conn.get()), 11000);
        } else {
            ASSERT_EQ(getNumNodes(conn.get()), 10000);
        }
    }

    void testMixedDeleteAndInsert(bool isCommit, TransactionTestType trxTestType) {
        conn->beginWriteTransaction();
        deleteNodes(std::vector<std::string>{"8000", "9000"});
        createNodes(std::vector<std::string>{"8000", "9000", "10001", "10002"});
        ASSERT_EQ(getNumNodes(readConn.get()), 10000);
        ASSERT_EQ(getNumNodes(conn.get()), 10002);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, trxTestType);
        if (isCommit) {
            ASSERT_EQ(getNumNodes(readConn.get()), 10002);
        } else {
            ASSERT_EQ(getNumNodes(conn.get()), 10000);
        }
        conn->query("MATCH (a:person) DELETE a");
        if (isCommit) {
            ASSERT_EQ(getNumNodes(readConn.get()), 10002);
        } else {
            ASSERT_EQ(getNumNodes(readConn.get()), 10000);
        }
        ASSERT_EQ(getNumNodes(conn.get()), 0);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, trxTestType);
        if (isCommit) {
            ASSERT_EQ(getNumNodes(readConn.get()), 0);
        } else {
            ASSERT_EQ(getNumNodes(conn.get()), 10000);
        }
        createNode("10000099");
        validateNodeExistOrNot(readConn.get(), "10000099", false /* exist */);
        validateNodeExistOrNot(conn.get(), "10000099", true /* exist */);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, trxTestType);
        if (isCommit) {
            validateNodeExistOrNot(readConn.get(), "10000099", true /* exist */);
        } else {
            validateNodeExistOrNot(conn.get(), "10000099", false /* not exist */);
        }
    }
};

class CreateDeleteStringNodeTrxTest : public BaseDeleteCreateTrxTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/node-insertion-deletion-tests/string-pk/");
    }

    void testIndexScanAfterInsertion(bool isCommit, TransactionTestType trxTestType) {
        auto nodeIDsToInsert = std::vector<std::string>{"'abcdefg'", "'huy23b287sfw33232'"};
        conn->beginWriteTransaction();
        createNodes(nodeIDsToInsert);
        validateNodesExistOrNot(conn.get(), nodeIDsToInsert, true /* exist */);
        validateNodesExistOrNot(readConn.get(), nodeIDsToInsert, false /* not exist */);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, trxTestType);
        if (isCommit) {
            validateNodesExistOrNot(readConn.get(), nodeIDsToInsert, true /* exist */);
        } else {
            validateNodesExistOrNot(conn.get(), nodeIDsToInsert, false /* not exist */);
        }
    }

    void testIndexScanAfterDeletion(bool isCommit, TransactionTestType trxTestType) {
        auto nodeIDsToDelete =
            std::vector<std::string>{"'999999999999'", "'1000000000000'", "'1000000000001'"};
        conn->beginWriteTransaction();
        deleteNodes(nodeIDsToDelete);
        validateNodesExistOrNot(conn.get(), nodeIDsToDelete, false /* not exist */);
        validateNodesExistOrNot(readConn.get(), nodeIDsToDelete, true /* exist */);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, trxTestType);
        if (isCommit) {
            validateNodesExistOrNot(readConn.get(), nodeIDsToDelete, false /* not exist */);
        } else {
            validateNodesExistOrNot(conn.get(), nodeIDsToDelete, true /* exist */);
        }
    }

    void testDeleteAllNodes(bool isCommit, TransactionTestType trxTestType) {
        conn->beginWriteTransaction();
        conn->query("MATCH (a:person) DELETE a");
        ASSERT_EQ(getNumNodes(conn.get()), 0);
        ASSERT_EQ(getNumNodes(readConn.get()), 10000);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, trxTestType);
        if (isCommit) {
            ASSERT_EQ(getNumNodes(readConn.get()), 0);
        } else {
            ASSERT_EQ(getNumNodes(conn.get()), 10000);
        }
    }

    void testSimpleInsertions(bool isCommit, TransactionTestType trxTestType) {
        conn->beginWriteTransaction();
        createNodes(1000, true /* string pk */);
        ASSERT_EQ(getNumNodes(conn.get()), 11000);
        ASSERT_EQ(getNumNodes(readConn.get()), 10000);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, trxTestType);
        if (isCommit) {
            ASSERT_EQ(getNumNodes(readConn.get()), 11000);
        } else {
            ASSERT_EQ(getNumNodes(conn.get()), 10000);
        }
    }

    void testMixedDeleteAndInsert(bool isCommit, TransactionTestType trxTestType) {
        conn->beginWriteTransaction();
        deleteNodes(std::vector<std::string>{"'999999995061'", "'1000000004126'"});
        createNodes(std::vector<std::string>{
            "'kmjiowe89njsn'", "'jdsknewkew'", "'jsnjwe893n2juihi3232'", "'jsnjwe893n2juihi3ew'"});
        ASSERT_EQ(getNumNodes(readConn.get()), 10000);
        ASSERT_EQ(getNumNodes(conn.get()), 10002);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, trxTestType);
        if (isCommit) {
            ASSERT_EQ(getNumNodes(readConn.get()), 10002);
        } else {
            ASSERT_EQ(getNumNodes(conn.get()), 10000);
        }
        conn->query("MATCH (a:person) DELETE a");
        if (isCommit) {
            ASSERT_EQ(getNumNodes(readConn.get()), 10002);
        } else {
            ASSERT_EQ(getNumNodes(readConn.get()), 10000);
        }
        ASSERT_EQ(getNumNodes(conn.get()), 0);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, trxTestType);
        if (isCommit) {
            ASSERT_EQ(getNumNodes(readConn.get()), 0);
        } else {
            ASSERT_EQ(getNumNodes(conn.get()), 10000);
        }
        createNode("'key0'");
        validateNodeExistOrNot(readConn.get(), "'key0'", false /* exist */);
        validateNodeExistOrNot(conn.get(), "'key0'", true /* exist */);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, trxTestType);
        if (isCommit) {
            validateNodeExistOrNot(readConn.get(), "'key0'", true /* exist */);
        } else {
            validateNodeExistOrNot(conn.get(), "'key0'", false /* not exist */);
        }
    }
};

class DeleteNodeWithEdgesErrorTest : public EmptyDBTest {
public:
    void SetUp() override {
        EmptyDBTest::SetUp();
        createDBAndConn();
    }
};

class NodeInsertionDeletionSerialPKTest : public DBTest {
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb-serial/");
    }
};

TEST_F(DeleteNodeWithEdgesErrorTest, DeleteNodeWithEdgesError) {
    auto conn = std::make_unique<Connection>(database.get());
    ASSERT_TRUE(conn->query("create node table person (ID INT64, primary key(ID));")->isSuccess());
    ASSERT_TRUE(conn->query("create rel table isFriend (from person to person);")->isSuccess());
    ASSERT_TRUE(conn->query("create (p:person {ID: 5})")->isSuccess());
    ASSERT_TRUE(
        conn->query("match (p0:person), (p1:person) create (p0)-[:isFriend]->(p1)")->isSuccess());
    auto result = conn->query("match (p:person) delete p");
    ASSERT_EQ(result->getErrorMessage(),
        "Runtime exception: Currently deleting a node with edges is not supported. node table 0 "
        "nodeOffset 0 has 1 (one-to-many or many-to-many) edges.");
}

TEST_F(CreateDeleteInt64NodeTrxTest, MixedInsertDeleteCommitNormalExecution) {
    testMixedDeleteAndInsert(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateDeleteInt64NodeTrxTest, MixedInsertDeleteCommitRecovery) {
    testMixedDeleteAndInsert(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateDeleteInt64NodeTrxTest, MixedInsertDeleteRollbackNormalExecution) {
    testMixedDeleteAndInsert(false /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateDeleteInt64NodeTrxTest, MixedInsertDeleteRollbackRecovery) {
    testMixedDeleteAndInsert(false /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateDeleteInt64NodeTrxTest, IndexScanAfterInsertionCommitNormalExecution) {
    testIndexScanAfterInsertion(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateDeleteInt64NodeTrxTest, IndexScanAfterInsertionCommitRecovery) {
    testIndexScanAfterInsertion(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateDeleteInt64NodeTrxTest, IndexScanAfterInsertionRollbackNormalExecution) {
    testIndexScanAfterInsertion(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateDeleteInt64NodeTrxTest, IndexScanAfterInsertionRollbackRecovery) {
    testIndexScanAfterInsertion(false /* rollback */, TransactionTestType::RECOVERY);
}

TEST_F(CreateDeleteInt64NodeTrxTest, IndexScanAfterDeletionCommitNormalExecution) {
    testIndexScanAfterDeletion(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateDeleteInt64NodeTrxTest, IndexScanAfterDeletionRollbackNormalExecution) {
    testIndexScanAfterDeletion(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateDeleteInt64NodeTrxTest, IndexScanAfterDeletionCommitRecovery) {
    testIndexScanAfterDeletion(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateDeleteInt64NodeTrxTest, IndexScanAfterDeletionRollbackRecovery) {
    testIndexScanAfterDeletion(false /* rollback */, TransactionTestType::RECOVERY);
}

TEST_F(CreateDeleteInt64NodeTrxTest, DeleteAllNodesCommitNormalExecution) {
    testDeleteAllNodes(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateDeleteInt64NodeTrxTest, DeleteAllNodesCommitRecovery) {
    testDeleteAllNodes(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateDeleteInt64NodeTrxTest, DeleteAllNodesRollbackNormalExecution) {
    testDeleteAllNodes(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateDeleteInt64NodeTrxTest, DeleteAllNodesRollbackRecovery) {
    testDeleteAllNodes(false /* rollback */, TransactionTestType::RECOVERY);
}

TEST_F(CreateDeleteInt64NodeTrxTest, SimpleAddCommitNormalExecution) {
    testSimpleInsertions(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateDeleteInt64NodeTrxTest, SimpleAddCommitRecovery) {
    testSimpleInsertions(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateDeleteInt64NodeTrxTest, SimpleAddRollbackNormalExecution) {
    testSimpleInsertions(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateDeleteInt64NodeTrxTest, SimpleAddRollbackRecovery) {
    testSimpleInsertions(false /* rollback */, TransactionTestType::RECOVERY);
}

TEST_F(CreateDeleteStringNodeTrxTest, IndexScanAfterInsertionCommitNormalExecution) {
    testIndexScanAfterInsertion(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateDeleteStringNodeTrxTest, IndexScanAfterInsertionCommitRecovery) {
    testIndexScanAfterInsertion(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateDeleteStringNodeTrxTest, IndexScanAfterInsertionRollbackNormalExecution) {
    testIndexScanAfterInsertion(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateDeleteStringNodeTrxTest, IndexScanAfterInsertionRollbackRecovery) {
    testIndexScanAfterInsertion(false /* rollback */, TransactionTestType::RECOVERY);
}

TEST_F(CreateDeleteStringNodeTrxTest, IndexScanAfterDeletionCommitNormalExecution) {
    testIndexScanAfterDeletion(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateDeleteStringNodeTrxTest, IndexScanAfterDeletionCommitRecovery) {
    testIndexScanAfterDeletion(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateDeleteStringNodeTrxTest, ScanAfterDeletionRollbackNormalExecution) {
    testIndexScanAfterDeletion(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateDeleteStringNodeTrxTest, ScanAfterDeletionRollbackRecovery) {
    testIndexScanAfterDeletion(false /* rollback */, TransactionTestType::RECOVERY);
}

TEST_F(CreateDeleteStringNodeTrxTest, DeleteAllNodesCommitNormalExecution) {
    testDeleteAllNodes(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateDeleteStringNodeTrxTest, DeleteAllNodesCommitRecovery) {
    testDeleteAllNodes(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateDeleteStringNodeTrxTest, DeleteAllNodesRollbackNormalExecution) {
    testDeleteAllNodes(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateDeleteStringNodeTrxTest, DeleteAllNodesRollbackRecovery) {
    testDeleteAllNodes(false /* rollback */, TransactionTestType::RECOVERY);
}

TEST_F(CreateDeleteStringNodeTrxTest, SimpleAddCommitNormalExecution) {
    testSimpleInsertions(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateDeleteStringNodeTrxTest, SimpleAddCommitRecovery) {
    testSimpleInsertions(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateDeleteStringNodeTrxTest, SimpleAddRollbackNormalExecution) {
    testSimpleInsertions(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateDeleteStringNodeTrxTest, SimpleAddRollbackRecovery) {
    testSimpleInsertions(false /* rollback */, TransactionTestType::RECOVERY);
}

TEST_F(CreateDeleteStringNodeTrxTest, MixedInsertDeleteCommitNormalExecution) {
    testMixedDeleteAndInsert(true /* commit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateDeleteStringNodeTrxTest, MixedInsertDeleteCommitRecovery) {
    testMixedDeleteAndInsert(true /* commit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateDeleteStringNodeTrxTest, MixedInsertDeleteRollbackNormalExecution) {
    testMixedDeleteAndInsert(false /* rollback */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateDeleteStringNodeTrxTest, MixedInsertDeleteRollbackRecovery) {
    testMixedDeleteAndInsert(false /* rollback */, TransactionTestType::RECOVERY);
}

TEST_F(NodeInsertionDeletionSerialPKTest, NodeInsertionDeletionWithSerial) {
    // Firstly, we insert two nodes with serial as primary key to movie table.
    ASSERT_TRUE(conn->query("CREATE(m : movies {length: 32})")->isSuccess());
    ASSERT_TRUE(conn->query("CREATE(m : movies {note: 'the movie is very boring'})")->isSuccess());
    auto actualResult = TestHelper::convertResultToString(
        *conn->query("match (m:movies) return m.ID, m.length, m.note"));
    auto expectedResult = std::vector<std::string>{"0|126| this is a very very good movie",
        "1|2544| the movie is very very good", "2|298|the movie is very interesting and funny",
        "3|32|", "4||the movie is very boring"};
    ASSERT_EQ(actualResult, expectedResult);
    // Then we delete node0 and node3.
    ASSERT_TRUE(conn->query("MATCH (m:movies) WHERE m.length = 32 or m.length = 126 DELETE m")
                    ->isSuccess());
    actualResult = TestHelper::convertResultToString(
        *conn->query("match (m:movies) return m.ID, m.length, m.note"));
    expectedResult = std::vector<std::string>{"1|2544| the movie is very very good",
        "2|298|the movie is very interesting and funny", "4||the movie is very boring"};
    ASSERT_EQ(actualResult, expectedResult);
    // Then we insert a new node with serial as primary key to movie table.
    ASSERT_TRUE(conn->query("CREATE(m : movies {length: 188})")->isSuccess());
    actualResult = TestHelper::convertResultToString(
        *conn->query("match (m:movies) return m.ID, m.length, m.note"));
    expectedResult = std::vector<std::string>{
        "1|2544| the movie is very very good",
        "2|298|the movie is very interesting and funny",
        "3|188|",
        "4||the movie is very boring",
    };
    ASSERT_EQ(actualResult, expectedResult);
}

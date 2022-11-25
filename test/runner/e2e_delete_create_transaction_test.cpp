#include <iostream>

#include "test_helper/test_helper.h"

using namespace kuzu::testing;

class BaseDeleteCreateTrxTest : public DBTest {
public:
    void SetUp() override {
        DBTest::SetUp();
        readConn = make_unique<Connection>(database.get());
    }

    void initDBAndConnection() {
        createDBAndConn();
        readConn = make_unique<Connection>(database.get());
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
    void createNodes(const vector<string>& nodeIDs) {
        for (auto& nodeID : nodeIDs) {
            createNode(nodeID);
        }
    }
    void createNodes(uint64_t numNodes, bool isStringPK) {
        for (auto i = 0u; i < numNodes; ++i) {
            auto key = 10000 + i;
            if (isStringPK) {
                createNode("'" + to_string(key) + "'");
            } else {
                createNode(to_string(key));
            }
        }
    }
    void createNode(const string& nodeID) {
        auto query = "CREATE (a:person {ID: " + nodeID + "})";
        ASSERT_TRUE(conn->query(query)->isSuccess());
    }
    // Delete nodes
    void deleteNodesInRange(uint64_t startOffset, uint64_t endOffset, bool isStringPK) {
        auto query = "MATCH (a:person) WHERE a.ID>=" +
                     (isStringPK ? "'" + to_string(startOffset) + "'" : to_string(startOffset)) +
                     " AND a.ID<" +
                     (isStringPK ? "'" + to_string(startOffset) + "'" : to_string(startOffset)) +
                     " DELETE a";
        ASSERT_TRUE(conn->query(query)->isSuccess());
    }
    void deleteNodes(const vector<string>& nodeIDs) {
        for (auto& nodeID : nodeIDs) {
            deleteNode(nodeID);
        }
    }
    void deleteNode(const string& nodeID) {
        auto query = "MATCH (a:person) WHERE a.ID=" + nodeID + " DELETE a";
        ASSERT_TRUE(conn->query(query)->isSuccess());
    }
    static int64_t getCount(Connection* connection, const string& queryPrefix) {
        auto result = connection->query(queryPrefix + " RETURN COUNT(*)");
        return result->getNext()->getResultValue(0)->getInt64Val();
    }
    int64_t getNumNodes(Connection* connection) { return getCount(connection, "MATCH (:person)"); }
    void validateNodesExistOrNot(
        Connection* connection, const vector<string>& nodeIDs, bool exist) {
        for (auto& nodeID : nodeIDs) {
            validateNodeExistOrNot(connection, nodeID, exist);
        }
    }
    // Will trigger index scan.
    void validateNodeExistOrNot(Connection* connection, const string& nodeID, bool exist) {
        auto query = "MATCH (a:person) WHERE a.ID=" + nodeID;
        ASSERT_EQ(getCount(connection, query), exist ? 1 : 0);
    }

public:
    unique_ptr<Connection> readConn;
};

class CreateDeleteInt64NodeTrxTest : public BaseDeleteCreateTrxTest {
public:
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/node-insertion-deletion-tests/int64-pk/");
    }

    void testIndexScanAfterInsertion(bool isCommit, TransactionTestType trxTestType) {
        auto nodeIDsToInsert = vector<string>{"10003", "10005"};
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
        auto nodeIDsToDelete = vector<string>{"10", "1400", "6000"};
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
        deleteNodes(vector<string>{"8000", "9000"});
        createNodes(vector<string>{"8000", "9000", "10001", "10002"});
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
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/node-insertion-deletion-tests/string-pk/");
    }

    void testIndexScanAfterInsertion(bool isCommit, TransactionTestType trxTestType) {
        auto nodeIDsToInsert = vector<string>{"'abcdefg'", "'huy23b287sfw33232'"};
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
            vector<string>{"'999999999999'", "'1000000000000'", "'1000000000001'"};
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
        deleteNodes(vector<string>{"'999999995061'", "'1000000004126'"});
        createNodes(vector<string>{
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

// TODO(Guodong/Xiyang): refactor these tests to follow the above convention.
class CreateRelTrxTest : public BaseDeleteCreateTrxTest {
public:
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/rel-insertion-tests/");
    }

    void insertKnowsRels(const string& srcLabel, const string& dstLabel, uint64_t numRelsToInsert,
        bool testNullAndLongString = false) {
        for (auto i = 0u; i < numRelsToInsert; ++i) {
            auto length = to_string(i);
            auto place = to_string(i);
            auto tag = to_string(i);
            if (testNullAndLongString) {
                length = "null";
                place = getLongStr(place);
            }
            insertKnowsRel(
                srcLabel, dstLabel, "1", to_string(i + 1), length, place, "['" + tag + "']");
        }
    }

    void insertKnowsRel(const string& srcLabel, const string& dstLabel, const string& srcID,
        const string& dstID, const string& length, const string& place, const string& tag) {
        auto matchClause = "MATCH (a:" + srcLabel + "),(b:" + dstLabel + ") WHERE a.ID=" + srcID +
                           " AND b.ID=" + dstID + " ";
        auto createClause = "CREATE (a)-[e:knows {length:" + length + ",place:'" + place +
                            "',tag:" + tag + "}]->(b)";
        string query = matchClause + createClause;
        assert(conn->query(query));
    }

    vector<string> readAllKnowsProperty(Connection* connection, const string& srcLabel,
        const string& dstLabel, bool validateBwdOrder = false) {
        auto result = connection->query("MATCH (a:" + srcLabel + ")-[e:knows]->(b:" + dstLabel +
                                        ") RETURN e.length, e.place, e.tag");
        return TestHelper::convertResultToString(*result, true /* checkOrder, do not sort */);
    }

    static string getLongStr(const string& val) { return "long long string prefix " + val; }

    void validateExceptionMessage(string query, string expectedException) {
        conn->query(query);
        auto result = conn->query(query);
        ASSERT_FALSE(result->isSuccess());
        ASSERT_STREQ(result->getErrorMessage().c_str(), expectedException.c_str());
    }

public:
    static const int64_t smallNumRelsToInsert = 10;
    // For a relTable with simple relation(src,dst nodeIDs are 1): we need to insert at least
    // PAGE_SIZE / 8 number of rels to make a smallList become largeList.
    // For a relTable with complex relation(src,dst nodeIDs > 1): we need to insert at least
    // PAGE_SIZE / 16 number of rels to make a smallList become largeList.
    static const int64_t largeNumRelsToInsert = 510;
};

static void validateInsertedRel(
    const string& result, uint64_t param, bool testNullAndLongString = false) {
    auto length = to_string(param);
    auto place = to_string(param);
    if (testNullAndLongString) {
        length = ""; // null
        place = CreateRelTrxTest::getLongStr(place);
    }
    auto groundTruth = length + "|" + place + "|[" + to_string(param) + "]";
    ASSERT_STREQ(groundTruth.c_str(), result.c_str());
}

static void validateInsertRelsToEmptyListSucceed(
    vector<string> resultStr, uint64_t numRelsInserted, bool testNullAndLongString = false) {
    for (auto i = 0u; i < numRelsInserted; i++) {
        validateInsertedRel(resultStr[i], i, testNullAndLongString);
    }
}

static void validateInsertRelsToEmptyListFail(const vector<string>& resultStr) {
    ASSERT_EQ(resultStr.size(), 0);
}

TEST_F(CreateRelTrxTest, InsertRelsToEmptyListCommitNormalExecution) {
    conn->beginWriteTransaction();
    insertKnowsRels("animal", "animal", smallNumRelsToInsert);
    validateInsertRelsToEmptyListSucceed(
        readAllKnowsProperty(conn.get(), "animal", "animal"), smallNumRelsToInsert);
    validateInsertRelsToEmptyListFail(readAllKnowsProperty(readConn.get(), "animal", "animal"));
    conn->commit();
    validateInsertRelsToEmptyListSucceed(
        readAllKnowsProperty(conn.get(), "animal", "animal"), smallNumRelsToInsert);
}

TEST_F(CreateRelTrxTest, InsertRelsToEmptyListCommitRecovery) {
    conn->beginWriteTransaction();
    insertKnowsRels("animal", "animal", smallNumRelsToInsert);
    commitButSkipCheckpointingForTestingRecovery(*conn);
    createDBAndConn(); // run recovery
    validateInsertRelsToEmptyListSucceed(
        readAllKnowsProperty(conn.get(), "animal", "animal"), smallNumRelsToInsert);
}

TEST_F(CreateRelTrxTest, InsertRelsToEmptyListRollbackNormalExecution) {
    conn->beginWriteTransaction();
    insertKnowsRels("animal", "animal", smallNumRelsToInsert);
    conn->rollback();
    validateInsertRelsToEmptyListFail(readAllKnowsProperty(conn.get(), "animal", "animal"));
}

TEST_F(CreateRelTrxTest, InsertRelsToEmptyListRollbackRecovery) {
    conn->beginWriteTransaction();
    insertKnowsRels("animal", "animal", smallNumRelsToInsert);
    rollbackButSkipCheckpointingForTestingRecovery(*conn);
    createDBAndConn(); // run recovery
    validateInsertRelsToEmptyListFail(readAllKnowsProperty(conn.get(), "animal", "animal"));
}

TEST_F(CreateRelTrxTest, InsertManyRelsToEmptyListWithNullAndLongStrCommitNormalExecution) {
    conn->beginWriteTransaction();
    insertKnowsRels("animal", "animal", largeNumRelsToInsert, true /* testNullAndLongString */);
    validateInsertRelsToEmptyListSucceed(readAllKnowsProperty(conn.get(), "animal", "animal"),
        largeNumRelsToInsert, true /* testNullAndLongString */);
    validateInsertRelsToEmptyListFail(readAllKnowsProperty(readConn.get(), "animal", "animal"));
    conn->commit();
    validateInsertRelsToEmptyListSucceed(readAllKnowsProperty(conn.get(), "animal", "animal"),
        largeNumRelsToInsert, true /* testNullAndLongString */);
}

TEST_F(CreateRelTrxTest, InsertManyRelsToEmptyListWithNullAndLongStrRollbackNormalExecution) {
    conn->beginWriteTransaction();
    insertKnowsRels("animal", "animal", largeNumRelsToInsert, true /* testNullAndLongString */);
    conn->rollback();
    validateInsertRelsToEmptyListFail(readAllKnowsProperty(conn.get(), "animal", "animal"));
}

TEST_F(CreateRelTrxTest, InsertManyRelsToEmptyListWithNullAndLongStrRollbackRecovery) {
    conn->beginWriteTransaction();
    insertKnowsRels("animal", "animal", largeNumRelsToInsert, true /* testNullAndLongString */);
    rollbackButSkipCheckpointingForTestingRecovery(*conn);
    createDBAndConn(); // run recovery
    validateInsertRelsToEmptyListFail(readAllKnowsProperty(conn.get(), "animal", "animal"));
}

static int64_t numAnimalPersonRelsInDB = 51;

static string generateStrFromInt(uint64_t val) {
    string result = to_string(val);
    if (val % 2 == 0) {
        for (auto i = 0; i < 2; i++) {
            result.append(to_string(val));
        }
    }
    return result;
}

static void validateExistingAnimalPersonRel(const string& result, uint64_t param) {
    auto strVal = generateStrFromInt(1000 - param);
    auto groundTruth = to_string(param) + "|" + strVal + "|[" + strVal + "]";
    ASSERT_STREQ(groundTruth.c_str(), result.c_str());
}

static void validateInsertRelsToSmallListSucceed(
    vector<string> resultStr, uint64_t numRelsInserted) {
    auto currIdx = 0u;
    auto insertedRelsOffset = 2;
    for (auto i = 0u; i < insertedRelsOffset; ++i, ++currIdx) {
        validateExistingAnimalPersonRel(resultStr[currIdx], i);
    }
    for (auto i = 0u; i < numRelsInserted; ++i, ++currIdx) {
        validateInsertedRel(resultStr[currIdx], i);
    }
    for (auto i = 0u; i < numAnimalPersonRelsInDB - insertedRelsOffset; ++i, ++currIdx) {
        validateExistingAnimalPersonRel(resultStr[currIdx], i + insertedRelsOffset);
    }
}

static void validateInsertRelsToSmallListFail(vector<string> resultStr) {
    ASSERT_EQ(resultStr.size(), numAnimalPersonRelsInDB);
    validateInsertRelsToSmallListSucceed(resultStr, 0);
}

TEST_F(CreateRelTrxTest, InsertRelsToSmallListCommitNormalExecution) {
    conn->beginWriteTransaction();
    insertKnowsRels("animal", "person", smallNumRelsToInsert);
    validateInsertRelsToSmallListSucceed(
        readAllKnowsProperty(conn.get(), "animal", "person"), smallNumRelsToInsert);
    validateInsertRelsToSmallListFail(readAllKnowsProperty(readConn.get(), "animal", "person"));
    conn->commit();
    validateInsertRelsToSmallListSucceed(
        readAllKnowsProperty(conn.get(), "animal", "person"), smallNumRelsToInsert);
}

TEST_F(CreateRelTrxTest, InsertRelsToSmallListCommitRecovery) {
    conn->beginWriteTransaction();
    insertKnowsRels("animal", "person", smallNumRelsToInsert);
    commitButSkipCheckpointingForTestingRecovery(*conn);
    createDBAndConn(); // run recovery
    validateInsertRelsToSmallListSucceed(
        readAllKnowsProperty(conn.get(), "animal", "person"), smallNumRelsToInsert);
}

TEST_F(CreateRelTrxTest, InsertRelsToSmallListRollbackNormalExecution) {
    conn->beginWriteTransaction();
    insertKnowsRels("animal", "person", smallNumRelsToInsert);
    conn->rollback();
    validateInsertRelsToSmallListFail(readAllKnowsProperty(conn.get(), "animal", "person"));
}

TEST_F(CreateRelTrxTest, InsertRelsToSmallListRollbackRecovery) {
    conn->beginWriteTransaction();
    insertKnowsRels("animal", "person", smallNumRelsToInsert);
    rollbackButSkipCheckpointingForTestingRecovery(*conn);
    createDBAndConn(); // run recovery
    validateInsertRelsToSmallListFail(readAllKnowsProperty(conn.get(), "animal", "person"));
}

TEST_F(CreateRelTrxTest, InsertLargeNumRelsToSmallListCommitNormalExecution) {
    conn->beginWriteTransaction();
    insertKnowsRels("animal", "person", largeNumRelsToInsert);
    validateInsertRelsToSmallListSucceed(
        readAllKnowsProperty(conn.get(), "animal", "person"), largeNumRelsToInsert);
    validateInsertRelsToSmallListFail(readAllKnowsProperty(readConn.get(), "animal", "person"));
    conn->commit();
    validateInsertRelsToSmallListSucceed(
        readAllKnowsProperty(conn.get(), "animal", "person"), largeNumRelsToInsert);
}

TEST_F(CreateRelTrxTest, InsertLargeNumRelsToSmallListCommitRecovery) {
    conn->beginWriteTransaction();
    insertKnowsRels("animal", "person", largeNumRelsToInsert);
    commitButSkipCheckpointingForTestingRecovery(*conn);
    createDBAndConn(); // run recovery
    validateInsertRelsToSmallListSucceed(
        readAllKnowsProperty(conn.get(), "animal", "person"), largeNumRelsToInsert);
}

static int numPersonPersonRelsInDB = 2500;

static void validateExistingPersonPersonRel(const string& result, uint64_t param) {
    auto strVal = generateStrFromInt(3000 - param);
    auto groundTruth = to_string(3 * param) + "|" + strVal + "|[" + strVal + "]";
    ASSERT_STREQ(groundTruth.c_str(), result.c_str());
}

static void validateInsertRelsToLargeListSucceed(
    vector<string> resultStr, uint64_t numRelsInserted, bool testNullAndLongString = false) {
    auto currIdx = 0u;
    for (auto i = 0u; i < numPersonPersonRelsInDB; ++i, ++currIdx) {
        validateExistingPersonPersonRel(resultStr[currIdx], i);
    }
    for (auto i = 0u; i < numRelsInserted; ++i, ++currIdx) {
        validateInsertedRel(resultStr[currIdx], i, testNullAndLongString);
    }
}

static void validateInsertRelsToLargeListFail(vector<string> resultStr) {
    ASSERT_EQ(resultStr.size(), numPersonPersonRelsInDB);
    validateInsertRelsToLargeListSucceed(resultStr, 0);
}

TEST_F(CreateRelTrxTest, InsertRelsToLargeListCommitNormalExecution) {
    conn->beginWriteTransaction();
    insertKnowsRels("person", "person", smallNumRelsToInsert);
    validateInsertRelsToLargeListSucceed(
        readAllKnowsProperty(conn.get(), "person", "person"), smallNumRelsToInsert);
    validateInsertRelsToLargeListFail(readAllKnowsProperty(readConn.get(), "person", "person"));
    conn->commit();
    validateInsertRelsToLargeListSucceed(
        readAllKnowsProperty(conn.get(), "person", "person"), smallNumRelsToInsert);
}

TEST_F(CreateRelTrxTest, InsertRelsToLargeListCommitRecovery) {
    conn->beginWriteTransaction();
    insertKnowsRels("person", "person", smallNumRelsToInsert);
    commitButSkipCheckpointingForTestingRecovery(*conn);
    createDBAndConn(); // run recovery
    validateInsertRelsToLargeListSucceed(
        readAllKnowsProperty(conn.get(), "person", "person"), smallNumRelsToInsert);
}

TEST_F(CreateRelTrxTest, InsertRelsToLargeListRollbackNormalExecution) {
    conn->beginWriteTransaction();
    insertKnowsRels("person", "person", smallNumRelsToInsert);
    conn->rollback();
    validateInsertRelsToLargeListFail(readAllKnowsProperty(conn.get(), "person", "person"));
}

TEST_F(CreateRelTrxTest, InsertRelsToLargeListRollbackRecovery) {
    conn->beginWriteTransaction();
    insertKnowsRels("person", "person", smallNumRelsToInsert);
    rollbackButSkipCheckpointingForTestingRecovery(*conn);
    createDBAndConn(); // run recovery
    validateInsertRelsToLargeListFail(readAllKnowsProperty(conn.get(), "person", "person"));
}

TEST_F(CreateRelTrxTest, InsertRelsToLargeListWithNullCommitNormalExecution) {
    conn->beginWriteTransaction();
    insertKnowsRels("person", "person", smallNumRelsToInsert, true /* testNullAndLongString */);
    validateInsertRelsToLargeListSucceed(readAllKnowsProperty(conn.get(), "person", "person"),
        smallNumRelsToInsert, true /* testNullAndLongString */);
    validateInsertRelsToLargeListFail(readAllKnowsProperty(readConn.get(), "person", "person"));
    conn->commit();
    validateInsertRelsToLargeListSucceed(readAllKnowsProperty(conn.get(), "person", "person"),
        smallNumRelsToInsert, true /* testNullAndLongString */);
}

TEST_F(CreateRelTrxTest, ViolateManyOneMultiplicityError) {
    conn->beginWriteTransaction();
    validateExceptionMessage(
        "MATCH (p1:person), (p2:person) WHERE p1.ID = 10 AND p2.ID = 10 CREATE "
        "(p1)-[:teaches]->(p2);",
        "Runtime exception: RelTable 5 is a MANY_ONE table, but node(nodeOffset: 10, tableID: 1) "
        "has more than one neighbour in the forward direction.");
}

TEST_F(CreateRelTrxTest, ViolateOneOneMultiplicityError) {
    conn->beginWriteTransaction();
    validateExceptionMessage("MATCH (a:animal), (p:person) WHERE a.ID = 10 AND p.ID = 10 CREATE "
                             "(a)-[:hasOwner]->(p);",
        "Runtime exception: RelTable 4 is a ONE_ONE table, but node(nodeOffset: 10, tableID: 0) "
        "has more than one neighbour in the forward direction.");
}

TEST_F(CreateRelTrxTest, CreateRelToEmptyRelTable) {
    conn->query("create node table student(id INT64, PRIMARY KEY(id))");
    conn->query("create rel table follows(FROM student TO student)");
    conn->query("CREATE (s:student {id: 1})");
    ASSERT_EQ(TestHelper::convertResultToString(
                  *conn->query("match (s:student)-[:follows]->(:student) return count(s.id)"))[0],
        to_string(0));
}

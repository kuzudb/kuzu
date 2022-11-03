#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

class BaseDeleteCreateTrxTest : public DBTest {
public:
    void SetUp() override {
        DBTest::SetUp();
        readConn = make_unique<Connection>(database.get());
    }

public:
    unique_ptr<Connection> readConn;
};

class CreateDeleteNodeTrxTest : public BaseDeleteCreateTrxTest {
public:
    string getInputCSVDir() override { return "dataset/node-insertion-deletion-tests/"; }

    void addNodes(uint64_t numNodes) {
        for (auto i = 0u; i < numNodes; ++i) {
            auto id = 10000 + i;
            ASSERT_TRUE(conn->query("CREATE (a:person {ID: " + to_string(id) + "})")->isSuccess());
        }
    }

    void deleteNodesInRange(uint64_t startOffset, uint64_t endOffset) {
        auto query = "MATCH (a:person) WHERE a.ID>=" + to_string(startOffset) + " AND a.ID<" +
                     to_string(endOffset) + " DELETE a";
        ASSERT_TRUE(conn->query(query)->isSuccess());
    }

    void deleteNodes(vector<uint64_t> nodeIDs) {
        for (auto& nodeID : nodeIDs) {
            deleteNode(nodeID);
        }
    }

    void deleteNode(uint64_t nodeID) {
        auto query = "MATCH (a:person) WHERE a.ID=" + to_string(nodeID) + " DELETE a";
        ASSERT_TRUE(conn->query(query)->isSuccess());
    }

    int64_t getCount(Connection* connection, const string& query) {
        auto result = connection->query(query + " RETURN COUNT(*)");
        return result->getNext()->getResultValue(0)->getInt64Val();
    }

    int64_t getNumNodes(Connection* connection) { return getCount(connection, "MATCH (:person)"); }

    void validateNodesExistOrNot(Connection* connection, vector<uint64_t> nodeIDs, bool isExist) {
        for (auto& nodeID : nodeIDs) {
            validateNodeExistOrNot(connection, nodeID, isExist);
        }
    }

    void validateNodeExistOrNot(Connection* connection, uint64_t nodeID, bool isExist) {
        auto query = "MATCH (a:person) WHERE a.ID=" + to_string(nodeID);
        ASSERT_EQ(getCount(connection, query), isExist ? 1 : 0);
    }
};

// TODO(Guogong): The following test use index scan after node insertion.
//  Uncomment once create support hash index.
// TEST_F(CreateDeleteNodeTrxTest, ScanAfterDeletionCommitNormalExecution) {
//    auto nodeIDsToDelete = vector<uint64_t>{10, 1400, 6000};
//    conn->beginWriteTransaction();
//    deleteNodes(nodeIDsToDelete);
//    validateNodesExistOrNot(conn.get(), nodeIDsToDelete, false /* isExist*/);
//    validateNodesExistOrNot(readConn.get(), nodeIDsToDelete, true /* isExist*/);
//    conn->commit();
//    validateNodesExistOrNot(conn.get(), nodeIDsToDelete, false /* isExist*/);
//}

TEST_F(CreateDeleteNodeTrxTest, ScanAfterDeletionRollbackNormalExecution) {
    auto nodeIDsToDelete = vector<uint64_t>{10, 1400, 6000};
    conn->beginWriteTransaction();
    deleteNodes(nodeIDsToDelete);
    conn->rollback();
    validateNodesExistOrNot(conn.get(), nodeIDsToDelete, true /* isExist*/);
}

TEST_F(CreateDeleteNodeTrxTest, DeleteEntireMorselCommitNormalExecution) {
    conn->beginWriteTransaction();
    deleteNodesInRange(2048, 4096);
    auto query = "MATCH (a:person) WHERE a.ID < 4096 ";
    ASSERT_EQ(getCount(conn.get(), query), 2048);
    ASSERT_EQ(getCount(readConn.get(), query), 4096);
    conn->commit();
    ASSERT_EQ(getCount(conn.get(), query), 2048);
}

TEST_F(CreateDeleteNodeTrxTest, DeleteAllNodesCommitNormalExecution) {
    conn->beginWriteTransaction();
    conn->query("MATCH (a:person) DELETE a");
    ASSERT_EQ(getNumNodes(conn.get()), 0);
    ASSERT_EQ(getNumNodes(readConn.get()), 10000);
    conn->commit();
    ASSERT_EQ(getNumNodes(conn.get()), 0);
}

TEST_F(CreateDeleteNodeTrxTest, DeleteAllNodesCommitRecovery) {
    conn->beginWriteTransaction();
    conn->query("MATCH (a:person) DELETE a");
    conn->commitButSkipCheckpointingForTestingRecovery();
    createDBAndConn(); // run recovery
    ASSERT_EQ(getNumNodes(conn.get()), 0);
}

TEST_F(CreateDeleteNodeTrxTest, DeleteAllNodesRollbackNormalExecution) {
    conn->beginWriteTransaction();
    conn->query("MATCH (a:person) DELETE a");
    conn->rollback();
    ASSERT_EQ(getNumNodes(conn.get()), 10000);
}

TEST_F(CreateDeleteNodeTrxTest, DeleteAllNodesRollbackRecovery) {
    conn->beginWriteTransaction();
    conn->query("MATCH (a:person) DELETE a");
    conn->rollbackButSkipCheckpointingForTestingRecovery();
    createDBAndConn(); // run recovery
    ASSERT_EQ(getNumNodes(conn.get()), 10000);
}

// TODO(Guodong): We need to extend these tests with queries that verify that the IDs are deleted,
// added, and can be queried correctly.
TEST_F(CreateDeleteNodeTrxTest, SimpleAddCommitNormalExecution) {
    conn->beginWriteTransaction();
    addNodes(1000);
    ASSERT_EQ(getNumNodes(conn.get()), 11000);
    ASSERT_EQ(getNumNodes(readConn.get()), 10000);
    conn->commit();
    ASSERT_EQ(getNumNodes(conn.get()), 11000);
}

TEST_F(CreateDeleteNodeTrxTest, SimpleAddCommitRecovery) {
    conn->beginWriteTransaction();
    addNodes(1000);
    conn->commitButSkipCheckpointingForTestingRecovery();
    createDBAndConn(); // run recovery
    ASSERT_EQ(getNumNodes(conn.get()), 11000);
}

TEST_F(CreateDeleteNodeTrxTest, SimpleAddRollbackNormalExecution) {
    conn->beginWriteTransaction();
    addNodes(1000);
    conn->rollback();
    ASSERT_EQ(getNumNodes(conn.get()), 10000);
}

TEST_F(CreateDeleteNodeTrxTest, SimpleAddRollbackRecovery) {
    conn->beginWriteTransaction();
    addNodes(1000);
    conn->rollbackButSkipCheckpointingForTestingRecovery();
    createDBAndConn(); // run recovery
    ASSERT_EQ(getNumNodes(conn.get()), 10000);
}

class CreateRelTrxTest : public BaseDeleteCreateTrxTest {
public:
    string getInputCSVDir() override { return "dataset/rel-insertion-tests/"; }

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
        assert(conn->query(matchClause + createClause));
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

static void validateInsertRelsToEmptyListFail(vector<string> resultStr) {
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
    conn->commitButSkipCheckpointingForTestingRecovery();
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
    conn->rollbackButSkipCheckpointingForTestingRecovery();
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
    conn->rollbackButSkipCheckpointingForTestingRecovery();
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
    conn->commitButSkipCheckpointingForTestingRecovery();
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
    conn->rollbackButSkipCheckpointingForTestingRecovery();
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
    conn->commitButSkipCheckpointingForTestingRecovery();
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
    conn->commitButSkipCheckpointingForTestingRecovery();
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
    conn->rollbackButSkipCheckpointingForTestingRecovery();
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

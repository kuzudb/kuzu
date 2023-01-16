#include "graph_test/graph_test.h"

using namespace kuzu::testing;

class DeleteRelTest : public DBTest {
public:
    string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/rel-insertion-tests/");
    }
    string getDeleteKnowsRelQuery(string srcTable, string dstTable, int64_t srcID, int64_t dstID) {
        return StringUtils::string_format(
            "MATCH (p1:%s)-[e:knows]->(p2:%s) WHERE p1.ID = %d AND p2.ID = %d delete e;",
            srcTable.c_str(), dstTable.c_str(), srcID, dstID);
    }
    string getInsertKnowsRelQuery(string srcTable, string dstTable, int64_t srcID, int64_t dstID) {
        return StringUtils::string_format("MATCH (p1:%s), (p2:%s) WHERE p1.ID = %d AND p2.ID "
                                          "= %d create (p1)-[:knows {length: %d}]->(p2);",
            srcTable.c_str(), dstTable.c_str(), srcID, dstID, srcID);
    }

    string getDeleteTeachesRelQuery(int64_t srcID, int64_t dstID) {
        return StringUtils::string_format(
            "MATCH (p1:person)-[t:teaches]->(p2:person) WHERE p1.ID = %d AND p2.ID = %d delete t;",
            srcID, dstID);
    }

    string getDeleteHasOwnerRelQuery(int64_t srcID, int64_t dstID) {
        return StringUtils::string_format(
            "MATCH (a:animal)-[h:hasOwner]->(p:person) WHERE a.ID = %d AND p.ID = %d delete h;",
            srcID, dstID);
    }

    void sortAndCheckTestResults(vector<string>& actualResult, vector<string>& expectedResult) {
        sort(expectedResult.begin(), expectedResult.end());
        ASSERT_EQ(actualResult, expectedResult);
    }

    void deleteRelsFromSmallList(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        // We delete all animal->person rels whose src nodeID offset is between 10-20 (inclusive);
        for (auto i = 10; i <= 20; i++) {
            ASSERT_TRUE(conn->query(getDeleteKnowsRelQuery("animal", "person", i, 0 /* dstID */))
                            ->isSuccess());
        }
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto actualResult = TestHelper::convertResultToString(
            *conn->query("MATCH (:animal)-[e:knows]->(:person) RETURN e.length"));
        vector<string> expectedResult;
        for (auto i = 0; i < NUM_RELS_FOR_ANIMAL_KNOWS_PERSON; i++) {
            // If we are not committing (e.g. rolling back), we expect to see all rels in the query
            // result. If we are committing, we should not see rels with length property
            // 10-20(inclusive) in the query result.
            if (!isCommit || (i < 10 || i > 20)) {
                expectedResult.push_back(to_string(i));
            }
        }
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void deleteAllRelsFromSmallList(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        ASSERT_TRUE(conn->query("MATCH (:animal)-[e:knows]->(:person) DELETE e")->isSuccess());
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        vector<string> expectedResult;
        if (!isCommit) {
            for (auto i = 0u; i <= 50; i++) {
                expectedResult.push_back(to_string(i));
            }
        }
        auto actualResult = TestHelper::convertResultToString(
            *conn->query("MATCH (:animal)-[e:knows]->(:person) RETURN e.length"));
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void deleteRelsFromLargeList(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        // We delete all person->person rels whose dst nodeID offset is between 100-200 (inclusive);
        for (auto i = 100; i <= 200; i++) {
            auto result = conn->query(getDeleteKnowsRelQuery("person", "person", 0 /* srcID */, i));
            ASSERT_TRUE(result->isSuccess());
        }
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result = conn->query("MATCH (p1:person)-[e:knows]->(p2:person) return e.length");
        auto actualResult = TestHelper::convertResultToString(*result);
        vector<string> expectedResult;
        for (auto i = 1; i <= NUM_RELS_FOR_PERSON_KNOWS_PERSON; i++) {
            // If we are not committing (e.g. rolling back), we are expected to see all rels in the
            // list. If we are committing, we are not expected to see the deleted rels whose dst
            // nodeID offset between 100-200 (inclusive).
            if (!isCommit || (i < 100 || i > 200)) {
                expectedResult.push_back(to_string(3 * (i - 1)));
            }
        }
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void deleteAllRelsFromLargeList(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        ASSERT_TRUE(conn->query("MATCH (:person)-[e:knows]->(:person) DELETE e")->isSuccess());
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        vector<string> expectedResult;
        if (!isCommit) {
            for (auto i = 1u; i <= 2500; i++) {
                expectedResult.push_back(to_string(3 * (i - 1)));
            }
        }
        auto actualResult = TestHelper::convertResultToString(
            *conn->query("MATCH (:person)-[e:knows]->(:person) RETURN e.length"));
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void deleteRelsFromUpdateStore(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        // We insert 3 rels: animal51->person0, animal52->person0, animal100->person10, and then
        // delete animal51->person0, animal100->person10.
        ASSERT_TRUE(
            conn->query(getInsertKnowsRelQuery("animal", "person", 51 /* srcID */, 0 /* dstID */))
                ->isSuccess());
        ASSERT_TRUE(
            conn->query(getInsertKnowsRelQuery("animal", "person", 52 /* srcID */, 0 /* dstID */))
                ->isSuccess());
        ASSERT_TRUE(
            conn->query(getInsertKnowsRelQuery("animal", "person", 100 /* srcID */, 10 /* dstID */))
                ->isSuccess());
        // Then we delete 2 rels: animal51->person0, animal100->person10.
        ASSERT_TRUE(
            conn->query(getDeleteKnowsRelQuery("animal", "person", 51 /* srcID */, 0 /* dstID */))
                ->isSuccess());
        ASSERT_TRUE(
            conn->query(getDeleteKnowsRelQuery("animal", "person", 100 /* srcID */, 10 /* dstID */))
                ->isSuccess());
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto actualResult = TestHelper::convertResultToString(
            *conn->query("MATCH (:animal)-[e:knows]->(:person) return e.length"));
        // After insertion and deletion, we are expected to see only one new rel: animal52->person0
        // (if we are committing).
        vector<string> expectedResult;
        for (auto i = 0; i < NUM_RELS_FOR_ANIMAL_KNOWS_PERSON; i++) {
            expectedResult.push_back(to_string(i));
        }
        if (isCommit) {
            expectedResult.push_back(to_string(52));
        }
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void deleteRelsAndQueryBWDList(bool isCommit, TransactionTestType transactionTestType) {
        auto query = "MATCH (a:person)-[e:knows]->(b:animal) RETURN e.length";
        // This check is to validate whether the query used in "deleteRelsAndQueryBWDList"
        // scans the backward list of the knows table. If this test fails (eg. we have updated the
        // planner), we should consider changing to a new query which scans the BWD list with
        // the new planner.
        validateQueryBestPlanJoinOrder(query, "HJ(a._id){E(a)S(b)}{S(a)}");
        conn->beginWriteTransaction();
        // We insert 10 rels: person[0..10]->animal0.
        for (auto i = 0u; i < 10; i++) {
            ASSERT_TRUE(conn->query(getInsertKnowsRelQuery("person", "animal", i, 0 /* dstID */))
                            ->isSuccess());
        }
        // Then we delete rels: person[2,4,6,8]->animal0.
        ASSERT_TRUE(
            conn->query(getDeleteKnowsRelQuery("person", "animal", 2, 0 /* dstID */))->isSuccess());
        ASSERT_TRUE(
            conn->query(getDeleteKnowsRelQuery("person", "animal", 4, 0 /* dstID */))->isSuccess());
        ASSERT_TRUE(
            conn->query(getDeleteKnowsRelQuery("person", "animal", 6, 0 /* dstID */))->isSuccess());
        ASSERT_TRUE(
            conn->query(getDeleteKnowsRelQuery("person", "animal", 8, 0 /* dstID */))->isSuccess());
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        vector<string> expectedResult =
            isCommit ? vector<string>{"0", "1", "3", "5", "7", "9"} : vector<string>{};
        auto actualResult = TestHelper::convertResultToString(*conn->query(query));
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void deleteMultipleRels(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        // Delete all rels from animal[10-49]->person0.
        ASSERT_TRUE(conn->query("MATCH (a:animal)-[e:knows]->(p:person) WHERE a.ID >= 10 AND "
                                "a.ID <= 49 AND p.ID = 0 DELETE e;")
                        ->isSuccess());
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        vector<string> expectedResult;
        if (isCommit) {
            expectedResult = vector<string>{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "50"};
        } else {
            for (auto i = 0u; i <= 50; i++) {
                expectedResult.push_back(to_string(i));
            }
        }
        auto actualResult = TestHelper::convertResultToString(
            *conn->query("MATCH (a:animal)-[e:knows]->(p:person) RETURN e.length"));
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void deleteAllInsertedRels(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        // Insert rels for animal[60-69]->person0.
        for (auto i = 60; i < 70; i++) {
            ASSERT_TRUE(conn->query(getInsertKnowsRelQuery("animal", "person", i, 0 /* dstID */))
                            ->isSuccess());
        }
        // Then delete those inserted rels.
        for (auto i = 60; i < 70; i++) {
            ASSERT_TRUE(conn->query(getDeleteKnowsRelQuery("animal", "person", i, 0 /* dstID */))
                            ->isSuccess());
        }
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        vector<string> expectedResult = {
            "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50"};
        auto actualResult = TestHelper::convertResultToString(*conn->query(
            "MATCH (a:animal)-[e:knows]->(p:person) WHERE a.ID >= 40 RETURN e.length"));
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void deleteLargeNumRelsFromLargeList(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        conn->query(
            "MATCH (p1:person)-[e:knows]->(p2:person) WHERE p1.ID = 0 AND p2.ID >= 10 delete e;");
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result = conn->query("MATCH (p1:person)-[e:knows]->(p2:person) return e.length");
        auto actualResult = TestHelper::convertResultToString(*result);
        vector<string> expectedResult;
        for (auto i = 1; i <= NUM_RELS_FOR_PERSON_KNOWS_PERSON; i++) {
            // If we are not committing (e.g. rolling back), we are expected to see all rels in the
            // list. If we are committing, we are not expected to see the deleted rels whose dst
            // nodeID offset >= 10.
            if (!isCommit || i < 10) {
                expectedResult.push_back(to_string(3 * (i - 1)));
            }
        }
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void deleteRelsFromManyToOneTable(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        conn->query(getDeleteTeachesRelQuery(11, 1));
        conn->query(getDeleteTeachesRelQuery(31, 3));
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result = conn->query("MATCH (:person)-[t:teaches]->(:person) return t.length");
        auto actualResult = TestHelper::convertResultToString(*result);
        vector<string> expectedResult = isCommit ?
                                            vector<string>{"21", "22", "32", "33"} :
                                            vector<string>{"11", "21", "22", "31", "32", "33"};
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void deleteRelsFromOneToOneTable(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        conn->query(getDeleteHasOwnerRelQuery(1, 51));
        conn->query(getDeleteHasOwnerRelQuery(3, 53));
        conn->query(getDeleteHasOwnerRelQuery(5, 55));
        conn->query(getDeleteHasOwnerRelQuery(7, 57));
        conn->query(getDeleteHasOwnerRelQuery(9, 59));
        conn->query(getDeleteHasOwnerRelQuery(11, 61));
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result = conn->query("MATCH (:animal)-[h:hasOwner]->(:person) return h.length");
        auto actualResult = TestHelper::convertResultToString(*result);
        vector<string> expectedResult =
            isCommit ? vector<string>{"2", "4", "6", "8"} :
                       vector<string>{"1", "2", "3", "4", "5", "6", "7", "8", "9", "11"};
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    static constexpr uint64_t NUM_RELS_FOR_PERSON_KNOWS_PERSON = 2500;
    static constexpr uint64_t NUM_RELS_FOR_ANIMAL_KNOWS_PERSON = 51;
};

TEST_F(DeleteRelTest, DeleteRelsFromSmallListCommitNormalExecution) {
    deleteRelsFromSmallList(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteRelsFromSmallListCommitRecovery) {
    deleteRelsFromSmallList(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteRelsFromSmallListRollbackNormalExecution) {
    deleteRelsFromSmallList(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteRelsFromSmallListRollbackRecovery) {
    deleteRelsFromSmallList(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteAllRelsFromSmallListCommitNormalExecution) {
    deleteAllRelsFromSmallList(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteAllRelsFromSmallListCommitRecovery) {
    deleteAllRelsFromSmallList(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteAllRelsFromSmallListRollbackNormalExecution) {
    deleteAllRelsFromSmallList(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteAllRelsFromSmallListRollbackRecovery) {
    deleteAllRelsFromSmallList(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteRelsFromLargeListCommitNormalExecution) {
    deleteRelsFromLargeList(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteRelsFromLargeListCommitRecovery) {
    deleteRelsFromLargeList(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteRelsFromLargeListRollbackNormalExecution) {
    deleteRelsFromLargeList(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteRelsFromLargeListRollbackRecovery) {
    deleteRelsFromLargeList(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteAllRelsFromLargeListCommitNormalExecution) {
    deleteAllRelsFromLargeList(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteAllRelsFromLargeListCommitRecovery) {
    deleteAllRelsFromLargeList(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteAllRelsFromLargeListRollbackNormalExecution) {
    deleteAllRelsFromLargeList(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteAllRelsFromLargeListRollbackRecovery) {
    deleteAllRelsFromLargeList(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteRelsFromUpdateStoreCommitNormalExecution) {
    deleteRelsFromUpdateStore(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteRelsFromUpdateStoreCommitRecovery) {
    deleteRelsFromUpdateStore(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteRelsFromUpdateStoreRollbackNormalExecution) {
    deleteRelsFromUpdateStore(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteRelsFromUpdateStoreRollbackRecovery) {
    deleteRelsFromUpdateStore(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteRelsAndQueryBWDListCommitNormalExecution) {
    deleteRelsAndQueryBWDList(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteRelsAndQueryBWDListCommitRecovery) {
    deleteRelsAndQueryBWDList(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteRelsAndQueryBWDListRollbackNormalExecution) {
    deleteRelsAndQueryBWDList(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteRelsAndQueryBWDListRollbackRecovery) {
    deleteRelsAndQueryBWDList(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteMultipleRelsCommitNormalExecution) {
    deleteMultipleRels(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteMultipleRelsCommitRecovery) {
    deleteMultipleRels(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteMultipleRelsRollbackNormalExecution) {
    deleteMultipleRels(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteMultipleRelsRollbackRecovery) {
    deleteMultipleRels(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteAllInsertedRelsCommitNormalExecution) {
    deleteAllInsertedRels(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteAllInsertedRelsCommitRecovery) {
    deleteAllInsertedRels(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteAllInsertedRelsRollbackNormalExecution) {
    deleteAllInsertedRels(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteAllInsertedRelsRollbackRecovery) {
    deleteAllInsertedRels(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteLargeNumRelsFromLargeListCommitNormalExecution) {
    deleteLargeNumRelsFromLargeList(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteLargeNumRelsFromLargeListCommitRecovery) {
    deleteLargeNumRelsFromLargeList(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteLargeNumRelsFromLargeListRollbackNormalExecution) {
    deleteLargeNumRelsFromLargeList(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteLargeNumRelsFromLargeListRollbackRecovery) {
    deleteLargeNumRelsFromLargeList(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteRelsTwoHop) {
    conn->beginWriteTransaction();
    ASSERT_TRUE(conn->query("MATCH (a:animal)-[e:knows]->(p1:person)-[:knows]->(p2:person) WHERE "
                            "a.ID = 5 AND p2.ID = 2439 DELETE e;")
                    ->isSuccess());
    vector<string> expectedResult = {"0", "1", "2", "3", "4", "6", "7", "8", "9"};
    ASSERT_EQ(
        TestHelper::convertResultToString(
            *conn->query("MATCH (a:animal)-[e:knows]->(p:person) WHERE a.ID < 10 RETURN e.length"),
            true /* checkOutputOrder */),
        expectedResult);
}

TEST_F(DeleteRelTest, MixedDeleteAndCreateRels) {
    conn->beginWriteTransaction();
    // We firstly delete rel person7|8 -> person0. Then insert person7->person0 and delete it again.
    auto knowsRelQuery = "MATCH (a:animal)-[e:knows]->(:person) where a.ID <= 10 return e.length";
    ASSERT_TRUE(
        conn->query(getDeleteKnowsRelQuery("animal", "person", 8 /* srcID */, 0 /* dstID */))
            ->isSuccess());
    ASSERT_TRUE(
        conn->query(getDeleteKnowsRelQuery("animal", "person", 7 /* srcID */, 0 /* dstID */))
            ->isSuccess());
    vector<string> expectedResult = {"0", "1", "2", "3", "4", "5", "6", "9", "10"};
    ASSERT_EQ(
        TestHelper::convertResultToString(*conn->query(knowsRelQuery), true /* checkOutputOrder */),
        expectedResult);
    auto result =
        conn->query(getInsertKnowsRelQuery("animal", "person", 7 /* srcID */, 0 /* dstID */));
    ASSERT_TRUE(result->isSuccess());
    expectedResult = {"0", "1", "2", "3", "4", "5", "6", "7", "9", "10"};
    result = conn->query(knowsRelQuery);
    ASSERT_EQ(
        TestHelper::convertResultToString(*result, true /* checkOutputOrder */), expectedResult);
    ASSERT_TRUE(
        conn->query(getDeleteKnowsRelQuery("animal", "person", 7 /* srcID */, 0 /* dstID */))
            ->isSuccess());
    expectedResult = {"0", "1", "2", "3", "4", "5", "6", "9", "10"};
    ASSERT_EQ(
        TestHelper::convertResultToString(*conn->query(knowsRelQuery), true /* checkOutputOrder */),
        expectedResult);
}

TEST_F(DeleteRelTest, DeleteRelsFromManyToOneTableCommitNormalExecution) {
    deleteRelsFromManyToOneTable(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteRelsFromManyToOneTableCommitRecovery) {
    deleteRelsFromManyToOneTable(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteRelsFromManyToOneTableRollbackNormalExecution) {
    deleteRelsFromManyToOneTable(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteRelsFromManyToOneTableRollbackRecovery) {
    deleteRelsFromManyToOneTable(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteRelsFromOneToOneTableCommitNormalExecution) {
    deleteRelsFromOneToOneTable(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteRelsFromOneToOneTableCommitRecovery) {
    deleteRelsFromOneToOneTable(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(DeleteRelTest, DeleteRelsFromOneToOneTableRollbackNormalExecution) {
    deleteRelsFromOneToOneTable(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(DeleteRelTest, DeleteRelsFromOneToOneTableRollbackRecovery) {
    deleteRelsFromOneToOneTable(false /* isCommit */, TransactionTestType::RECOVERY);
}

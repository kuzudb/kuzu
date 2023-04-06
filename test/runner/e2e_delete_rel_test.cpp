#include "common/string_utils.h"
#include "graph_test/graph_test.h"

using namespace kuzu::common;
using namespace kuzu::testing;

class DeleteRelTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/rel-update-tests/");
    }
    std::string getDeleteKnowsRelQuery(
        std::string srcTable, std::string dstTable, int64_t srcID, int64_t dstID) {
        return StringUtils::string_format(
            "MATCH (p1:{})-[e:knows]->(p2:{}) WHERE p1.ID = {} AND p2.ID = {} delete e;", srcTable,
            dstTable, srcID, dstID);
    }
    std::string getInsertKnowsRelQuery(
        std::string srcTable, std::string dstTable, int64_t srcID, int64_t dstID) {
        return StringUtils::string_format("MATCH (p1:{}), (p2:{}) WHERE p1.ID = {} AND p2.ID "
                                          "= {} create (p1)-[:knows {{length: {}}}]->(p2);",
            srcTable, dstTable, srcID, dstID, dstID);
    }

    std::string getDeleteTeachesRelQuery(int64_t srcID, int64_t dstID) {
        return StringUtils::string_format(
            "MATCH (p1:person)-[t:teaches]->(p2:person) WHERE p1.ID = {} AND p2.ID = {} delete t;",
            srcID, dstID);
    }

    std::string getDeleteHasOwnerRelQuery(int64_t srcID, int64_t dstID) {
        return StringUtils::string_format(
            "MATCH (a:animal)-[h:hasOwner]->(p:person) WHERE a.ID = {} AND p.ID = {} delete h;",
            srcID, dstID);
    }

    void sortAndCheckTestResults(
        std::vector<std::string>& actualResult, std::vector<std::string>& expectedResult) {
        sort(expectedResult.begin(), expectedResult.end());
        ASSERT_EQ(actualResult, expectedResult);
    }

    void deleteRelsFromSmallList(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        // We delete all person->person rels whose dst nodeID offset is between 10-20 (inclusive);
        for (auto i = 10; i <= 20; i++) {
            ASSERT_TRUE(conn->query(getDeleteKnowsRelQuery(
                                        "person", "person", 0 /* srcID */, i /* dstID */))
                            ->isSuccess());
        }
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto actualResult = TestHelper::convertResultToString(
            *conn->query("MATCH (p:person)-[e:knows]->(:person) WHERE p.ID  = 0 RETURN e.length"));
        std::vector<std::string> expectedResult;
        for (auto i = 1; i <= NUM_RELS_FOR_PERSON_0; i++) {
            // If we are not committing (e.g. rolling back), we expect to see all rels in the query
            // result. If we are committing, we should not see rels with length property
            // 10-20(inclusive) in the query result.
            if (!isCommit || (i < 10 || i > 20)) {
                expectedResult.push_back(std::to_string(i));
            }
        }
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void deleteAllRelsFromSmallList(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        ASSERT_TRUE(conn->query("MATCH (:person)-[e:knows]->(:person) DELETE e")->isSuccess());
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        std::vector<std::string> expectedResult;
        if (!isCommit) {
            for (auto i = 1u; i <= 2300; i++) {
                expectedResult.push_back(std::to_string(i));
            }
        }
        auto actualResult = TestHelper::convertResultToString(
            *conn->query("MATCH (p:person)-[e:knows]->(:person) WHERE p.ID = 0 RETURN e.length"));
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void deleteRelsFromLargeList(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        // We delete all person->person rels whose dst nodeID offset is between 100-200 (inclusive);
        for (auto i = 100; i <= 200; i++) {
            ASSERT_TRUE(conn->query(getDeleteKnowsRelQuery("person", "person", 0 /* srcID */, i))
                            ->isSuccess());
        }
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result =
            conn->query("MATCH (p:person)-[e:knows]->(:person) WHERE p.ID = 0 RETURN e.length");
        auto actualResult = TestHelper::convertResultToString(*result);
        std::vector<std::string> expectedResult;
        for (auto i = 1; i <= NUM_RELS_FOR_PERSON_0; i++) {
            // If we are not committing (e.g. rolling back), we are expected to see all rels in the
            // list. If we are committing, we are not expected to see the deleted rels whose dst
            // nodeID offset between 100-200 (inclusive).
            if (!isCommit || (i < 100 || i > 200)) {
                expectedResult.push_back(std::to_string(i));
            }
        }
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void deleteAllRelsFromLargeList(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        ASSERT_TRUE(conn->query("MATCH (:person)-[e:knows]->(:person) DELETE e")->isSuccess());
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        std::vector<std::string> expectedResult;
        if (!isCommit) {
            for (auto i = 1u; i <= NUM_RELS_FOR_PERSON_0; i++) {
                expectedResult.push_back(std::to_string(i));
            }
        }
        auto actualResult = TestHelper::convertResultToString(
            *conn->query("MATCH (p:person)-[e:knows]->(:person) WHERE p.ID = 0 RETURN e.length"));
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void deleteRelsFromUpdateStore(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        // We insert 3 rels: person51->person0, person52->person0, person100->person10, and then
        // delete person51->person0, person100->person10.
        ASSERT_TRUE(
            conn->query(getInsertKnowsRelQuery("person", "person", 51 /* srcID */, 0 /* dstID */))
                ->isSuccess());
        ASSERT_TRUE(
            conn->query(getInsertKnowsRelQuery("person", "person", 52 /* srcID */, 0 /* dstID */))
                ->isSuccess());
        ASSERT_TRUE(
            conn->query(getInsertKnowsRelQuery("person", "person", 100 /* srcID */, 10 /* dstID */))
                ->isSuccess());
        // Then we delete 2 rels: person51->person0, person100->person10.
        ASSERT_TRUE(
            conn->query(getDeleteKnowsRelQuery("person", "person", 51 /* srcID */, 0 /* dstID */))
                ->isSuccess());
        ASSERT_TRUE(
            conn->query(getDeleteKnowsRelQuery("person", "person", 100 /* srcID */, 10 /* dstID */))
                ->isSuccess());
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto actualResult = TestHelper::convertResultToString(
            *conn->query("MATCH (p:person)-[e:knows]->(:person) WHERE p.ID > 10 return e.length"));
        // After insertion and deletion, we are expected to see only one new rel: person52->person0
        // (if we are committing).
        std::vector<std::string> expectedResult;
        if (isCommit) {
            expectedResult.push_back(std::to_string(0));
        }
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void deleteMultipleRels(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        // Delete all rels from person1->person[0-40].
        ASSERT_TRUE(conn->query("MATCH (p:person)-[e:knows]->(p1:person) WHERE p.ID = 1 AND p1.ID "
                                "< 45 DELETE e;")
                        ->isSuccess());
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        std::vector<std::string> expectedResult;
        if (isCommit) {
            expectedResult = std::vector<std::string>{"45", "46", "47", "48", "49", "50"};
        } else {
            for (auto i = 0u; i <= 50; i++) {
                expectedResult.push_back(std::to_string(i));
            }
        }
        auto actualResult = TestHelper::convertResultToString(
            *conn->query("MATCH (a:person)-[e:knows]->(p:person) WHERE a.ID = 1 RETURN e.length"));
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void deleteAllInsertedRels(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        // Insert rels for person1->person[51-60].
        for (auto i = 51; i < 60; i++) {
            ASSERT_TRUE(conn->query(getInsertKnowsRelQuery("person", "person", 1, i))->isSuccess());
        }
        // Then delete those inserted rels.
        for (auto i = 51; i < 60; i++) {
            ASSERT_TRUE(conn->query(getDeleteKnowsRelQuery("person", "person", 1, i))->isSuccess());
        }
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        std::vector<std::string> expectedResult = {
            "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50"};
        auto actualResult = TestHelper::convertResultToString(
            *conn->query("MATCH (p:person)-[e:knows]->(p1:person) WHERE p1.ID >= 40 AND p.ID = 1 "
                         "RETURN e.length"));
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void deleteLargeNumRelsFromLargeList(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        conn->query(
            "MATCH (p1:person)-[e:knows]->(p2:person) WHERE p1.ID = 0 AND p2.ID >= 10 delete e;");
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result =
            conn->query("MATCH (p1:person)-[e:knows]->(p2:person) WHERE p1.ID = 0 return e.length");
        auto actualResult = TestHelper::convertResultToString(*result);
        std::vector<std::string> expectedResult;
        for (auto i = 1; i <= NUM_RELS_FOR_PERSON_0; i++) {
            // If we are not committing (e.g. rolling back), we are expected to see all rels in the
            // list. If we are committing, we are not expected to see the deleted rels whose dst
            // nodeID offset >= 10.
            if (!isCommit || i < 10) {
                expectedResult.push_back(std::to_string(i));
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
        std::vector<std::string> expectedResult =
            isCommit ? std::vector<std::string>{"21", "22", "32", "33"} :
                       std::vector<std::string>{"11", "21", "22", "31", "32", "33"};
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
        std::vector<std::string> expectedResult =
            isCommit ? std::vector<std::string>{"2", "4", "6", "8"} :
                       std::vector<std::string>{"1", "2", "3", "4", "5", "6", "7", "8", "9", "11"};
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    static constexpr uint64_t NUM_RELS_FOR_PERSON_0 = 2300;
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
    // This query will delete rels: person1->person0->person5 and person1->person1->person5
    ASSERT_TRUE(conn->query("MATCH (p:person)-[e:knows]->(p1:person)-[:knows]->(p2:person) WHERE "
                            "p.ID = 1 AND p2.ID = 5 DELETE e;")
                    ->isSuccess());
    std::vector<std::string> expectedResult = {"2", "3", "4", "5", "6", "7", "8", "9"};
    ASSERT_EQ(TestHelper::convertResultToString(
                  *conn->query("MATCH (a:person)-[e:knows]->(p:person) WHERE a.ID = 1 AND p.ID < "
                               "10 RETURN e.length"),
                  true /* checkOutputOrder */),
        expectedResult);
}

TEST_F(DeleteRelTest, MixedDeleteAndCreateRels) {
    conn->beginWriteTransaction();
    // We firstly delete rel person0 -> person7|8. Then insert person0->person7 and delete it again.
    auto knowsRelQuery =
        "MATCH (p0:person)-[e:knows]->(p1:person) where p0.ID = 0 AND p1.ID <= 10 return e.length";
    ASSERT_TRUE(
        conn->query(getDeleteKnowsRelQuery("person", "person", 0 /* srcID */, 7 /* dstID */))
            ->isSuccess());
    ASSERT_TRUE(
        conn->query(getDeleteKnowsRelQuery("person", "person", 0 /* srcID */, 8 /* dstID */))
            ->isSuccess());
    std::vector<std::string> expectedResult = {"1", "2", "3", "4", "5", "6", "9", "10"};
    ASSERT_EQ(
        TestHelper::convertResultToString(*conn->query(knowsRelQuery), true /* checkOutputOrder */),
        expectedResult);
    auto result =
        conn->query(getInsertKnowsRelQuery("person", "person", 0 /* srcID */, 7 /* dstID */));
    ASSERT_TRUE(result->isSuccess());
    expectedResult = {"1", "2", "3", "4", "5", "6", "7", "9", "10"};
    result = conn->query(knowsRelQuery);
    ASSERT_EQ(
        TestHelper::convertResultToString(*result, true /* checkOutputOrder */), expectedResult);
    ASSERT_TRUE(
        conn->query(getDeleteKnowsRelQuery("person", "person", 0 /* srcID */, 7 /* dstID */))
            ->isSuccess());
    expectedResult = {"1", "2", "3", "4", "5", "6", "9", "10"};
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

#include "common/string_utils.h"
#include "graph_test/graph_test.h"

using namespace kuzu::common;
using namespace kuzu::testing;

class UpdateRelTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/rel-update-tests/");
    }
    std::string getUpdateRelQuery(std::string srcTable, std::string dstTable, std::string relation,
        int64_t srcID, int64_t dstID, std::string setPropertyClause) {
        return StringUtils::string_format(
                   "MATCH (p1:{})-[e:{}]->(p2:{}) WHERE p1.ID = {} AND p2.ID = {} ", srcTable,
                   relation, dstTable, srcID, dstID) +
               setPropertyClause;
    }
    std::string getInsertKnowsRelQuery(std::string srcTable, std::string dstTable, int64_t srcID,
        int64_t dstID, int64_t lengthProp) {
        return StringUtils::string_format(
            "MATCH (p1:{}), (p2:{}) WHERE p1.ID = {} AND p2.ID = {} create "
            "(p1)-[:knows {{length: {}}}]->(p2);",
            srcTable, dstTable, srcID, dstID, lengthProp);
    }
    std::string getDeleteKnowsRelQuery(
        std::string srcTable, std::string dstTable, int64_t srcID, int64_t dstID) {
        return StringUtils::string_format(
            "MATCH (p1:{})-[e:knows]->(p2:{}) WHERE p1.ID = {} AND p2.ID = {} DELETE e", srcTable,
            dstTable, srcID, dstID);
    }

    void updateIntProp(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
                                    "knows" /* relation */, 0 /* srcID */, 20 /* dstID */,
                                    "set e.length = null"))
                        ->isSuccess());
        ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
                                    "knows" /* relation */, 0 /* srcID */, 25 /* dstID */,
                                    "set e.length = 210"))
                        ->isSuccess());
        ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
                                    "knows" /* relation */, 0 /* srcID */, 30 /* dstID */,
                                    "set e.length = 300"))
                        ->isSuccess());
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result =
            conn->query("MATCH (p0:person)-[e:knows]->(p1:person) WHERE p1.ID >= 20 AND p1.ID "
                        "<= 30 AND p0.ID = 0 return e.length");
        auto actualResult = TestHelper::convertResultToString(*result);
        std::vector<std::string> expectedResult =
            isCommit ? std::vector<std::string>{"", "21", "22", "23", "24", "210", "26", "27", "28",
                           "29", "300"} :
                       std::vector<std::string>{
                           "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30"};
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void updateStrProp(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
                                    "knows" /* relation */, 0 /* srcID */, 15 /* dstID */,
                                    "set e.place = 'waterloo'"))
                        ->isSuccess());
        ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
                                    "knows" /* relation */, 0 /* srcID */, 16 /* dstID */,
                                    "set e.place = 'kitchener'"))
                        ->isSuccess());
        ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
                                    "knows" /* relation */, 0 /* srcID */, 19 /* dstID */,
                                    "set e.place = null"))
                        ->isSuccess());
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result = conn->query("MATCH (p0:person)-[e:knows]->(p1:person) where p1.ID >= 10 and "
                                  "p1.ID <= 20 and p0.ID = 0 return e.place");
        auto actualResult = TestHelper::convertResultToString(*result);
        auto expectedResult = isCommit ?
                                  std::vector<std::string>{"2990", "2989", "2988", "2987", "2986",
                                      "waterloo", "kitchener", "2983", "2982", "", "2980"} :
                                  std::vector<std::string>{"2990", "2989", "2988", "2987", "2986",
                                      "2985", "2984", "2983", "2982", "2981", "2980"};
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void updateListProp(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
                                    "knows" /* relation */, 0 /* srcID */, 43 /* dstID */,
                                    "set e.tag = [\"updated property1\", \"50\"]"))
                        ->isSuccess());
        ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
                                    "knows" /* relation */, 0 /* srcID */, 47 /* dstID */,
                                    "set e.tag = null"))
                        ->isSuccess());
        ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
                                    "knows" /* relation */, 0 /* srcID */, 49 /* dstID */,
                                    "set e.tag = [\"updated property3\", \"54\"]"))
                        ->isSuccess());
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result = conn->query("MATCH (p0:person)-[e:knows]->(p1:person) where p1.ID >= 40 and "
                                  "p1.ID <= 50 and p0.ID = 0 return e.tag");
        auto actualResult = TestHelper::convertResultToString(*result);
        auto expectedResult =
            isCommit ? std::vector<std::string>{"[2960]", "[2959]", "[2958]",
                           "[updated property1,50]", "[2956]", "[2955]", "[2954]", "", "[2952]",
                           "[updated property3,54]", "[2950]"} :
                       std::vector<std::string>{"[2960]", "[2959]", "[2958]", "[2957]", "[2956]",
                           "[2955]", "[2954]", "[2953]", "[2952]", "[2951]", "[2950]"};
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void updateEachElementOfSmallList(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        std::vector<std::string> expectedResult;
        for (auto i = 0u; i <= 50; i++) {
            ASSERT_TRUE(
                conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
                                "knows" /* relation */, 1 /* srcID */, i /* dstID */,
                                "set e.length = " + std::to_string(i + 10)))
                    ->isSuccess());
            expectedResult.push_back(std::to_string(isCommit ? i + 10 : i));
        }
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result =
            conn->query("MATCH (p0:person)-[e:knows]->(p1:person) WHERE p0.ID = 1 RETURN e.length");
        auto actualResult = TestHelper::convertResultToString(*result);
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void updateEachElementOfLargeList(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        std::vector<std::string> expectedResult;
        for (auto i = 1u; i <= NUM_RELS_FOR_PERSON_0; i++) {
            ASSERT_TRUE(
                conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
                                "knows" /* relation */, 0 /* srcID */, i /* dstID */,
                                "set e.length = " + std::to_string(3 * i)))
                    ->isSuccess());
            expectedResult.push_back(std::to_string(isCommit ? 3 * i : i));
        }
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result =
            conn->query("MATCH (p0:person)-[e:knows]->(p1:person) WHERE p0.ID = 0 return e.length");
        auto actualResult = TestHelper::convertResultToString(*result);
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void updateNewlyInsertedRels(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        ASSERT_TRUE(conn->query(getInsertKnowsRelQuery("person" /* srcTable */,
            "person" /* dstTable */, 5 /*  srcID */, 8 /* dstID */, 10 /* lengthProp */)));
        ASSERT_TRUE(conn->query(getInsertKnowsRelQuery("person" /* srcTable */,
            "person" /* dstTable */, 7 /*  srcID */, 10 /* dstID */, 20 /* lengthProp */)));
        ASSERT_TRUE(conn->query(getInsertKnowsRelQuery("person" /* srcTable */,
            "person" /* dstTable */, 32 /*  srcID */, 51 /* dstID */, 30 /* lengthProp */)));
        ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
            "knows" /* relation */, 5 /* srcID */, 8 /* dstID */, "set e.length = 55")));
        ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
            "knows" /* relation */, 7 /* srcID */, 10 /* dstID */, "set e.length = null")));
        ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
            "knows" /* relation */, 32 /* srcID */, 51 /* dstID */, "set e.length = 201")));
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result =
            conn->query("MATCH (p:person)-[e:knows]->(:person) where p.ID > 4 return e.length");
        auto actualResult = TestHelper::convertResultToString(*result);
        auto expectedResult =
            isCommit ? std::vector<std::string>{"", "55", "201"} : std::vector<std::string>{};
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void updateRelsTwoHop(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        ASSERT_TRUE(
            conn->query("MATCH (p0:person)-[e1:knows]->(p1:person)-[e2:knows]->(p2:person) WHERE "
                        "p0.ID = 1 AND p2.ID = 1145 SET e1.length = 300, e2.length = null"));
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result =
            conn->query("MATCH (p0:person)-[e1:knows]->(p1:person)-[e2:knows]->(p2:person) WHERE "
                        "p0.ID = 1 AND p2.ID = 1145 RETURN e1.length, e2.length");
        auto actualResult = TestHelper::convertResultToString(*result);
        auto expectedResult =
            isCommit ? std::vector<std::string>{"300|"} : std::vector<std::string>{"0|1145"};
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void insertDeleteAndUpdateRelsInSameList(
        bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        // Firstly, we delete the following knows rels: person0->person50, person0->person612,
        // person0->person1300.
        ASSERT_TRUE(conn->query(getDeleteKnowsRelQuery(
            "person" /* srcTable */, "person" /* dstTable */, 0 /* srcID */, 50 /* dstID */)));
        ASSERT_TRUE(conn->query(getDeleteKnowsRelQuery(
            "person" /* srcTable */, "person" /* dstTable */, 0 /* srcID */, 612 /* dstID */)));
        ASSERT_TRUE(conn->query(getDeleteKnowsRelQuery(
            "person" /* srcTable */, "person" /* dstTable */, 0 /* srcID */, 1300 /* dstID */)));
        // Then, we insert a knows rel: person0->person0(with length=30).
        ASSERT_TRUE(conn->query(getInsertKnowsRelQuery(
            "person" /* srcTable */, "person" /* dstTable */, 0 /* srcID */, 0 /* dstID */, 30)));
        // Lastly, we update the following knows rels:
        // person0->person100(SET length=712), person0->person500(SET length=400)
        ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
            "knows" /* relation */, 0 /* srcID */, 100 /* dstID */, "SET e.length=712")));
        ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
            "knows" /* relation */, 0 /* srcID */, 500 /* dstID */, "SET e.length=400")));
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        std::vector<std::string> expectedResult;
        for (auto i = 1u; i <= NUM_RELS_FOR_PERSON_0; i++) {
            expectedResult.push_back(std::to_string(i));
        }
        if (isCommit) {
            // 1. We deleted knows rels: person0->person50 (lengthProp = 50),
            // person0->person612 (lengthProp = 612), person0->person1300(lengthProp = 1300).
            expectedResult.erase(
                std::remove(expectedResult.begin(), expectedResult.end(), std::to_string(50)),
                expectedResult.end());
            expectedResult.erase(
                std::remove(expectedResult.begin(), expectedResult.end(), std::to_string(612)),
                expectedResult.end());
            expectedResult.erase(
                std::remove(expectedResult.begin(), expectedResult.end(), std::to_string(1300)),
                expectedResult.end());
            // 2. We insert knows rel: person0->person0 (lengthProp = 30);
            expectedResult.emplace_back("30");
            // 3. We update the following rels: person0->person100(SET length=712),
            // person0->person500(SET length=400)
            *find(expectedResult.begin(), expectedResult.end(), std::to_string(100)) = "712";
            *find(expectedResult.begin(), expectedResult.end(), std::to_string(500)) = "400";
        }
        auto result =
            conn->query("MATCH (p0:person)-[e:knows]->(p1:person) WHERE p0.ID = 0 return e.length");
        auto actualResult = TestHelper::convertResultToString(*result);
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void insertAndUpdateRelsForNewlyAddedNode(
        bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        ASSERT_TRUE(conn->query("CREATE (p:person {ID: 2501})"));
        ASSERT_TRUE(conn->query(getInsertKnowsRelQuery("person" /* srcTable */,
            "person" /* dstTable */, 2501 /* srcID */, 5 /* dstID */, 100 /* lengthProp */)));
        ASSERT_TRUE(conn->query(getInsertKnowsRelQuery("person" /* srcTable */,
            "person" /* dstTable */, 2501 /* srcID */, 8 /* dstID */, 700 /* lengthProp */)));
        ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
            "knows" /* relation */, 2501 /* srcID */, 5 /* dstID */, "SET e.length = 20")));
        ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* relation */,
            "knows" /* relation */, 2501 /* srcID */, 8 /* dstID */, "SET e.length = 421")));
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto expectedResult =
            isCommit ? std::vector<std::string>{"20", "421"} : std::vector<std::string>{};
        auto result =
            conn->query("MATCH (p:person)-[e:knows]->(:person) WHERE p.ID > 2500 RETURN e.length");
        auto actualResult = TestHelper::convertResultToString(*result);
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void updateManyToOneRelTable(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
            "teaches" /* relation */, 21 /* srcID */, 2 /* dstID */, "SET e.length=null")));
        ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
            "teaches" /* relation */, 32 /* srcID */, 3 /* dstID */, "SET e.length = 512")));
        ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* relation */,
            "teaches" /* relation */, 33 /* srcID */, 3 /* dstID */, "SET e.length = 312")));
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto expectedResult = isCommit ?
                                  std::vector<std::string>{"11", "", "22", "31", "512", "312"} :
                                  std::vector<std::string>{"11", "21", "22", "31", "32", "33"};
        auto result = conn->query("MATCH (p:person)-[e:teaches]->(:person) RETURN e.length");
        auto actualResult = TestHelper::convertResultToString(*result);
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void updateOneToOneRelTable(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        ASSERT_TRUE(conn->query(getUpdateRelQuery("animal" /* srcTable */, "person" /* dstTable */,
            "hasOwner" /* relation */, 2 /* srcID */, 52 /* dstID */, "SET e.place='kuzu'")));
        ASSERT_TRUE(conn->query(getUpdateRelQuery("animal" /* srcTable */, "person" /* dstTable */,
            "hasOwner" /* relation */, 4 /* srcID */, 54 /* dstID */, "SET e.place='db'")));
        ASSERT_TRUE(conn->query(getUpdateRelQuery("animal" /* srcTable */, "person" /* relation */,
            "hasOwner" /* relation */, 8 /* srcID */, 58 /* dstID */, "SET e.place=null")));
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto expectedResult =
            isCommit ? std::vector<std::string>{"1999", "kuzu", "1997", "db", "1995",
                           "199419941994", "1993", "", "1991", "1989"} :
                       std::vector<std::string>{"1999", "199819981998", "1997", "199619961996",
                           "1995", "199419941994", "1993", "199219921992", "1991", "1989"};
        auto result = conn->query("MATCH (:animal)-[e:hasOwner]->(:person) RETURN e.place");
        auto actualResult = TestHelper::convertResultToString(*result);
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    static constexpr uint64_t NUM_RELS_FOR_PERSON_0 = 2300;
};

TEST_F(UpdateRelTest, UpdateIntPropCommitNormalExecution) {
    updateIntProp(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, UpdateIntPropCommitRecovery) {
    updateIntProp(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(UpdateRelTest, UpdateIntPropRollbackNormalExecution) {
    updateIntProp(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, UpdateIntPropRollbacktRecovery) {
    updateIntProp(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(UpdateRelTest, UpdateStrPropCommitNormalExecution) {
    updateStrProp(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, UpdateStrPropCommitRecovery) {
    updateStrProp(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(UpdateRelTest, UpdateStrPropRollbackNormalExecution) {
    updateStrProp(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, UpdateStrPropRollbacktRecovery) {
    updateStrProp(false /* isCommit */, TransactionTestType::RECOVERY);
}

// TEST_F(UpdateRelTest, UpdateListPropCommitNormalExecution) {
//    updateListProp(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(UpdateRelTest, UpdateListPropCommitRecovery) {
//    updateListProp(true /* isCommit */, TransactionTestType::RECOVERY);
//}
//
// TEST_F(UpdateRelTest, UpdateListPropRollbackNormalExecution) {
//    updateListProp(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(UpdateRelTest, UpdateListPropRollbacktRecovery) {
//    updateListProp(false /* isCommit */, TransactionTestType::RECOVERY);
//}

TEST_F(UpdateRelTest, UpdateEachElementOfSmallListCommitNormalExecution) {
    updateEachElementOfSmallList(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, UpdateEachElementOfSmallListCommitRecovery) {
    updateEachElementOfSmallList(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(UpdateRelTest, UpdateEachElementOfSmallListRollbackNormalExecution) {
    updateEachElementOfSmallList(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, UpdateEachElementOfSmallListRollbackRecovery) {
    updateEachElementOfSmallList(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(UpdateRelTest, UpdateEachElementOfLargeListCommitNormalExecution) {
    updateEachElementOfLargeList(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, UpdateEachElementOfLargeListCommitRecovery) {
    updateEachElementOfLargeList(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(UpdateRelTest, UpdateEachElementOfLargeListRollbackNormalExecution) {
    updateEachElementOfLargeList(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, UpdateEachElementOfLargeListRollbackRecovery) {
    updateEachElementOfLargeList(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(UpdateRelTest, UpdateNewlyInsertedRelsCommitNormalExecution) {
    updateNewlyInsertedRels(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, UpdateNewlyInsertedRelsCommitRecovery) {
    updateNewlyInsertedRels(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(UpdateRelTest, UpdateNewlyInsertedRelsRollbackNormalExecution) {
    updateNewlyInsertedRels(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, UpdateNewlyInsertedRelsRollbackRecovery) {
    updateNewlyInsertedRels(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(UpdateRelTest, UpdateRelsTwoHopCommitNormalExecution) {
    updateRelsTwoHop(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, UpdateRelsTwoHopCommitRecovery) {
    updateRelsTwoHop(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(UpdateRelTest, UpdateRelsTwoHopRollbackNormalExecution) {
    updateRelsTwoHop(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, UpdateRelsTwoHopRollbackRecovery) {
    updateRelsTwoHop(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(UpdateRelTest, InsertDeleteAndUpdateRelsInSameListCommitNormalExecution) {
    insertDeleteAndUpdateRelsInSameList(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, InsertDeleteAndUpdateRelsInSameListCommitRecovery) {
    insertDeleteAndUpdateRelsInSameList(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(UpdateRelTest, InsertDeleteAndUpdateRelsInSameListRollbackNormalExecution) {
    insertDeleteAndUpdateRelsInSameList(
        false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, InsertDeleteAndUpdateRelsInSameListRollbackRecovery) {
    insertDeleteAndUpdateRelsInSameList(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(UpdateRelTest, InsertAndUpdateRelsForNewlyAddedNodeCommitNormalExecution) {
    insertAndUpdateRelsForNewlyAddedNode(
        true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, InsertAndUpdateRelsForNewlyAddedNodeCommitRecovery) {
    insertAndUpdateRelsForNewlyAddedNode(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(UpdateRelTest, InsertAndUpdateRelsForNewlyAddedNodeRollbackNormalExecution) {
    insertAndUpdateRelsForNewlyAddedNode(
        false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, InsertAndUpdateRelsForNewlyAddedNodeRollbackRecovery) {
    insertAndUpdateRelsForNewlyAddedNode(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(UpdateRelTest, UpdateManyToOneRelTableCommitNormalExecution) {
    updateManyToOneRelTable(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, UpdateManyToOneRelTableCommitRecovery) {
    updateManyToOneRelTable(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(UpdateRelTest, UpdateManyToOneRelTableRollbackNormalExecution) {
    updateManyToOneRelTable(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, UpdateManyToOneRelTableRollbackRecovery) {
    updateManyToOneRelTable(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(UpdateRelTest, UpdateOneToOneRelTableCommitNormalExecution) {
    updateOneToOneRelTable(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, UpdateOneToOneRelTableCommitRecovery) {
    updateOneToOneRelTable(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(UpdateRelTest, UpdateOneToOneRelTableRollbackNormalExecution) {
    updateOneToOneRelTable(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, UpdateOneToOneRelTableRollbackRecovery) {
    updateOneToOneRelTable(false /* isCommit */, TransactionTestType::RECOVERY);
}

#include "test_helper/test_helper.h"

using namespace kuzu::testing;

class UpdateRelTest : public DBTest {
public:
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/rel-insertion-tests/");
    }
    string getUpdateRelQuery(string srcTable, string dstTable, string relation, int64_t srcID,
        int64_t dstID, string setPropertyClause) {
        return StringUtils::string_format(
            "MATCH (p1:%s)-[e:%s]->(p2:%s) WHERE p1.ID = %d AND p2.ID = %d " + setPropertyClause,
            srcTable.c_str(), relation.c_str(), dstTable.c_str(), srcID, dstID);
    }
    string getInsertKnowsRelQuery(
        string srcTable, string dstTable, int64_t srcID, int64_t dstID, int64_t lengthProp) {
        return StringUtils::string_format("MATCH (p1:%s), (p2:%s) WHERE p1.ID = %d AND p2.ID "
                                          "= %d create (p1)-[:knows {length: %d}]->(p2);",
            srcTable.c_str(), dstTable.c_str(), srcID, dstID, lengthProp);
    }
    string getDeleteKnowsRelQuery(string srcTable, string dstTable, int64_t srcID, int64_t dstID) {
        return StringUtils::string_format(
            "MATCH (p1:%s)-[e:knows]->(p2:%s) WHERE p1.ID = %d AND p2.ID = %d DELETE e",
            srcTable.c_str(), dstTable.c_str(), srcID, dstID);
    }

    void updateIntProp(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        ASSERT_TRUE(conn->query(getUpdateRelQuery("animal" /* srcTable */, "person" /* dstTable */,
                                    "knows" /* relation */, 20 /* srcID */, 0 /* dstID */,
                                    "set e.length = null"))
                        ->isSuccess());
        ASSERT_TRUE(conn->query(getUpdateRelQuery("animal" /* srcTable */, "person" /* dstTable */,
                                    "knows" /* relation */, 25 /* srcID */, 0 /* dstID */,
                                    "set e.length = 210"))
                        ->isSuccess());
        ASSERT_TRUE(conn->query(getUpdateRelQuery("animal" /* srcTable */, "person" /* dstTable */,
                                    "knows" /* relation */, 30 /* srcID */, 0 /* dstID */,
                                    "set e.length = 300"))
                        ->isSuccess());
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result = conn->query("MATCH (a:animal)-[e:knows]->(:person) where a.ID >= 20 and a.ID "
                                  "<= 30 return e.length");
        auto actualResult = TestHelper::convertResultToString(*result);
        vector<string> expectedResult =
            isCommit ?
                vector<string>{"", "21", "22", "23", "24", "210", "26", "27", "28", "29", "300"} :
                vector<string>{"20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "30"};
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void updateStrProp(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        ASSERT_TRUE(conn->query(getUpdateRelQuery("animal" /* srcTable */, "person" /* dstTable */,
                                    "knows" /* relation */, 15 /* srcID */, 0 /* dstID */,
                                    "set e.place = 'waterloo'"))
                        ->isSuccess());
        ASSERT_TRUE(conn->query(getUpdateRelQuery("animal" /* srcTable */, "person" /* dstTable */,
                                    "knows" /* relation */, 16 /* srcID */, 0 /* dstID */,
                                    "set e.place = 'kitchener'"))
                        ->isSuccess());
        ASSERT_TRUE(conn->query(getUpdateRelQuery("animal" /* srcTable */, "person" /* dstTable */,
                                    "knows" /* relation */, 19 /* srcID */, 0 /* dstID */,
                                    "set e.place = null"))
                        ->isSuccess());
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result = conn->query(
            "MATCH (a:animal)-[e:knows]->(:person) where a.ID >= 10 and a.ID <= 20 return e.place");
        auto actualResult = TestHelper::convertResultToString(*result);
        auto expectedResult =
            isCommit ? vector<string>{"990990990", "989", "988988988", "987", "986986986",
                           "waterloo", "kitchener", "983", "982982982", "", "980980980"} :
                       vector<string>{"990990990", "989", "988988988", "987", "986986986", "985",
                           "984984984", "983", "982982982", "981", "980980980"};
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void updateListProp(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        ASSERT_TRUE(conn->query(getUpdateRelQuery("animal" /* srcTable */, "person" /* dstTable */,
                                    "knows" /* relation */, 43 /* srcID */, 0 /* dstID */,
                                    "set e.tag = [\"updated property1\", \"50\"]"))
                        ->isSuccess());
        ASSERT_TRUE(conn->query(getUpdateRelQuery("animal" /* srcTable */, "person" /* dstTable */,
                                    "knows" /* relation */, 47 /* srcID */, 0 /* dstID */,
                                    "set e.tag = null"))
                        ->isSuccess());
        ASSERT_TRUE(conn->query(getUpdateRelQuery("animal" /* srcTable */, "person" /* dstTable */,
                                    "knows" /* relation */, 49 /* srcID */, 0 /* dstID */,
                                    "set e.tag = [\"updated property3\", \"54\"]"))
                        ->isSuccess());
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result = conn->query(
            "MATCH (a:animal)-[e:knows]->(:person) where a.ID >= 40 and a.ID <= 50 return e.tag");
        auto actualResult = TestHelper::convertResultToString(*result);
        auto expectedResult =
            isCommit ? vector<string>{"[960960960]", "[959]", "[958958958]",
                           "[updated property1,50]", "[956956956]", "[955]", "[954954954]", "",
                           "[952952952]", "[updated property3,54]", "[950950950]"} :
                       vector<string>{"[960960960]", "[959]", "[958958958]", "[957]", "[956956956]",
                           "[955]", "[954954954]", "[953]", "[952952952]", "[951]", "[950950950]"};
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void updateEachElementOfSmallList(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        vector<string> expectedResult;
        for (auto i = 0u; i <= 50; i++) {
            ASSERT_TRUE(
                conn->query(getUpdateRelQuery("animal" /* srcTable */, "person" /* dstTable */,
                                "knows" /* relation */, i /* srcID */, 0 /* dstID */,
                                "set e.length = " + to_string(i + 10)))
                    ->isSuccess());
            expectedResult.push_back(to_string(isCommit ? i + 10 : i));
        }
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result = conn->query("MATCH (:animal)-[e:knows]->(p:person) return e.length");
        auto actualResult = TestHelper::convertResultToString(*result);
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void updateEachElementOfLargeList(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        vector<string> expectedResult;
        for (auto i = 1u; i <= NUM_PERSON_KNOWS_PERSON_RELS; i++) {
            ASSERT_TRUE(
                conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
                                "knows" /* relation */, 0 /* srcID */, i /* dstID */,
                                "set e.length = " + to_string(i)))
                    ->isSuccess());
            expectedResult.push_back(to_string(isCommit ? i : 3 * (i - 1)));
        }
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result = conn->query("MATCH (:person)-[e:knows]->(p:person) return e.length");
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
        auto expectedResult = isCommit ? vector<string>{"", "55", "201"} : vector<string>{};
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void updateRelsTwoHop(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        ASSERT_TRUE(
            conn->query("MATCH (a:animal)-[e1:knows]->(p:person)-[e2:knows]->(p1:person) WHERE "
                        "p1.ID = 2500 AND a.ID = 5 SET e1.length = 300, e2.length = null"));
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result =
            conn->query("MATCH (a:animal)-[e1:knows]->(p:person)-[e2:knows]->(p1:person) WHERE "
                        "p1.ID = 2500 AND a.ID = 5 RETURN e1.length, e2.length");
        auto actualResult = TestHelper::convertResultToString(*result);
        auto expectedResult = isCommit ? vector<string>{"300|"} : vector<string>{"5|7497"};
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
        vector<string> expectedResult;
        for (auto i = 1u; i <= NUM_PERSON_KNOWS_PERSON_RELS; i++) {
            expectedResult.push_back(to_string(3 * (i - 1)));
        }
        if (isCommit) {
            // 1. We deleted knows rels: person0->person50 (lengthProp = 3 * (50 - 1)),
            // person0->person612 (lengthProp = 3 * (612 - 1)), person0->person1300(lengthProp = 3 *
            // (1300 - 1)).
            expectedResult.erase(
                std::remove(expectedResult.begin(), expectedResult.end(), to_string(3 * (50 - 1))),
                expectedResult.end());
            expectedResult.erase(
                std::remove(expectedResult.begin(), expectedResult.end(), to_string(3 * (612 - 1))),
                expectedResult.end());
            expectedResult.erase(std::remove(expectedResult.begin(), expectedResult.end(),
                                     to_string(3 * (1300 - 1))),
                expectedResult.end());
            // 2. We insert knows rel: person0->person0 (lengthProp = 30);
            expectedResult.push_back("30");
            // 3. We update the following rels: person0->person100(SET length=712),
            // person0->person500(SET length=400)
            *find(expectedResult.begin(), expectedResult.end(), to_string(3 * (100 - 1))) = "712";
            *find(expectedResult.begin(), expectedResult.end(), to_string(3 * (500 - 1))) = "400";
        }
        auto result = conn->query("MATCH (:person)-[e:knows]->(p:person) return e.length");
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
        auto expectedResult = isCommit ? vector<string>{"20", "421"} : vector<string>{};
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
        auto expectedResult = isCommit ? vector<string>{"11", "", "22", "31", "512", "312"} :
                                         vector<string>{"11", "21", "22", "31", "32", "33"};
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
            isCommit ? vector<string>{"1999", "kuzu", "1997", "db", "1995", "199419941994", "1993",
                           "", "1991", "1989"} :
                       vector<string>{"1999", "199819981998", "1997", "199619961996", "1995",
                           "199419941994", "1993", "199219921992", "1991", "1989"};
        auto result = conn->query("MATCH (:animal)-[e:hasOwner]->(:person) RETURN e.place");
        auto actualResult = TestHelper::convertResultToString(*result);
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    static constexpr uint64_t NUM_PERSON_KNOWS_PERSON_RELS = 2500;
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

TEST_F(UpdateRelTest, UpdateListPropCommitNormalExecution) {
    updateListProp(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, UpdateListPropCommitRecovery) {
    updateListProp(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(UpdateRelTest, UpdateListPropRollbackNormalExecution) {
    updateListProp(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(UpdateRelTest, UpdateListPropRollbacktRecovery) {
    updateListProp(false /* isCommit */, TransactionTestType::RECOVERY);
}

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

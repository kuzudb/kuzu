#include "common/string_utils.h"
#include "graph_test/graph_test.h"

using namespace kuzu::common;
using namespace kuzu::testing;

class CreateRelTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/rel-update-tests/");
    }

    void insertRel(std::string srcNode, int64_t srcPk, std::string dstNode, int64_t dstPK,
        std::string relation, std::string propertyValues) {
        auto matchClause = "MATCH (a:" + srcNode + "),(b:" + dstNode +
                           ") WHERE a.ID=" + std::to_string(srcPk) +
                           " AND b.ID=" + std::to_string(dstPK) + " ";
        auto createClause = "CREATE (a)-[e:" + relation + " " + propertyValues + "]->(b)";
        ASSERT_TRUE(conn->query(matchClause + createClause)->isSuccess());
    }

    void insertRelsToSmallList(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        insertRel("person" /* srcNode */, 1 /* srcPK */, "person" /* dstNode */, 300 /* dstPK */,
            "knows" /* relation */, "{length: 300, place: 'this is a long str', tag: ['123', 'good']}" /* propertyValues */);
        insertRel("person" /* srcNode */, 1 /* srcPK */, "person" /* dstNode */, 700 /* dstPK */,
            "knows" /* relation */, "{length: 45, tag: ['123', 'good']}" /* propertyValues */);
        insertRel("person" /* srcNode */, 1 /* srcPK */, "person" /* dstNode */, 400 /* dstPK */,
            "knows" /* relation */, "{length: 34, place: 'short'}" /* propertyValues */);
        insertRel("person" /* srcNode */, 1 /* srcPK */, "person" /* dstNode */, 600 /* dstPK */,
            "knows" /* relation */, "{}" /* propertyValues */);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result =
            conn->query("MATCH (a:person)-[e:knows]->(b:person) where b.ID > 200 AND a.ID = 1 "
                        "RETURN e.length, e.place, e.tag");
        auto actualResult = TestHelper::convertResultToString(*result, false /* checkOrder */);
        auto expectedResult =
            isCommit ? std::vector<std::string>{"||", "300|this is a long str|[123,good]",
                           "34|short|", "45||[123,good]"} :
                       std::vector<std::string>{};
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void insertRelsToLargeList(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        insertRel("person" /* srcNode */, 0 /* srcPK */, "person" /* dstNode */, 2301 /* dstPK */,
            "knows" /* relation */,
            "{length: 543, place: 'waterloo', tag: ['good']}" /* propertyValues */);
        insertRel("person" /* srcNode */, 0 /* srcPK */, "person" /* dstNode */, 2305 /* dstPK */,
            "knows" /* relation */, "{place: 'ontario', tag: ['excellent']}" /* propertyValues */);
        insertRel("person" /* srcNode */, 0 /* srcPK */, "person" /* dstNode */, 2410 /* dstPK */,
            "knows" /* relation */, "{length: 2340, place: 'toronto'}" /* propertyValues */);
        insertRel("person" /* srcNode */, 0 /* srcPK */, "person" /* dstNode */, 2424 /* dstPK */,
            "knows" /* relation */, "" /* propertyValues */);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result = conn->query("MATCH (a:person)-[e:knows]->(b:person) WHERE a.ID = 0 RETURN "
                                  "e.length, e.place, e.tag");
        auto actualResult = TestHelper::convertResultToString(*result, false /* checkOrder */);
        std::vector<std::string> expectedResult;
        // Regardless of whether we commit or rollback, we can always see the rels that are stored
        // in the original list.
        for (auto i = 1; i < 2301; i++) {
            expectedResult.push_back(
                StringUtils::string_format("{}|{}|[{}]", i, 3000 - i, 3000 - i));
        }
        if (isCommit) {
            // We can only see newly inserted rels only if we commit the transaction.
            expectedResult.emplace_back("543|waterloo|[good]");
            expectedResult.emplace_back("|ontario|[excellent]");
            expectedResult.emplace_back("2340|toronto|");
            expectedResult.emplace_back("||");
        }
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    // This test is designed to test the case that a small list may become large after rel
    // insertions. Note: we use adjList to determine whether the lists should be small or large.
    // After rel insertions, the size of the adjList is: 4 bytes * 1500 num of rels = 6000 bytes,
    // which is larger than the size of DEFAULT_PAGE_SIZE. As a result, lists will become large
    // after insertion.
    void smallListBecomesLargeListAfterInsertion(
        bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        for (auto i = 51; i < 1500; i++) {
            insertRel("person" /* srcNode */, 1 /* srcPK */, "person" /* dstNode */, i /* dstPK */,
                "knows" /* relation */,
                StringUtils::string_format("{{length: {}, place: '{}', tag: ['{}']}}", i, 1000 - i,
                    1000 - i) /* propertyValues */);
        }
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result = conn->query("MATCH (a:person)-[e:knows]->(b:person) WHERE a.ID = 1 RETURN "
                                  "e.length, e.place, e.tag");
        auto actualResult = TestHelper::convertResultToString(*result, false /* checkOrder */);
        std::vector<std::string> expectedResult;
        // Regardless of whether we commit or rollback, we can always see the rels that are stored
        // in the original list.
        for (auto i = 0; i <= 50; i++) {
            expectedResult.push_back(
                StringUtils::string_format("{}|{}|[{}]", i, 1000 - i, 1000 - i));
        }
        if (isCommit) {
            // We can only see newly inserted rels only if we commit the transaction.
            for (auto i = 51; i < 1500; i++) {
                expectedResult.push_back(
                    StringUtils::string_format("{}|{}|[{}]", i, 1000 - i, 1000 - i));
            }
        }
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void insertRelsToDifferentNodes(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        insertRel("person" /* srcNode */, 500 /* srcPK */, "person" /* dstNode */, 2200 /* dstPK */,
            "knows" /* relation */, "{}" /* propertyValues */);
        insertRel("person" /* srcNode */, 243 /* srcPK */, "person" /* dstNode */, 744 /* dstPK */,
            "knows" /* relation */,
            "{length: 752, place: 'waterloo', tag: ['good test']}" /* propertyValues */);
        insertRel("person" /* srcNode */, 67 /* srcPK */, "person" /* dstNode */, 43 /* dstPK */,
            "knows" /* relation */,
            "{length: 143,  tag: ['long long string']}" /* propertyValues */);
        insertRel("person" /* srcNode */, 84 /* srcPK */, "person" /* dstNode */, 21 /* dstPK */,
            "knows" /* relation */, "{place: 'very very special string'}" /* propertyValues */);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result = conn->query("MATCH (a:person)-[e:knows]->(b:person) WHERE a.ID > 50 RETURN "
                                  "e.length, e.place, e.tag");
        auto actualResult = TestHelper::convertResultToString(*result, false /* checkOrder */);
        std::vector<std::string> expectedResult = isCommit ?
                                                      std::vector<std::string>{
                                                          "752|waterloo|[good test]",
                                                          "143||[long long string]",
                                                          "|very very special string|",
                                                          "||",
                                                      } :
                                                      std::vector<std::string>{};
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void insertRelsToManyToOneRelTable(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        insertRel("person" /* srcNode */, 704 /* srcPK */, "person" /* dstNode */, 2103 /* dstPK */,
            "teaches" /* relation */, "{length: 50}" /* propertyValues */);
        insertRel("person" /* srcNode */, 970 /* srcPK */, "person" /* dstNode */, 1765 /* dstPK */,
            "teaches" /* relation */, "{length: 1000}" /* propertyValues */);
        insertRel("person" /* srcNode */, 444 /* srcPK */, "person" /* dstNode */, 1432 /* dstPK */,
            "teaches" /* relation */, "{}" /* propertyValues */);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result =
            conn->query("MATCH (a:person)-[e:teaches]->(:person) WHERE a.ID > 200 RETURN e.length");
        auto actualResult = TestHelper::convertResultToString(*result, false /* checkOrder */);
        std::vector<std::string> expectedResult =
            isCommit ? std::vector<std::string>{"50", "1000", ""} : std::vector<std::string>{};
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void insertRelsToOneToOneRelTable(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        insertRel("animal" /* srcNode */, 64 /* srcPK */, "person" /* dstNode */, 2100 /* dstPK */,
            "hasOwner" /* relation */,
            "{length: 50, place: 'long long string test'}" /* propertyValues */);
        insertRel("animal" /* srcNode */, 56 /* srcPK */, "person" /* dstNode */, 782 /* dstPK */,
            "hasOwner" /* relation */, "{length: 43}" /* propertyValues */);
        insertRel("animal" /* srcNode */, 62 /* srcPK */, "person" /* dstNode */, 230 /* dstPK */,
            "hasOwner" /* relation */, "{}" /* propertyValues */);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result = conn->query("MATCH (a:animal)-[e:hasOwner]->(:person) WHERE a.ID > 50 RETURN "
                                  "e.length, e.place");
        auto actualResult = TestHelper::convertResultToString(*result, false /* checkOrder */);
        std::vector<std::string> expectedResult =
            isCommit ? std::vector<std::string>{"50|long long string test", "43|", "|"} :
                       std::vector<std::string>{};
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void insertRelsToNewlyAddedNode(bool isCommit, TransactionTestType transactionTestType) {
        conn->beginWriteTransaction();
        ASSERT_TRUE(conn->query("CREATE (a:person {ID: 3000})")->isSuccess());
        insertRel("person" /* srcNode */, 3000 /* srcPK */, "person" /* dstNode */,
            2100 /* dstPK */, "knows" /* relation */,
            "{length: 50, place: 'long long string test'}" /* propertyValues */);
        insertRel("person" /* srcNode */, 3000 /* srcPK */, "person" /* dstNode */, 782 /* dstPK */,
            "knows" /* relation */, "{length: 43}" /* propertyValues */);
        insertRel("person" /* srcNode */, 3000 /* srcPK */, "person" /* dstNode */, 230 /* dstPK */,
            "knows" /* relation */, "{}" /* propertyValues */);
        commitOrRollbackConnectionAndInitDBIfNecessary(isCommit, transactionTestType);
        auto result = conn->query("MATCH (a:person)-[e:knows]->(:person) WHERE a.ID > 2600 RETURN "
                                  "e.length, e.place");
        auto actualResult = TestHelper::convertResultToString(*result, false /* checkOrder */);
        std::vector<std::string> expectedResult =
            isCommit ? std::vector<std::string>{"50|long long string test", "43|", "|"} :
                       std::vector<std::string>{};
        sortAndCheckTestResults(actualResult, expectedResult);
    }

    void validateExceptionMessage(std::string query, std::string expectedException) {
        auto result = conn->query(query);
        ASSERT_FALSE(result->isSuccess());
        ASSERT_STREQ(result->getErrorMessage().c_str(), expectedException.c_str());
    }
};

TEST_F(CreateRelTest, InsertRelsToSmallListCommitNormalExecution) {
    insertRelsToSmallList(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateRelTest, InsertRelsToSmallListCommitRecovery) {
    insertRelsToSmallList(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateRelTest, InsertRelsToSmallListRollbackNormalExecution) {
    insertRelsToSmallList(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateRelTest, InsertRelsToSmallListRollbackRecovery) {
    insertRelsToSmallList(false /* isCommit */, TransactionTestType::RECOVERY);
}

// TEST_F(CreateRelTest, InsertRelsToLargeListCommitNormalExecution) {
//    insertRelsToLargeList(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(CreateRelTest, InsertRelsToLargeListCommitRecovery) {
//    insertRelsToLargeList(true /* isCommit */, TransactionTestType::RECOVERY);
//}
//
// TEST_F(CreateRelTest, InsertRelsToLargeListRollbackNormalExecution) {
//    insertRelsToLargeList(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(CreateRelTest, InsertRelsToLargeListRollbackRecovery) {
//    insertRelsToLargeList(false /* isCommit */, TransactionTestType::RECOVERY);
//}

// TEST_F(CreateRelTest, SmallListBecomeLargeListAfterInsertionCommitNormalExecution) {
//    smallListBecomesLargeListAfterInsertion(
//        true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
//}
//
// TEST_F(CreateRelTest, SmallListBecomeLargeListAfterInsertionCommitRecovery) {
//    smallListBecomesLargeListAfterInsertion(true /* isCommit */, TransactionTestType::RECOVERY);
//}

TEST_F(CreateRelTest, SmallListBecomeLargeListAfterInsertionRollbackNormalExecution) {
    smallListBecomesLargeListAfterInsertion(
        false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateRelTest, SmallListBecomeLargeListAfterInsertionRollbackRecovery) {
    smallListBecomesLargeListAfterInsertion(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateRelTest, InsertRelsToDifferentNodesCommitNormalExecution) {
    insertRelsToDifferentNodes(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateRelTest, InsertRelsToDifferentNodesCommitRecovery) {
    insertRelsToDifferentNodes(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateRelTest, InsertRelsToDifferentNodesRollbackNormalExecution) {
    insertRelsToDifferentNodes(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateRelTest, InsertRelsToDifferentNodesRollbackRecovery) {
    insertRelsToDifferentNodes(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateRelTest, InsertRelsToManyToOneRelTableCommitNormalExecution) {
    insertRelsToManyToOneRelTable(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateRelTest, InsertRelsToManyToOneRelTableCommitRecovery) {
    insertRelsToManyToOneRelTable(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateRelTest, InsertRelsToManyToOneRelTableRollbackNormalExecution) {
    insertRelsToManyToOneRelTable(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateRelTest, InsertRelsToManyToOneRelTableRollbackRecovery) {
    insertRelsToManyToOneRelTable(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateRelTest, InsertRelsToOneToOneRelTableCommitNormalExecution) {
    insertRelsToOneToOneRelTable(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateRelTest, InsertRelsToOneToOneRelTableCommitRecovery) {
    insertRelsToOneToOneRelTable(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateRelTest, InsertRelsToOneToOneRelTableRollbackNormalExecution) {
    insertRelsToOneToOneRelTable(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateRelTest, InsertRelsToOneToOneRelTableRollbackRecovery) {
    insertRelsToOneToOneRelTable(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateRelTest, InsertRelsToNewlyAddedNodeCommitNormalExecution) {
    insertRelsToNewlyAddedNode(true /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateRelTest, InsertRelsToNewlyAddedNodeCommitRecovery) {
    insertRelsToNewlyAddedNode(true /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateRelTest, InsertRelsToNewlyAddedNodeRollbackNormalExecution) {
    insertRelsToNewlyAddedNode(false /* isCommit */, TransactionTestType::NORMAL_EXECUTION);
}

TEST_F(CreateRelTest, InsertRelsToNewlyAddedNodeRollbackRecovery) {
    insertRelsToNewlyAddedNode(false /* isCommit */, TransactionTestType::RECOVERY);
}

TEST_F(CreateRelTest, ViolateManyOneMultiplicityError) {
    conn->beginWriteTransaction();
    validateExceptionMessage(
        "MATCH (p1:person), (p2:person) WHERE p1.ID = 11 AND p2.ID = 10 CREATE "
        "(p1)-[:teaches]->(p2);",
        "Runtime exception: Node(nodeOffset: 11, tableID: 1) in RelTable 4 cannot have more than "
        "one neighbour in the forward direction.");
}

TEST_F(CreateRelTest, ViolateOneOneMultiplicityError) {
    conn->beginWriteTransaction();
    validateExceptionMessage("MATCH (a:animal), (p:person) WHERE a.ID = 2 AND p.ID = 10 CREATE "
                             "(a)-[:hasOwner]->(p);",
        "Runtime exception: Node(nodeOffset: 2, tableID: 0) in RelTable 3 cannot have more than "
        "one neighbour in the forward direction.");
}

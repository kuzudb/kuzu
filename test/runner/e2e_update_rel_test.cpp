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
};

TEST_F(UpdateRelTest, UpdateIntProp) {
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
    auto result = conn->query(
        "MATCH (a:animal)-[e:knows]->(:person) where a.ID >= 20 and a.ID <= 30 return e.length");
    auto actualResult = TestHelper::convertResultToString(*result);
    vector<string> expectedResult =
        vector<string>{"", "21", "22", "23", "24", "210", "26", "27", "28", "29", "300"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

TEST_F(UpdateRelTest, UpdateStrProp) {
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
    auto result = conn->query(
        "MATCH (a:animal)-[e:knows]->(:person) where a.ID >= 10 and a.ID <= 20 return e.place");
    auto actualResult = TestHelper::convertResultToString(*result);
    auto expectedResult = vector<string>{"990990990", "989", "988988988", "987", "986986986",
        "waterloo", "kitchener", "983", "982982982", "", "980980980"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

TEST_F(UpdateRelTest, UpdateListProp) {
    conn->beginWriteTransaction();
    ASSERT_TRUE(conn->query(getUpdateRelQuery("animal" /* srcTable */, "person" /* dstTable */,
                                "knows" /* relation */, 43 /* srcID */, 0 /* dstID */,
                                "set e.tag = [\"updated property1\", \"50\"]"))
                    ->isSuccess());
    ASSERT_TRUE(
        conn->query(getUpdateRelQuery("animal" /* srcTable */, "person" /* dstTable */,
                        "knows" /* relation */, 47 /* srcID */, 0 /* dstID */, "set e.tag = null"))
            ->isSuccess());
    ASSERT_TRUE(conn->query(getUpdateRelQuery("animal" /* srcTable */, "person" /* dstTable */,
                                "knows" /* relation */, 49 /* srcID */, 0 /* dstID */,
                                "set e.tag = [\"updated property3\", \"54\"]"))
                    ->isSuccess());
    auto result = conn->query(
        "MATCH (a:animal)-[e:knows]->(:person) where a.ID >= 40 and a.ID <= 50 return e.tag");
    auto actualResult = TestHelper::convertResultToString(*result);
    auto expectedResult = vector<string>{"[960960960]", "[959]", "[958958958]",
        "[updated property1,50]", "[956956956]", "[955]", "[954954954]", "", "[952952952]",
        "[updated property3,54]", "[950950950]"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

TEST_F(UpdateRelTest, UpdateEachElemOfSmallList) {
    conn->beginWriteTransaction();
    vector<string> expectedResult;
    for (auto i = 0u; i <= 50; i++) {
        ASSERT_TRUE(conn->query(getUpdateRelQuery("animal" /* srcTable */, "person" /* dstTable */,
                                    "knows" /* relation */, i /* srcID */, 0 /* dstID */,
                                    "set e.length = " + to_string(i)))
                        ->isSuccess());
        expectedResult.push_back(to_string(i));
    }
    auto result = conn->query("MATCH (:animal)-[e:knows]->(p:person) return e.length");
    auto actualResult = TestHelper::convertResultToString(*result);
    sortAndCheckTestResults(actualResult, expectedResult);
}

TEST_F(UpdateRelTest, UpdateEachElemOfLargeList) {
    conn->beginWriteTransaction();
    vector<string> expectedResult;
    for (auto i = 1u; i <= 2500; i++) {
        ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
                                    "knows" /* relation */, 0 /* srcID */, i /* dstID */,
                                    "set e.length = " + to_string(i)))
                        ->isSuccess());
        expectedResult.push_back(to_string(i));
    }
    auto result = conn->query("MATCH (:person)-[e:knows]->(p:person) return e.length");
    auto actualResult = TestHelper::convertResultToString(*result);
    sortAndCheckTestResults(actualResult, expectedResult);
}

TEST_F(UpdateRelTest, UpdateNewlyInsertedEdges) {
    conn->beginWriteTransaction();
    ASSERT_TRUE(conn->query(getInsertKnowsRelQuery("person" /* srcTable */, "person" /* dstTable */,
        5 /*  srcID */, 8 /* dstID */, 10 /* lengthProp */)));
    ASSERT_TRUE(conn->query(getInsertKnowsRelQuery("person" /* srcTable */, "person" /* dstTable */,
        7 /*  srcID */, 10 /* dstID */, 20 /* lengthProp */)));
    ASSERT_TRUE(conn->query(getInsertKnowsRelQuery("person" /* srcTable */, "person" /* dstTable */,
        32 /*  srcID */, 51 /* dstID */, 30 /* lengthProp */)));
    ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
        "knows" /* relation */, 5 /* srcID */, 8 /* dstID */, "set e.length = 55")));
    ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
        "knows" /* relation */, 7 /* srcID */, 10 /* dstID */, "set e.length = null")));
    ASSERT_TRUE(conn->query(getUpdateRelQuery("person" /* srcTable */, "person" /* dstTable */,
        "knows" /* relation */, 32 /* srcID */, 51 /* dstID */, "set e.length = 201")));
    auto result =
        conn->query("MATCH (p:person)-[e:knows]->(:person) where p.ID > 4 return e.length");
    auto actualResult = TestHelper::convertResultToString(*result);
    auto expectedResult = vector<string>{"", "55", "201"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

TEST_F(UpdateRelTest, UpdateRelsTwoHop) {
    conn->beginWriteTransaction();
    ASSERT_TRUE(conn->query("MATCH (a:animal)-[e1:knows]->(p:person)-[e2:knows]->(p1:person) WHERE "
                            "p1.ID = 2500 AND a.ID = 5 SET e1.length = 300, e2.length = null"));
    auto result =
        conn->query("MATCH (a:animal)-[e1:knows]->(p:person)-[e2:knows]->(p1:person) WHERE "
                    "p1.ID = 2500 AND a.ID = 5 RETURN e1.length, e2.length");
    auto actualResult = TestHelper::convertResultToString(*result);
    auto expectedResult = vector<string>{"300|"};
    sortAndCheckTestResults(actualResult, expectedResult);
}

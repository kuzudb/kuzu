#include "test_helper/test_helper.h"

using namespace kuzu::testing;

class DeleteRelTest : public DBTest {
public:
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/rel-insertion-tests/");
    }
    string getDeleteKnowsRelQuery(string srcTable, string dstTable, int64_t srcID, int64_t dstID) {
        return StringUtils::string_format(
            "MATCH (p1:%s)-[e:knows]->(p2:%s) WHERE p1.ID = %d AND p2.ID = %d delete e;",
            srcTable.c_str(), dstTable.c_str(), srcID, dstID);
    }
    string getInsertKnowsRelQuery(string srcTable, string dstTable, int64_t srcID, int64_t dstID) {
        auto str = StringUtils::string_format("MATCH (p1:%s), (p2:%s) WHERE p1.ID = %d AND p2.ID "
                                              "= %d create (p1)-[:knows {length: %d}]->(p2);",
            srcTable.c_str(), dstTable.c_str(), srcID, dstID, srcID);
        return str;
    }

    static constexpr uint64_t NUM_RELS_FOR_PERSON_KNOWS_PERSON = 2500;
    static constexpr uint64_t NUM_RELS_FOR_ANIMAL_KNOWS_PERSON = 51;
};

TEST_F(DeleteRelTest, DeleteRelsFromLargeList) {
    conn->beginWriteTransaction();
    // We delete all person->person rels whose dst nodeID offset is between 100-200 (inclusive);
    for (auto i = 100; i <= 200; i++) {
        ASSERT_TRUE(
            conn->query(getDeleteKnowsRelQuery("person", "person", 0 /* srcID */, i))->isSuccess());
    }
    auto result = conn->query("MATCH (:person)-[e:knows]->(:person) return e.length");
    auto actualResult = TestHelper::convertResultToString(*result, true /* checkOutputOrder */);
    vector<string> expectedResult;
    for (auto i = 1; i <= NUM_RELS_FOR_PERSON_KNOWS_PERSON; i++) {
        if (i < 100 || i > 200) {
            expectedResult.push_back(to_string(3 * (i - 1)));
        }
    }
    ASSERT_EQ(actualResult, expectedResult);
}

TEST_F(DeleteRelTest, DeleteRelsFromSmallList) {
    conn->beginWriteTransaction();
    // We delete all animal->person rels whose src nodeID offset is between 10-20 (inclusive);
    for (auto i = 10; i <= 20; i++) {
        ASSERT_TRUE(
            conn->query(getDeleteKnowsRelQuery("animal", "person", i, 0 /* dstID */))->isSuccess());
    }
    auto actualResult = TestHelper::convertResultToString(
        *conn->query("MATCH (:animal)-[e:knows]->(:person) return e.length"),
        true /* checkOutputOrder */);
    vector<string> expectedResult;
    for (auto i = 0; i < NUM_RELS_FOR_ANIMAL_KNOWS_PERSON; i++) {
        if (i < 10 || i > 20) {
            expectedResult.push_back(to_string(i));
        }
    }
    ASSERT_EQ(actualResult, expectedResult);
}

TEST_F(DeleteRelTest, DeleteRelsFromUpdateStore) {
    conn->beginWriteTransaction();
    // We insert 3 rels: animal51->person0, animal52->person0, animal100->person10, and then delete
    // animal51->person0, animal100->person10.
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

    auto actualResult = TestHelper::convertResultToString(
        *conn->query("MATCH (:animal)-[e:knows]->(:person) return e.length"),
        true /* checkOutputOrder */);
    // After insertion and deletion, we are expected to see only one new rel: animal52->person0.
    vector<string> expectedResult;
    for (auto i = 0; i < NUM_RELS_FOR_ANIMAL_KNOWS_PERSON; i++) {
        expectedResult.push_back(to_string(i));
    }
    expectedResult.push_back(to_string(52));
    ASSERT_EQ(actualResult, expectedResult);
}

TEST_F(DeleteRelTest, DeleteRelsAndQueryBWDList) {
    auto query = "MATCH (a:person)-[e:knows]->(b:animal) RETURN e.length";
    // This check is to validate whether the query used in "deleteRelsAndQueryBWDList"
    // scans the backward list of the knows table. If this test fails (eg. we have updated the
    // planner), we should consider changing to a new query which scans the BWD list with
    // the new planner.
    validateQueryBestPlanJoinOrder(query, "HJ(a){E(a)S(b)}{S(a)}");
    conn->beginWriteTransaction();
    // We insert 10 rels: person[0..10]->animal0.
    for (auto i = 0u; i < 10; i++) {
        ASSERT_TRUE(
            conn->query(getInsertKnowsRelQuery("person", "animal", i, 0 /* dstID */))->isSuccess());
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
    vector<string> expectedResult = {"0", "1", "3", "5", "7", "9"};
    ASSERT_EQ(TestHelper::convertResultToString(*conn->query(query), true /* checkOutputOrder */),
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
    ASSERT_TRUE(
        conn->query(getInsertKnowsRelQuery("animal", "person", 7 /* srcID */, 0 /* dstID */))
            ->isSuccess());
    expectedResult = {"0", "1", "2", "3", "4", "5", "6", "7", "9", "10"};
    ASSERT_EQ(
        TestHelper::convertResultToString(*conn->query(knowsRelQuery), true /* checkOutputOrder */),
        expectedResult);
    ASSERT_TRUE(
        conn->query(getDeleteKnowsRelQuery("animal", "person", 7 /* srcID */, 0 /* dstID */))
            ->isSuccess());
    expectedResult = {"0", "1", "2", "3", "4", "5", "6", "9", "10"};
    ASSERT_EQ(
        TestHelper::convertResultToString(*conn->query(knowsRelQuery), true /* checkOutputOrder */),
        expectedResult);
}

TEST_F(DeleteRelTest, DeleteMultipleRels) {
    conn->beginWriteTransaction();
    // Delete all rels from animal[10-49]->person0.
    ASSERT_TRUE(conn->query("MATCH (a:animal)-[e:knows]->(p:person) WHERE a.ID >= 10 AND "
                            "a.ID <= 49 AND p.ID = 0 DELETE e;")
                    ->isSuccess());
    vector<string> expectedResult = {"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "50"};
    ASSERT_EQ(TestHelper::convertResultToString(
                  *conn->query("MATCH (a:animal)-[e:knows]->(p:person) RETURN e.length"),
                  true /* checkOutputOrder */),
        expectedResult);
}

TEST_F(DeleteRelTest, DeleteAllInsertedRels) {
    conn->beginWriteTransaction();
    // Insert rels for animal[60-69]->person0.
    for (auto i = 60; i < 70; i++) {
        ASSERT_TRUE(
            conn->query(getInsertKnowsRelQuery("animal", "person", i, 0 /* dstID */))->isSuccess());
    }
    // Then delete those inserted rels.
    for (auto i = 60; i < 70; i++) {
        ASSERT_TRUE(
            conn->query(getDeleteKnowsRelQuery("animal", "person", i, 0 /* dstID */))->isSuccess());
    }
    vector<string> expectedResult = {
        "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "50"};
    ASSERT_EQ(
        TestHelper::convertResultToString(
            *conn->query("MATCH (a:animal)-[e:knows]->(p:person) WHERE a.ID >= 40 RETURN e.length"),
            true /* checkOutputOrder */),
        expectedResult);
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
